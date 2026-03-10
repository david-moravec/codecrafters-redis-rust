use bytes::Bytes;
use std::{
    cmp::max,
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};
use tokio::sync::Notify;

use tokio::sync::oneshot;

use std::sync::Arc;

use crate::parser::StreamEntryIDOpt;
use crate::stream::{Stream, StreamEntry, StreamEntryID, StreamError, XRange, XRead};

struct Expiry {
    now: Instant,
    duration: Duration,
}

impl Expiry {
    fn expired(&self) -> bool {
        return self.now.elapsed() > self.duration;
    }
}

impl From<Duration> for Expiry {
    fn from(duration: Duration) -> Self {
        Self {
            now: Instant::now(),
            duration,
        }
    }
}

pub struct XReadWaiter {
    id: StreamEntryIDOpt,
    tx: oneshot::Sender<XRead>,
}

struct State {
    db: HashMap<String, Bytes>,
    list_db: HashMap<String, Vec<Bytes>>,
    streams: HashMap<String, Stream>,
    expiry: HashMap<String, Expiry>,
    blpop_waiters: HashMap<String, Vec<oneshot::Sender<Bytes>>>,
    blpop_waiters_ready: Vec<String>,
    xread_waiters: HashMap<String, Vec<XReadWaiter>>,
    xread_waiters_ready: Vec<String>,
}

impl State {
    fn new() -> Self {
        Self {
            db: HashMap::new(),
            list_db: HashMap::new(),
            streams: HashMap::new(),
            expiry: HashMap::new(),
            blpop_waiters: HashMap::new(),
            blpop_waiters_ready: vec![],
            xread_waiters: HashMap::new(),
            xread_waiters_ready: vec![],
        }
    }
}

struct Shared {
    state: Mutex<State>,
    blpop_waiters_background_task: Notify,
    xread_waiters_background_task: Notify,
}

impl Shared {
    fn handle_blpop_waiters(&self) {
        let mut state = self.state.lock().unwrap();

        let mut ready_waiters = vec![];
        std::mem::swap(&mut state.blpop_waiters_ready, &mut ready_waiters);

        for key in ready_waiters {
            loop {
                let data = state.list_db.get(&key).unwrap()[0].clone();

                match state.blpop_waiters.get_mut(&key) {
                    Some(waiters) => {
                        if waiters.len() == 0 {
                            break;
                        }
                        let waiter = waiters.remove(0);

                        match waiter.send(data) {
                            Ok(()) => {
                                // Sending was succesful remove the data
                                state.list_db.get_mut(&key).unwrap().remove(0);
                                break;
                            }
                            Err(_) => {}
                        }
                    }
                    None => break,
                }
            }
        }
    }

    fn handle_xread_waiters(&self) {
        let mut state = self.state.lock().unwrap();

        let mut ready_waiters = vec![];
        std::mem::swap(&mut state.xread_waiters_ready, &mut ready_waiters);

        for key in ready_waiters {
            let mut i = 0;

            loop {
                let waiter = match state.xread_waiters.get(&key).unwrap().get(i) {
                    Some(w) => w,
                    None => break,
                };

                let xrange = state
                    .streams
                    .get(&key)
                    .unwrap()
                    .xrange(Some(waiter.id), None, false)
                    .unwrap();

                if xrange.entries_len() == 0 {
                    continue;
                }

                let xread = XRead::new(vec![(key.clone(), xrange)]);
                let waiter = state.xread_waiters.get_mut(&key).unwrap().remove(i);

                match waiter.tx.send(xread.clone()) {
                    Ok(()) => {}
                    Err(_) => {}
                }
                i += 1;
            }
        }
    }
}

#[derive(Clone)]
pub struct Db {
    shared: Arc<Shared>,
}

impl Db {
    pub fn new() -> Self {
        let shared = Arc::new(Shared {
            state: Mutex::new(State::new()),
            blpop_waiters_background_task: Notify::new(),
            xread_waiters_background_task: Notify::new(),
        });

        tokio::spawn(blpop_waiters_background_process(shared.clone()));
        tokio::spawn(xread_waiters_background_process(shared.clone()));

        Db { shared }
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        let mut state = self.shared.state.lock().unwrap();

        if let Some(expiry) = state.expiry.get(key) {
            if expiry.expired() {
                state.db.remove(key);
                state.expiry.remove(key);

                return None;
            }
        }

        state.db.get(key).map(|b| b.clone())
    }

    pub fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap();

        if let Some(expire) = expire {
            state.expiry.insert(key.clone(), Expiry::from(expire));
        }

        state.db.insert(key, value);
    }

    fn notify_blpop_waiters(&self, key: &str) {
        let mut state = self.shared.state.lock().unwrap();

        if state.blpop_waiters.contains_key(key) {
            state.blpop_waiters_ready.push(key.to_string());
            self.shared.blpop_waiters_background_task.notify_one();
        }
    }

    pub fn rpush(&self, key: String, values: Vec<Bytes>) -> usize {
        self.notify_blpop_waiters(&key);
        let mut state = self.shared.state.lock().unwrap();

        let list = state.list_db.entry(key).or_insert_with(|| vec![]);

        for value in values {
            list.push(value);
        }

        list.len()
    }

    pub fn lpush(&self, key: String, values: Vec<Bytes>) -> usize {
        self.notify_blpop_waiters(&key);

        let mut state = self.shared.state.lock().unwrap();

        let list = state.list_db.entry(key).or_insert_with(|| vec![]);

        for value in values {
            list.insert(0, value);
        }

        list.len()
    }

    pub fn lpop(&self, key: &str, start: Option<i64>, stop: Option<i64>) -> Option<Vec<Bytes>> {
        let mut state = self.shared.state.lock().unwrap();

        let list = match state.list_db.get_mut(key) {
            Some(l) => l,
            None => return None,
        };

        if start.is_none() && stop.is_none() {
            return Some(vec![list.remove(0)]);
        }

        let list_len = list.len();

        let true_start = {
            if stop.is_none() {
                0
            } else if start.unwrap() < 0 {
                max(list_len as i64 - start.unwrap().abs(), 0)
            } else if start.unwrap() >= list_len as i64 {
                return None;
            } else {
                start.unwrap()
            }
        };

        let true_stop = {
            if stop.is_none() {
                start.unwrap() - 1
            } else if stop.unwrap() < 0 {
                max(list_len as i64 - stop.unwrap().abs(), 0)
            } else if stop.unwrap() >= list_len as i64 {
                (list_len - 1) as i64
            } else {
                stop.unwrap()
            }
        };

        let mut result = vec![];

        for _ in true_start as usize..=true_stop as usize {
            result.push(list.remove(0))
        }

        Some(result)
    }

    pub fn blpop(&self, key: String) -> (Option<Bytes>, Option<oneshot::Receiver<Bytes>>) {
        if let Some(mut popped) = self.lpop(&key, None, None) {
            return (Some(popped.remove(0)), None);
        }

        let mut state = self.shared.state.lock().unwrap();

        let (tx, rx) = oneshot::channel();

        state
            .blpop_waiters
            .entry(key)
            .or_insert_with(|| vec![])
            .push(tx);

        (None, Some(rx))
    }

    pub fn llen(&self, key: String) -> usize {
        let mut state = self.shared.state.lock().unwrap();
        let list = state.list_db.entry(key).or_insert_with(|| vec![]);

        list.len()
    }

    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Vec<Bytes> {
        let state = self.shared.state.lock().unwrap();
        let list = match state.list_db.get(key) {
            Some(l) => l,
            None => return vec![],
        };
        let list_len = list.len();

        let true_stop = {
            if stop < 0 {
                max(list_len as i64 - stop.abs(), 0)
            } else if stop >= list_len as i64 {
                (list_len - 1) as i64
            } else {
                stop
            }
        };

        let true_start = {
            if start < 0 {
                max(list_len as i64 - start.abs(), 0)
            } else if start >= list_len as i64 {
                return vec![];
            } else {
                start
            }
        };

        if true_start > true_stop {
            return vec![];
        }

        list[true_start as usize..=true_stop as usize]
            .iter()
            .cloned()
            .collect()
    }

    pub fn value_type(&self, key: &str) -> String {
        let state = self.shared.state.lock().unwrap();

        if state.db.contains_key(key) {
            "string".to_string()
        } else if state.streams.contains_key(key) {
            "stream".to_string()
        } else {
            "none".to_string()
        }
    }

    fn notify_xread_waiters(&self, key: &str) {
        let mut state = self.shared.state.lock().unwrap();

        if state.xread_waiters.contains_key(key) {
            state.xread_waiters_ready.push(key.to_string());
            self.shared.xread_waiters_background_task.notify_one();
        }
    }

    pub fn xadd(
        &self,
        key: String,
        id_opt: StreamEntryIDOpt,
        values: StreamEntry,
    ) -> Result<StreamEntryID, StreamError> {
        let id = {
            let mut state = self.shared.state.lock().unwrap();
            let stream = state.streams.entry(key.clone()).or_insert(Stream::new());
            stream.insert(id_opt, values)
        };
        self.notify_xread_waiters(&key);
        id
    }

    pub fn xrange(
        &self,
        key: String,
        start: Option<StreamEntryIDOpt>,
        end: Option<StreamEntryIDOpt>,
    ) -> Result<XRange, StreamError> {
        let mut state = self.shared.state.lock().unwrap();
        let stream = state.streams.entry(key).or_insert(Stream::new());

        stream.xrange(start, end, true)
    }

    pub fn xread(
        &self,
        timeout: Option<u64>,
        keys: Vec<String>,
        ids: Vec<StreamEntryIDOpt>,
    ) -> Result<(Option<XRead>, Option<oneshot::Receiver<XRead>>), StreamError> {
        let mut state = self.shared.state.lock().unwrap();
        let mut xread_entries = vec![];

        let mut rx_opt = None;

        for (key, id) in keys.into_iter().zip(ids.into_iter()) {
            if state.streams.contains_key(&key) {
                let xrange =
                    state
                        .streams
                        .get(&key)
                        .unwrap()
                        .xrange(Some(id), None, timeout.is_none())?;

                if xrange.entries_len() == 0 && timeout.is_some() {
                    let (tx, rx) = oneshot::channel();
                    state
                        .xread_waiters
                        .entry(key.clone())
                        .or_insert(vec![])
                        .push(XReadWaiter { id, tx });
                    rx_opt = Some(rx);

                    return Ok((None, rx_opt));
                }

                xread_entries.push((key, xrange));
            }
        }

        Ok((Some(XRead::new(xread_entries)), rx_opt))
    }
}

async fn blpop_waiters_background_process(shared: Arc<Shared>) {
    loop {
        shared.blpop_waiters_background_task.notified().await;
        shared.handle_blpop_waiters();
    }
}

async fn xread_waiters_background_process(shared: Arc<Shared>) {
    loop {
        shared.xread_waiters_background_task.notified().await;
        shared.handle_xread_waiters();
    }
}
