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
use crate::stream::{Stream, StreamEntry, StreamEntryID, StreamError};

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

struct State {
    db: HashMap<String, Bytes>,
    list_db: HashMap<String, Vec<Bytes>>,
    streams: HashMap<String, Stream>,
    expiry: HashMap<String, Expiry>,
    waiters: HashMap<String, Vec<oneshot::Sender<Bytes>>>,
    waiters_ready: Vec<String>,
}

impl State {
    fn new() -> Self {
        Self {
            db: HashMap::new(),
            list_db: HashMap::new(),
            streams: HashMap::new(),
            expiry: HashMap::new(),
            waiters: HashMap::new(),
            waiters_ready: vec![],
        }
    }
}

struct Shared {
    state: Mutex<State>,
    waiters_background_task: Notify,
}

impl Shared {
    fn handle_ready_waiters(&self) {
        let mut state = self.state.lock().unwrap();

        let mut ready_waiters = vec![];
        std::mem::swap(&mut state.waiters_ready, &mut ready_waiters);

        for key in ready_waiters {
            loop {
                let data = state.list_db.get(&key).unwrap()[0].clone();

                match state.waiters.get_mut(&key) {
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
}

#[derive(Clone)]
pub struct Db {
    shared: Arc<Shared>,
}

impl Db {
    pub fn new() -> Self {
        let shared = Arc::new(Shared {
            state: Mutex::new(State::new()),
            waiters_background_task: Notify::new(),
        });

        tokio::spawn(handle_ready_waiters(shared.clone()));

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

    fn check_for_ready_waiters(&self, key: &str) {
        let mut state = self.shared.state.lock().unwrap();

        if state.waiters.contains_key(key) {
            state.waiters_ready.push(key.to_string());
            self.shared.waiters_background_task.notify_one();
        }
    }

    pub fn rpush(&self, key: String, values: Vec<Bytes>) -> usize {
        self.check_for_ready_waiters(&key);
        let mut state = self.shared.state.lock().unwrap();

        let list = state.list_db.entry(key).or_insert_with(|| vec![]);

        for value in values {
            list.push(value);
        }

        list.len()
    }

    pub fn lpush(&self, key: String, values: Vec<Bytes>) -> usize {
        self.check_for_ready_waiters(&key);

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

        state.waiters.entry(key).or_insert_with(|| vec![]).push(tx);

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

    pub fn xadd(
        &self,
        key: String,
        id_opt: StreamEntryIDOpt,
        values: StreamEntry,
    ) -> Result<StreamEntryID, StreamError> {
        let mut state = self.shared.state.lock().unwrap();
        let stream = state.streams.entry(key).or_insert(Stream::new());

        stream.insert(id_opt, values)
    }

    pub fn xrange(
        &self,
        key: String,
        start: Option<StreamEntryIDOpt>,
        end: Option<StreamEntryIDOpt>,
    ) -> Result<Vec<(StreamEntryID, StreamEntry)>, StreamError> {
        let mut state = self.shared.state.lock().unwrap();
        let stream = state.streams.entry(key).or_insert(Stream::new());

        stream.xrange(start, end)
    }
}

async fn handle_ready_waiters(shared: Arc<Shared>) {
    loop {
        shared.waiters_background_task.notified().await;
        shared.handle_ready_waiters();
    }
}
