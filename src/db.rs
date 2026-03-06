use bytes::Bytes;
use std::{
    cmp::max,
    collections::{BTreeMap, HashMap},
    fmt::Display,
    sync::Mutex,
    time::{Duration, Instant},
};
use tokio::sync::Notify;

use thiserror::Error;
use tokio::sync::oneshot;

use std::sync::Arc;

use crate::parser::StreamIDOpt;

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

#[derive(Debug, PartialEq, Eq, PartialOrd, Hash, Clone, Ord, Copy)]
pub struct StreamID {
    miliseconds: u64,
    sequence: u64,
}

impl TryFrom<StreamIDOpt> for StreamID {
    type Error = StreamError;

    fn try_from(value: StreamIDOpt) -> Result<Self, Self::Error> {
        let miliseconds = value.miliseconds.ok_or(StreamError::MissingValue)?;
        let sequence = value.sequence.ok_or(StreamError::MissingValue)?;

        if sequence == 0 && miliseconds == 0 {
            Err(StreamError::ZeroZeroID)
        } else {
            Ok(Self {
                miliseconds,
                sequence,
            })
        }
    }
}

impl Display for StreamID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:}-{:}", self.miliseconds, self.sequence)
    }
}

pub type StreamEntry = Vec<(String, Bytes)>;

type Stream = BTreeMap<StreamID, StreamEntry>;

#[derive(Debug, Error)]
pub enum StreamError {
    #[error("ERR The ID specified in XADD is equal or smaller than the target stream top item")]
    InvalidId,
    #[error("ERR The ID specified in XADD must be greater 0-0")]
    ZeroZeroID,
    #[error("ERR The ID has sequence or miliseconds missing")]
    MissingValue,
    #[error("other error occured")]
    Other(#[from] anyhow::Error),
}

struct State {
    db: HashMap<String, Bytes>,
    list_db: HashMap<String, Vec<Bytes>>,
    streams: HashMap<String, Stream>,
    expiry: HashMap<String, Expiry>,
    waiters: HashMap<String, Vec<oneshot::Sender<Bytes>>>,
    waiters_ready: Vec<(String, oneshot::Sender<Bytes>)>,
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

        for (key, waiter) in ready_waiters {
            waiter
                .send(state.list_db.get_mut(&key).unwrap().remove(0))
                .unwrap()
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
            let waiters = state.waiters.get_mut(key).unwrap();
            let waiter = waiters.remove(0);
            if waiters.len() == 0 {
                state.waiters.remove(key);
            }
            state.waiters_ready.push((key.to_string(), waiter));
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
        id_opt: StreamIDOpt,
        values: StreamEntry,
    ) -> Result<StreamID, StreamError> {
        let id = StreamID::try_from(id_opt)?;

        let mut state = self.shared.state.lock().unwrap();
        let stream = state.streams.entry(key).or_insert(Stream::new());

        if let Some(e) = stream.last_entry() {
            let key = *e.key();
            if id <= key {
                return Err(StreamError::InvalidId);
            }
        }

        stream.insert(id, values);

        Ok(id)
    }
}

async fn handle_ready_waiters(shared: Arc<Shared>) {
    loop {
        shared.waiters_background_task.notified().await;
        shared.handle_ready_waiters();
    }
}
