use bytes::Bytes;
use std::{
    cmp::max,
    collections::HashMap,
    sync::{Mutex, mpsc},
    time::{Duration, Instant},
};

use std::sync::Arc;

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
    expiry: HashMap<String, Expiry>,
    waiters: HashMap<String, Vec<mpsc::Sender<Bytes>>>,
}

impl State {
    fn new() -> Self {
        Self {
            db: HashMap::new(),
            list_db: HashMap::new(),
            expiry: HashMap::new(),
            waiters: HashMap::new(),
        }
    }
}

struct Shared {
    state: Mutex<State>,
}

#[derive(Clone)]
pub struct Db {
    shared: Arc<Shared>,
}

impl Db {
    pub fn new() -> Self {
        Db {
            shared: Arc::new(Shared {
                state: Mutex::new(State::new()),
            }),
        }
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
            state.expiry.insert(
                key.clone(),
                Expiry {
                    now: Instant::now(),
                    duration: expire,
                },
            );
        }

        state.db.insert(key, value);
    }

    pub fn rpush(&self, key: String, values: Vec<Bytes>) -> usize {
        let mut state = self.shared.state.lock().unwrap();
        let list = state.list_db.entry(key).or_insert_with(|| vec![]);

        for value in values {
            list.push(value);
        }

        list.len()
    }

    pub fn lpush(&self, key: String, values: Vec<Bytes>) -> usize {
        let mut state = self.shared.state.lock().unwrap();
        let list = state.list_db.entry(key).or_insert_with(|| vec![]);

        for value in values {
            list.insert(0, value);
        }

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
}
