use bytes::Bytes;
use std::{
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
}
