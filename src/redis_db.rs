use core::time;
use std::{
    cell::RefCell,
    collections::HashMap,
    time::{Duration, Instant},
};

use crate::redis_parser::{RESPData, RESPMap};

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

pub struct RedisDB {
    db: RESPMap,
    expiry: HashMap<RESPData, Expiry>,
}

impl RedisDB {
    pub fn new() -> Self {
        Self {
            db: RESPMap::new(),
            expiry: HashMap::new(),
        }
    }

    pub fn insert(
        &mut self,
        key: RESPData,
        value: RESPData,
        duration: Option<Duration>,
    ) -> Option<RESPData> {
        if let Some(d) = duration {
            self.expiry.insert(key.clone(), Expiry::from(d));
        }

        self.db.insert(key, value)
    }

    pub fn get(&mut self, key: &RESPData) -> Option<&RESPData> {
        if let Some(expiry) = self.expiry.get(key) {
            if expiry.expired() {
                self.db.remove(key);
                self.expiry.remove(key);

                return None;
            }
        }

        self.db.get(key)
    }
}

#[cfg(test)]
mod tests {
    use std::thread::sleep;

    use crate::redis_parser::NULL_STRING;

    use super::*;

    #[test]
    fn test_expiry() {
        let mut db = RedisDB::new();

        db.insert(
            NULL_STRING.clone(),
            NULL_STRING.clone(),
            Some(Duration::from_millis(10)),
        );

        assert!(db.get(&NULL_STRING) == Some(&NULL_STRING));

        sleep(Duration::from_millis(10));

        assert!(db.get(&NULL_STRING) == None);
    }
}
