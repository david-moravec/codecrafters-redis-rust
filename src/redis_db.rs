use core::time;
use std::{
    cell::RefCell,
    collections::HashMap,
    ops::Range,
    sync::{Mutex, mpsc},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Result, anyhow};

use crate::redis_parser::{Aggregate, NULL_STRING, RESPData, RESPMap};

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

impl TryFrom<(&RESPData, &RESPData)> for Expiry {
    type Error = anyhow::Error;

    fn try_from(value: (&RESPData, &RESPData)) -> Result<Self> {
        let duration: u64;

        if let RESPData::Aggregate(Aggregate::BulkString(Some(b))) = value.1 {
            // duration = str::from_utf8(&b[..])?.parse::<u64>()?;
            duration = value.1.try_bulk_string_to_int()? as u64;

            if let RESPData::Aggregate(Aggregate::BulkString(Some(s))) = value.0 {
                if str::from_utf8(&s[..]).map(|s| s.to_uppercase()) == Ok("PX".to_string()) {
                    Ok(Expiry::from(Duration::from_millis(duration)))
                } else if str::from_utf8(&s[..]).map(|s| s.to_uppercase()) == Ok("EX".to_string()) {
                    Ok(Expiry::from(Duration::from_secs(duration)))
                } else {
                    Err(anyhow!(
                        "Unknown duration precision specifier {:?}",
                        String::from_utf8(s.clone())
                    ))
                }
            } else {
                Err(anyhow!(
                    "Expected duration precision specifier, got {}",
                    value.0.serialize()
                ))
            }
        } else {
            Err(anyhow!(
                "Exected duration as the second arg, got {}",
                value.1.serialize()
            ))
        }
    }
}

type Db = HashMap<RESPData, RESPData>;
type ListDb = HashMap<RESPData, Vec<RESPData>>;

pub struct RedisDB {
    db: Db,
    list_db: Mutex<ListDb>,
    expiry: HashMap<RESPData, Expiry>,
    waiters: Mutex<HashMap<RESPData, Vec<mpsc::Sender<RESPData>>>>,
}

impl RedisDB {
    pub fn new() -> Self {
        Self {
            db: Db::new(),
            list_db: Mutex::new(ListDb::new()),
            expiry: HashMap::new(),
            waiters: Mutex::new(HashMap::new()),
        }
    }

    pub fn insert(
        &mut self,
        key: RESPData,
        value: RESPData,
        expiry: Option<(&RESPData, &RESPData)>,
    ) -> Result<Option<RESPData>> {
        if let Some(d) = expiry {
            self.expiry.insert(key.clone(), Expiry::try_from(d)?);
        }

        Ok(self.db.insert(key, value))
    }

    pub fn push(&mut self, key: &RESPData, value: RESPData) -> u64 {
        if self
            .with_waiters_mut(key, |vec_tx| vec_tx.remove(0).send(value.clone()))
            .is_some()
        {
            return self.with_list(key, |l| l.len() as u64).unwrap_or(0);
        }

        self.with_list_mut_create_default(key, |list| {
            list.push(value);
            list.len() as u64
        })
    }

    pub fn lpush(&mut self, key: &RESPData, value: RESPData) -> u64 {
        if self
            .with_waiters_mut(key, |vec_tx| vec_tx.remove(0).send(value.clone()))
            .is_some()
        {
            return self.with_list(key, |l| l.len() as u64).unwrap_or(0);
        }

        self.with_list_mut_create_default(key, |list| {
            list.insert(0, value);
            list.len() as u64
        })
    }

    pub fn push_many(&mut self, key: &RESPData, values: Vec<RESPData>) -> u64 {
        let mut result = 0;

        for value in values.into_iter() {
            result = self.push(key, value);
        }

        result
    }

    pub fn lpush_many(&mut self, key: &RESPData, values: Vec<RESPData>) -> u64 {
        let mut result = 0;

        for value in values.into_iter() {
            result = self.lpush(key, value);
        }

        result
    }

    pub fn get(&mut self, key: &RESPData) -> &RESPData {
        if let Some(expiry) = self.expiry.get(key) {
            if expiry.expired() {
                self.db.remove(key);
                self.expiry.remove(key);

                return &NULL_STRING;
            }
        }

        self.db.get(key).unwrap_or(&NULL_STRING)
    }

    pub fn with_list<F, R>(&self, key: &RESPData, f: F) -> Option<R>
    where
        F: FnOnce(&Vec<RESPData>) -> R,
    {
        self.list_db.lock().expect("Lock failed").get(key).map(f)
    }

    pub fn with_list_mut<F, R>(&mut self, key: &RESPData, f: F) -> Option<R>
    where
        F: FnOnce(&mut Vec<RESPData>) -> R,
    {
        self.list_db
            .lock()
            .expect("Lock failed")
            .get_mut(key)
            .map(f)
    }

    pub fn with_list_mut_create_default<F, R>(&mut self, key: &RESPData, f: F) -> R
    where
        F: FnOnce(&mut Vec<RESPData>) -> R,
    {
        let mut locked_db = self.list_db.lock().expect("Lock failed");

        if !locked_db.contains_key(key) {
            locked_db.insert(key.clone(), vec![]);
        }

        locked_db.get_mut(key).map(f).unwrap()
    }

    fn with_waiters_mut_create_default<F, R>(&mut self, key: &RESPData, f: F) -> R
    where
        F: FnOnce(&mut Vec<mpsc::Sender<RESPData>>) -> R,
    {
        let mut locked_db = self.waiters.lock().expect("Lock failed");

        if !locked_db.contains_key(key) {
            locked_db.insert(key.clone(), vec![]);
        }

        locked_db.get_mut(key).map(f).unwrap()
    }

    fn with_waiters_mut<F, R>(&mut self, key: &RESPData, f: F) -> Option<R>
    where
        F: FnOnce(&mut Vec<mpsc::Sender<RESPData>>) -> R,
    {
        self.waiters
            .lock()
            .expect("Lock failed")
            .get_mut(key)
            .map(f)
    }

    pub fn list_len(&self, key: &RESPData) -> RESPData {
        match self.with_list(key, |l| l.len()) {
            Some(len) => RESPData::from(len as u64),
            None => RESPData::from(0),
        }
    }

    pub fn list_pop(&mut self, key: &RESPData) -> RESPData {
        let removed = self.with_list_mut(key, |list| list.remove(0));

        match removed {
            Some(e) => e,
            None => NULL_STRING.clone(),
        }
    }

    pub fn list_pop_many(&mut self, key: &RESPData, count: u64) -> RESPData {
        let mut result: Vec<RESPData> = vec![];

        for _ in 0..count {
            result.push(self.list_pop(key))
        }

        RESPData::from_iter(result.iter())
    }

    pub fn list_range(
        &self,
        key: &RESPData,
        start: &RESPData,
        stop: &RESPData,
    ) -> Result<RESPData> {
        if let Some(list_len) = self.with_list(key, |l| l.len()) {
            let start = start.try_bulk_string_to_int()?;
            let stop = stop.try_bulk_string_to_int()?;

            let start = {
                if start < 0 {
                    let mut a = list_len as i128 + start;
                    if a < 0 {
                        a = 0;
                    }
                    a as usize
                } else {
                    start as usize
                }
            };

            let mut stop = {
                if stop < 0 {
                    let mut a = list_len as i128 + stop;
                    if a < 0 {
                        a = 0;
                    }
                    a as usize
                } else {
                    stop as usize
                }
            };

            if start >= list_len || start > stop {
                return Ok(RESPData::from_iter([].iter()));
            } else if stop >= list_len {
                stop = list_len - 1;
            }

            Ok(RESPData::from_iter(
                self.with_list(key, |l| l[start..=stop].to_vec())
                    .unwrap()
                    .iter(),
            ))
        } else {
            Ok(RESPData::from_iter([].iter()))
        }
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
            RESPData::bulk_string("key"),
            RESPData::bulk_string("ahoj"),
            Some((&RESPData::bulk_string("PX"), &RESPData::bulk_string("10"))),
        )
        .unwrap();

        assert!(db.get(&RESPData::bulk_string("key")) == &RESPData::bulk_string("ahoj"));

        sleep(Duration::from_millis(10));

        assert!(db.get(&RESPData::bulk_string("key")) == &NULL_STRING);
    }

    #[test]
    fn test_expiry_try_from() {
        let duration_resp = (&RESPData::bulk_string("PX"), &RESPData::bulk_string("10"));
        let expiry = Expiry::try_from(duration_resp).unwrap();
        assert!(expiry.duration == Duration::from_millis(10));

        let duration_resp = (&RESPData::bulk_string("EX"), &RESPData::bulk_string("18"));
        let expiry = Expiry::try_from(duration_resp).unwrap();
        assert!(expiry.duration == Duration::from_secs(18));
    }

    #[test]
    fn test_push() {
        let mut db = RedisDB::new();
        let key = RESPData::bulk_string("list_key");
        let key1 = RESPData::bulk_string("list_key1");

        assert!(db.push(&key, NULL_STRING.clone()) == 1);
        assert!(db.push(&key, NULL_STRING.clone()) == 2);
        assert!(db.push(&key, NULL_STRING.clone()) == 3);

        assert!(db.push(&key1, NULL_STRING.clone()) == 1);
    }
}
