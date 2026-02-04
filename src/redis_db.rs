use core::time;
use std::{
    cell::RefCell,
    collections::HashMap,
    time::{Duration, Instant},
};

use anyhow::{Result, anyhow};

use crate::redis_parser::{Aggregate, RESPData, RESPMap};

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
            duration = str::from_utf8(&b[..])?.parse::<u64>()?;

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
    list_db: ListDb,
    expiry: HashMap<RESPData, Expiry>,
}

impl RedisDB {
    pub fn new() -> Self {
        Self {
            db: Db::new(),
            list_db: ListDb::new(),
            expiry: HashMap::new(),
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

    pub fn push(&mut self, key: RESPData, value: RESPData) -> u64 {
        let length;

        if let Some(v) = self.list_db.get_mut(&key) {
            v.push(value);
            length = v.len();
        } else {
            self.list_db.insert(key, vec![value]);
            length = 1;
        }

        length as u64
    }

    fn push_no_default(&mut self, key: &RESPData, value: RESPData) -> u64 {
        let v = self.list_db.get_mut(key).unwrap();
        v.push(value);

        v.len() as u64
    }

    pub fn push_many(&mut self, key: RESPData, mut values: Vec<RESPData>) -> u64 {
        let mut result: u64 = 0;

        let key_copy = key.clone();

        let mut result = self.push(key, values.remove(0));

        for value in values.into_iter() {
            result = self.push_no_default(&key_copy, value);
        }

        result
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
            Some((&RESPData::bulk_string("PX"), &RESPData::bulk_string("10"))),
        )
        .unwrap();

        assert!(db.get(&NULL_STRING) == Some(&NULL_STRING));

        sleep(Duration::from_millis(10));

        assert!(db.get(&NULL_STRING) == None);
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

        assert!(db.push(RESPData::bulk_string("list_key"), NULL_STRING.clone()) == 1);
        assert!(db.push(RESPData::bulk_string("list_key"), NULL_STRING.clone()) == 2);
        assert!(db.push(RESPData::bulk_string("list_key"), NULL_STRING.clone()) == 3);

        assert!(db.push(RESPData::bulk_string("list_key1"), NULL_STRING.clone()) == 1);
    }
}
