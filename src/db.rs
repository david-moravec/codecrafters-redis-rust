use bytes::{Buf, Bytes};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{
    cmp::max,
    collections::HashMap,
    sync::Mutex,
    time::{Duration, Instant},
};
use tokio::sync::Notify;
use tokio::sync::oneshot;

use crate::rdb_file::RdbFile;
use crate::stream::{Stream, StreamEntry, StreamEntryID, StreamError, XRange, XRead};
use crate::zset::{ZSet, ZSetError};
use thiserror::Error;

#[derive(Debug)]
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

#[derive(Debug, Error)]
pub enum CommandError {
    #[error("ERR value is not an integer or out of range")]
    NotANumber,
    #[error("{0}")]
    StreamError(#[from] StreamError),
    #[error(transparent)]
    EntryError(#[from] DbEntryError),
}

pub type CommandResult<T> = Result<T, CommandError>;

#[derive(Debug)]
pub struct XReadWaiter {
    id: StreamEntryID,
    tx: oneshot::Sender<XRead>,
}

#[derive(Debug, Error)]
pub enum DbEntryError {
    #[error("ERR entry is not of expected type ({0})")]
    WrongEntryType(&'static str),
    #[error(transparent)]
    Other(#[from] ZSetError),
}

type DbEntryResult<T> = Result<T, DbEntryError>;

#[derive(Debug)]
pub enum DbEntry {
    Single(Bytes),
    List(Vec<Bytes>),
    Stream(Stream),
    ZSet(ZSet),
}

impl DbEntry {
    fn remove(&mut self, index: usize) -> Option<Bytes> {
        if let Self::List(l) = self {
            Some(l.remove(index))
        } else {
            None
        }
    }

    pub(crate) fn get(&self) -> Option<Bytes> {
        if let Self::Single(b) = self {
            Some(b.clone())
        } else {
            None
        }
    }

    fn set(&mut self, value: Bytes) -> DbEntryResult<()> {
        if let Self::Single(_) = self {
            *self = DbEntry::Single(value);
            Ok(())
        } else {
            Err(DbEntryError::WrongEntryType("single"))
        }
    }

    fn push(&mut self, value: Bytes) -> DbEntryResult<()> {
        if let Self::List(l) = self {
            l.push(value);
            Ok(())
        } else {
            Err(DbEntryError::WrongEntryType("list"))
        }
    }

    fn insert(&mut self, index: usize, value: Bytes) -> DbEntryResult<()> {
        if let Self::List(l) = self {
            l.insert(index, value);
            Ok(())
        } else {
            Err(DbEntryError::WrongEntryType("list"))
        }
    }

    fn len(&self) -> DbEntryResult<usize> {
        if let Self::List(l) = self {
            Ok(l.len())
        } else {
            Err(DbEntryError::WrongEntryType("list"))
        }
    }

    fn xadd(
        &mut self,
        id: Bytes,
        values: StreamEntry,
    ) -> DbEntryResult<Result<StreamEntryID, StreamError>> {
        if let Self::Stream(s) = self {
            Ok(s.xadd(id, values))
        } else {
            Err(DbEntryError::WrongEntryType("stream"))
        }
    }

    fn xread(&self, id: &StreamEntryID) -> DbEntryResult<Result<XRange, StreamError>> {
        if let Self::Stream(s) = self {
            Ok(s.xread(id))
        } else {
            Err(DbEntryError::WrongEntryType("stream"))
        }
    }

    fn xrange(
        &self,
        start_id: &Bytes,
        end_id: &Bytes,
    ) -> DbEntryResult<Result<XRange, StreamError>> {
        if let Self::Stream(s) = self {
            Ok(s.xrange(start_id, end_id))
        } else {
            Err(DbEntryError::WrongEntryType("stream"))
        }
    }

    fn zadd(&mut self, score: f64, member: Bytes) -> DbEntryResult<usize> {
        if let Self::ZSet(s) = self {
            Ok(s.zadd(score, member))
        } else {
            Err(DbEntryError::WrongEntryType("set"))
        }
    }

    fn zrank(&self, member: Bytes) -> DbEntryResult<Option<usize>> {
        if let Self::ZSet(s) = self {
            Ok(s.zrank(member))
        } else {
            Err(DbEntryError::WrongEntryType("set"))
        }
    }

    fn zcard(&self) -> DbEntryResult<usize> {
        if let Self::ZSet(s) = self {
            Ok(s.zcard())
        } else {
            Err(DbEntryError::WrongEntryType("set"))
        }
    }

    fn zscore(&self, member: &Bytes) -> DbEntryResult<Option<f64>> {
        if let Self::ZSet(s) = self {
            Ok(s.zscore(member))
        } else {
            Err(DbEntryError::WrongEntryType("set"))
        }
    }

    fn zrem(&mut self, member: Bytes) -> DbEntryResult<usize> {
        if let Self::ZSet(s) = self {
            Ok(s.zrem(member))
        } else {
            Err(DbEntryError::WrongEntryType("set"))
        }
    }

    fn geoadd(&mut self, longitude: f64, latitude: f64, member: Bytes) -> DbEntryResult<usize> {
        if let Self::ZSet(s) = self {
            Ok(s.geoadd(longitude, latitude, member)?)
        } else {
            Err(DbEntryError::WrongEntryType("set"))
        }
    }

    fn geopos(&self, members: Vec<Bytes>) -> DbEntryResult<Vec<Option<(f64, f64)>>> {
        if let Self::ZSet(s) = self {
            Ok(s.geopos(members))
        } else {
            Err(DbEntryError::WrongEntryType("set"))
        }
    }

    fn geodist(&self, member1: &Bytes, member2: &Bytes) -> DbEntryResult<f64> {
        if let Self::ZSet(s) = self {
            Ok(s.geodist(member1, member2))
        } else {
            Err(DbEntryError::WrongEntryType("set"))
        }
    }

    fn geosearch(&self, lon: f64, lat: f64, radius: f64) -> DbEntryResult<Vec<Bytes>> {
        if let Self::ZSet(s) = self {
            Ok(s.geosearch(lon, lat, radius))
        } else {
            Err(DbEntryError::WrongEntryType("set"))
        }
    }
}

#[derive(Debug)]
struct State {
    db: HashMap<String, DbEntry>,
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
            expiry: HashMap::new(),
            blpop_waiters: HashMap::new(),
            blpop_waiters_ready: vec![],
            xread_waiters: HashMap::new(),
            xread_waiters_ready: vec![],
        }
    }

    fn from_rdb_file(rdb_file: RdbFile) -> Self {
        let mut expiry = HashMap::new();
        for (key, dur) in rdb_file.expiry.into_iter() {
            let now = Instant::now();
            let duration = dur - SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
            let e = Expiry { now, duration };
            expiry.insert(key, e);
        }

        Self {
            db: rdb_file.db,
            expiry,
            blpop_waiters: HashMap::new(),
            blpop_waiters_ready: vec![],
            xread_waiters: HashMap::new(),
            xread_waiters_ready: vec![],
        }
    }
}

#[derive(Debug)]
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
                let data = {
                    if let DbEntry::List(l) = state.db.get(&key).unwrap()
                        && l.len() > 0
                    {
                        l[0].clone()
                    } else {
                        continue;
                    }
                };

                match state.blpop_waiters.get_mut(&key) {
                    Some(waiters) => {
                        if waiters.len() == 0 {
                            break;
                        }

                        match waiters.remove(0).send(data) {
                            Ok(()) => {
                                // Sending was succesful remove the data
                                state.db.get_mut(&key).unwrap().remove(0);
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
                i += 1;

                let waiter = match state.xread_waiters.get(&key).unwrap().get(i - 1) {
                    Some(w) => w,
                    None => break,
                };

                let xrange = state
                    .db
                    .get(&key)
                    .unwrap()
                    .xread(&waiter.id)
                    .unwrap()
                    .unwrap();

                if xrange.entries_len() == 0 {
                    continue;
                }

                let xread = XRead::new(vec![(key.clone(), xrange)]);
                let waiter = state.xread_waiters.get_mut(&key).unwrap().remove(i - 1);

                match waiter.tx.send(xread.clone()) {
                    Ok(()) => {}
                    Err(_) => {}
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct Db {
    shared: Arc<Shared>,
}

impl Db {
    pub fn from_rdb_file(file_path: &std::path::Path) -> Self {
        let mut rdb_file = RdbFile::new();
        let state = match rdb_file.parse(file_path) {
            Ok(()) => Mutex::new(State::from_rdb_file(rdb_file)),

            Err(err) => {
                eprintln!("During parsing of rdb file error occured; {:}", err);
                Mutex::new(State::new())
            }
        };
        let shared = Arc::new(Shared {
            state,
            blpop_waiters_background_task: Notify::new(),
            xread_waiters_background_task: Notify::new(),
        });
        Db::init_background_processes(shared.clone());

        Db { shared }
    }

    fn init_background_processes(shared: Arc<Shared>) {
        tokio::spawn(blpop_waiters_background_process(shared.clone()));
        tokio::spawn(xread_waiters_background_process(shared.clone()));
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

        state.db.get(key).map(|b| b.get()).flatten()
    }

    pub fn set(&self, key: String, value: Bytes, expire: Option<Duration>) {
        let mut state = self.shared.state.lock().unwrap();

        if let Some(expire) = expire {
            state.expiry.insert(key.clone(), Expiry::from(expire));
        }

        state.db.insert(key, DbEntry::Single(value));
    }

    pub fn keys(&self, pattern: &str) -> Vec<String> {
        let state = self.shared.state.lock().unwrap();

        if pattern == "*" {
            state.db.keys().cloned().collect()
        } else {
            state
                .db
                .keys()
                .filter(|k| k.as_str() == pattern)
                .cloned()
                .collect()
        }
    }

    pub fn incr(&self, key: String) -> CommandResult<u64> {
        let mut state = self.shared.state.lock().unwrap();

        let entry = state
            .db
            .entry(key)
            .or_insert(DbEntry::Single(Bytes::from("0")));

        use atoi::atoi;

        let mut number =
            atoi::<u64>(entry.get().unwrap().chunk()).ok_or(CommandError::NotANumber)?;
        number += 1;
        entry.set(Bytes::from(format!("{:}", number)))?;

        Ok(number)
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

        let list = state.db.entry(key).or_insert_with(|| DbEntry::List(vec![]));

        for value in values {
            list.push(value).unwrap();
        }

        list.len().unwrap()
    }

    pub fn lpush(&self, key: String, values: Vec<Bytes>) -> usize {
        self.notify_blpop_waiters(&key);

        let mut state = self.shared.state.lock().unwrap();

        let list = state.db.entry(key).or_insert_with(|| DbEntry::List(vec![]));

        for value in values {
            list.insert(0, value).unwrap();
        }

        list.len().unwrap()
    }

    pub fn lpop(&self, key: &str, start: Option<i64>, stop: Option<i64>) -> Option<Vec<Bytes>> {
        let mut state = self.shared.state.lock().unwrap();

        let list = match state.db.get_mut(key) {
            Some(l) => l,
            None => return None,
        };

        if start.is_none() && stop.is_none() {
            return Some(vec![list.remove(0).unwrap()]);
        }

        let list_len = list.len().unwrap();

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
            result.push(list.remove(0).unwrap())
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
        let list = state.db.entry(key).or_insert_with(|| DbEntry::List(vec![]));

        list.len().unwrap()
    }

    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Vec<Bytes> {
        let state = self.shared.state.lock().unwrap();
        let list = match state.db.get(key) {
            Some(DbEntry::List(l)) => l,
            Some(_) | None => return vec![],
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

        match state.db.get(key) {
            Some(entry) => match entry {
                DbEntry::Single(_) => "string",
                DbEntry::List(_) => "list",
                DbEntry::Stream(_) => "stream",
                DbEntry::ZSet(_) => "zset",
            },
            None => "none",
        }
        .to_string()
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
        id: Bytes,
        values: StreamEntry,
    ) -> Result<StreamEntryID, StreamError> {
        let id = {
            let mut state = self.shared.state.lock().unwrap();
            let stream = state
                .db
                .entry(key.clone())
                .or_insert(DbEntry::Stream(Stream::new()));
            stream.xadd(id, values).unwrap()
        };
        self.notify_xread_waiters(&key);
        id
    }

    pub fn xrange(&self, key: String, start: &Bytes, end: &Bytes) -> Result<XRange, StreamError> {
        let mut state = self.shared.state.lock().unwrap();
        let stream = state
            .db
            .entry(key)
            .or_insert(DbEntry::Stream(Stream::new()));

        stream.xrange(start, end).unwrap()
    }

    pub fn xread(
        &self,
        timeout: Option<u64>,
        keys: Vec<String>,
        ids: Vec<Bytes>,
    ) -> Result<(Option<XRead>, Option<oneshot::Receiver<XRead>>), StreamError> {
        let mut state = self.shared.state.lock().unwrap();
        let mut xread_entries = vec![];

        let mut rx_opt = None;

        for (key, id) in keys.into_iter().zip(ids.into_iter()) {
            if state.db.contains_key(&key) {
                if let DbEntry::Stream(stream) = state.db.get(&key).unwrap() {
                    let entry_id = stream.generate_id(&id)?;
                    let xrange = stream.xread(&entry_id)?;

                    if xrange.entries_len() == 0 && timeout.is_some() {
                        let (tx, rx) = oneshot::channel();
                        state
                            .xread_waiters
                            .entry(key.clone())
                            .or_insert(vec![])
                            .push(XReadWaiter { id: entry_id, tx });
                        rx_opt = Some(rx);

                        return Ok((None, rx_opt));
                    }

                    xread_entries.push((key, xrange));
                };
            }
        }

        Ok((Some(XRead::new(xread_entries)), rx_opt))
    }

    pub fn zadd(&self, key: String, score: f64, member: Bytes) -> usize {
        let mut state = self.shared.state.lock().unwrap();

        let set = state
            .db
            .entry(key)
            .or_insert_with(|| DbEntry::ZSet(ZSet::new()));

        set.zadd(score, member).unwrap()
    }

    pub fn geoadd(
        &self,
        key: String,
        longitude: f64,
        latitude: f64,
        member: Bytes,
    ) -> CommandResult<usize> {
        let mut state = self.shared.state.lock().unwrap();

        let set = state
            .db
            .entry(key)
            .or_insert_with(|| DbEntry::ZSet(ZSet::new()));

        Ok(set.geoadd(longitude, latitude, member)?)
    }

    pub fn zrank(&self, key: String, member: Bytes) -> Option<usize> {
        let state = self.shared.state.lock().unwrap();

        state
            .db
            .get(&key)
            .map(|e| e.zrank(member).unwrap())
            .flatten()
    }

    pub fn zrange(&self, key: String, start: i64, stop: i64) -> Vec<Bytes> {
        let state = self.shared.state.lock().unwrap();
        match state.db.get(&key) {
            Some(DbEntry::ZSet(set)) => set.zrange(start, stop),
            Some(_) | None => return vec![],
        }
    }

    pub fn zcard(&self, key: String) -> usize {
        let mut state = self.shared.state.lock().unwrap();
        let set_entry = state
            .db
            .entry(key)
            .or_insert_with(|| DbEntry::ZSet(ZSet::new()));

        set_entry.zcard().unwrap()
    }

    pub fn zscore(&self, key: String, member: Bytes) -> Option<f64> {
        let state = self.shared.state.lock().unwrap();
        state
            .db
            .get(&key)
            .map(|e| e.zscore(&member).unwrap())
            .flatten()
    }

    pub fn zrem(&self, key: String, member: Bytes) -> usize {
        let mut state = self.shared.state.lock().unwrap();

        match state.db.get_mut(&key) {
            Some(entry) => entry.zrem(member).unwrap(),
            None => 0,
        }
    }

    pub fn geopos(&self, key: &str, members: Vec<Bytes>) -> Vec<Option<(f64, f64)>> {
        let state = self.shared.state.lock().unwrap();

        match state.db.get(key) {
            Some(entry) => entry.geopos(members).unwrap(),
            None => members.into_iter().map(|_| None).collect(),
        }
    }

    pub fn geodist(&self, key: &str, member1: &Bytes, member2: &Bytes) -> Option<f64> {
        let state = self.shared.state.lock().unwrap();

        match state.db.get(key) {
            Some(entry) => entry.geodist(member1, member2).ok(),
            None => None,
        }
    }

    pub fn geosearch(&self, key: &str, lon: f64, lat: f64, radius: f64) -> Vec<Bytes> {
        let state = self.shared.state.lock().unwrap();

        match state.db.get(key) {
            Some(entry) => entry.geosearch(lon, lat, radius).unwrap(),
            None => vec![],
        }
    }

    pub fn to_rdb_file(&self) -> Bytes {
        Bytes::from(hex::decode(
            "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2",
        ).unwrap())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_db_from_rdb_file() {
        let db = Db::from_rdb_file(std::path::Path::new("dump.rdb"));

        assert!(db.get("baz") == Some(Bytes::from("jazz".to_string())));
    }
}
