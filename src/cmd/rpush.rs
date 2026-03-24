use crate::frame::Frame;
use crate::parser::Parse;

use bytes::Bytes;

#[derive(Debug)]
pub struct RPush {
    key: String,
    values: Vec<Bytes>,
}

impl RPush {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let key = parse.next_string()?;

        let mut values = vec![];

        while let Ok(bytes) = parse.next_bytes() {
            values.push(bytes);
        }

        Ok(RPush { key, values })
    }
    pub fn apply(self, db: &crate::db::Db) -> anyhow::Result<Frame> {
        let len = db.rpush(self.key, self.values);
        Ok(Frame::Integer(len as u64))
    }
}
