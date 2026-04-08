use crate::frame::Frame;
use crate::parser::Parse;

use bytes::Bytes;

#[derive(Debug)]
pub struct ZRem {
    key: String,
    member: Bytes,
}

impl ZRem {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let key = parse.next_string()?;

        let member = Bytes::from(parse.next_string()?);

        Ok(ZRem { key, member })
    }
    pub fn apply(self, db: &crate::db::Db) -> anyhow::Result<Frame> {
        let frame = Frame::Integer(db.zrem(self.key, self.member) as u64);
        Ok(frame)
    }
}
