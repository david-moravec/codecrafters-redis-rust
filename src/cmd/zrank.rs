use crate::frame::Frame;
use crate::parser::Parse;

use bytes::Bytes;

#[derive(Debug)]
pub struct ZRank {
    key: String,
    member: Bytes,
}

impl ZRank {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let key = parse.next_string()?;

        let member = Bytes::from(parse.next_string()?);

        Ok(ZRank { key, member })
    }
    pub fn apply(self, db: &crate::db::Db) -> anyhow::Result<Frame> {
        let frame = match db.zrank(self.key, self.member) {
            Some(rank) => Frame::Integer(rank as u64),
            None => Frame::NullBulkString,
        };
        Ok(frame)
    }
}
