use crate::frame::Frame;
use crate::parser::Parse;

use bytes::Bytes;

#[derive(Debug)]
pub struct GeoDist {
    key: String,
    member1: Bytes,
    member2: Bytes,
}

impl GeoDist {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let key = parse.next_string()?;
        let member1 = parse.next_bytes()?;
        let member2 = parse.next_bytes()?;

        Ok(GeoDist {
            key,
            member1,
            member2,
        })
    }
    pub fn apply(self, db: &crate::db::Db) -> anyhow::Result<Frame> {
        let frame = match db.geodist(&self.key, &self.member1, &self.member2) {
            Some(dist) => Frame::BulkString(Bytes::from(format!("{:.4}", dist))),
            None => Frame::NullBulkString,
        };

        Ok(frame)
    }
}
