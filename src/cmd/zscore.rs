use crate::frame::Frame;
use crate::parser::Parse;

use bytes::Bytes;

#[derive(Debug)]
pub struct ZScore {
    key: String,
    member: Bytes,
}

impl ZScore {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let key = parse.next_string()?;

        let member = Bytes::from(parse.next_string()?);

        Ok(ZScore { key, member })
    }
    pub fn apply(self, db: &crate::db::Db) -> anyhow::Result<Frame> {
        let frame = match db.zscore(self.key, self.member) {
            Some(score) => Frame::BulkString(Bytes::from(format!("{:}", score))),
            None => Frame::NullBulkString,
        };
        Ok(frame)
    }
}
