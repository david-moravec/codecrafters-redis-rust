use crate::frame::Frame;
use crate::parser::Parse;

use bytes::Bytes;

#[derive(Debug)]
pub struct ZAdd {
    key: String,
    score: f64,
    member: Bytes,
}

impl ZAdd {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let key = parse.next_string()?;
        let score = parse.next_f64()?;

        let member = Bytes::from(parse.next_string()?);

        Ok(ZAdd { key, score, member })
    }
    pub fn apply(self, db: &crate::db::Db) -> anyhow::Result<Frame> {
        let frame = Frame::Integer(db.zadd(self.key, self.score, self.member) as u64);
        Ok(frame)
    }
}
