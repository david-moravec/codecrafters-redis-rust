use crate::frame::Frame;
use crate::parser::Parse;

#[derive(Debug)]
pub struct ZCard {
    key: String,
}

impl ZCard {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let key = parse.next_string()?;

        Ok(ZCard { key })
    }
    pub fn apply(self, db: &crate::db::Db) -> anyhow::Result<Frame> {
        Ok(Frame::Integer(db.zcard(self.key) as u64))
    }
}
