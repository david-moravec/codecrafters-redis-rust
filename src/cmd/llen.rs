use crate::frame::Frame;
use crate::parser::Parse;

#[derive(Debug)]
pub struct LLen {
    key: String,
}

impl LLen {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let key = parse.next_string()?;

        Ok(LLen { key })
    }
    pub fn apply(self, db: &crate::db::Db) -> anyhow::Result<Frame> {
        Ok(Frame::Integer(db.llen(self.key) as u64))
    }
}
