use crate::frame::Frame;
use crate::parser::Parse;

#[derive(Debug)]
pub struct Get {
    key: String,
}

impl Get {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        Ok(Get {
            key: parse.next_string()?,
        })
    }
    pub fn apply(self, db: &crate::db::Db) -> anyhow::Result<Frame> {
        let frame = if let Some(bytes) = db.get(&self.key) {
            Frame::BulkString(bytes)
        } else {
            Frame::NullBulkString
        };

        Ok(frame)
    }
}
