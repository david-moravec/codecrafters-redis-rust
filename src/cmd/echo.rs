use crate::frame::Frame;
use crate::parser::Parse;

use bytes::Bytes;

#[derive(Debug)]
pub struct Echo {
    line: String,
}

impl Echo {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        Ok(Echo {
            line: parse.next_string()?,
        })
    }
    pub fn apply(self, _: &crate::db::Db) -> anyhow::Result<Frame> {
        let frame = Frame::BulkString(Bytes::copy_from_slice(self.line.as_bytes()));
        Ok(frame)
    }
}
