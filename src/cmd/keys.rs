use bytes::Bytes;

use crate::frame::Frame;
use crate::parser::Parse;

#[derive(Debug)]
pub struct Keys {
    pattern: String,
}

impl Keys {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        Ok(Keys {
            pattern: parse.next_string()?,
        })
    }
    pub fn apply(self, db: &crate::db::Db) -> anyhow::Result<Frame> {
        let frame = Frame::bulk_strings_array(
            db.keys(&self.pattern)
                .into_iter()
                .map(|s| Bytes::from(s))
                .collect(),
        );

        Ok(frame)
    }
}
