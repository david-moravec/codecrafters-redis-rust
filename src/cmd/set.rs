use crate::frame::Frame;
use crate::parser::Parse;

use bytes::Bytes;

pub struct Set {
    key: String,
    value: Bytes,
}

impl Set {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        Ok(Set {
            key: parse.next_string()?,
            value: parse.next_bytes()?,
        })
    }
    pub async fn apply(
        self,
        db: &crate::db::Db,
        dst: &mut crate::connection::Connection,
    ) -> anyhow::Result<()> {
        db.set(self.key, self.value);
        dst.write_frame(&Frame::Simple("OK".to_string())).await?;
        Ok(())
    }

    pub fn get_name(&self) -> &str {
        "set"
    }
}
