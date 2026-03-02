use crate::frame::Frame;
use crate::parser::Parse;

use bytes::Bytes;

pub struct LLen {
    key: String,
}

impl LLen {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let key = parse.next_string()?;

        Ok(LLen { key })
    }
    pub async fn apply(
        self,
        db: &crate::db::Db,
        dst: &mut crate::connection::Connection,
    ) -> anyhow::Result<()> {
        let len = db.llen(self.key);
        dst.write_frame(&Frame::Integer(len as u64)).await?;
        Ok(())
    }
}
