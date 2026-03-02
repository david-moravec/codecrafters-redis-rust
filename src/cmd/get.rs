use crate::frame::Frame;
use crate::parser::Parse;

pub struct Get {
    key: String,
}

impl Get {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        Ok(Get {
            key: parse.next_string()?,
        })
    }
    pub async fn apply(
        self,
        db: &crate::db::Db,
        dst: &mut crate::connection::Connection,
    ) -> anyhow::Result<()> {
        let frame = if let Some(bytes) = db.get(&self.key) {
            Frame::BulkString(bytes)
        } else {
            Frame::NullBulkString
        };

        dst.write_frame(&frame).await?;
        Ok(())
    }
}
