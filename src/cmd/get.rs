use std::io::Cursor;

use crate::parser::Parse;

use crate::frame::Frame;

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
        &self,
        db: &crate::db::Db,
        dst: &mut crate::connection::Connection,
    ) -> anyhow::Result<()> {
        let frame = if let Some(bytes) = db.get(&self.key) {
            let mut cursor = Cursor::new(&bytes[..]);
            Frame::parse(&mut cursor)?
        } else {
            Frame::Null
        };

        dst.write_frame(&frame).await?;
        Ok(())
    }

    pub fn get_name(&self) -> &str {
        "get"
    }
}
