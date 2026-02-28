use crate::frame::Frame;
use crate::parser::Parse;

use bytes::Bytes;

pub struct Echo {
    line: String,
}

impl Echo {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        Ok(Echo {
            line: parse.next_string()?,
        })
    }
    pub async fn apply(
        self,
        db: &crate::db::Db,
        dst: &mut crate::connection::Connection,
    ) -> anyhow::Result<()> {
        let frame = Frame::BulkString(Bytes::copy_from_slice(self.line.as_bytes()));
        dst.write_frame(&frame).await?;
        Ok(())
    }

    pub fn get_name(&self) -> &str {
        "echo"
    }
}
