use crate::frame::Frame;
use crate::parser::Parse;

use bytes::Bytes;

pub struct RPush {
    key: String,
    values: Vec<Bytes>,
}

impl RPush {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let key = parse.next_string()?;

        let mut values = vec![];

        while let Ok(bytes) = parse.next_bytes() {
            values.push(bytes);
        }

        Ok(RPush { key, values })
    }
    pub async fn apply(
        self,
        db: &crate::db::Db,
        dst: &mut crate::connection::Connection,
    ) -> anyhow::Result<()> {
        let len = db.rpush(self.key, self.values);
        dst.write_frame(&Frame::Integer(len as u64)).await?;
        Ok(())
    }

    pub fn get_name(&self) -> &str {
        "rpush"
    }
}
