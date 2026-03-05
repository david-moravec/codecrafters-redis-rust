use crate::db::StreamEntry;
use crate::frame::Frame;
use crate::parser::Parse;

use bytes::Bytes;

pub struct XAdd {
    key: String,
    id: Bytes,
    values: StreamEntry,
}

impl XAdd {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let key = parse.next_string()?;
        let id = parse.next_bytes()?;

        let mut values = vec![];

        loop {
            values.push((parse.next_string()?, parse.next_bytes()?));

            if parse.end_of_stream_reached() {
                break;
            }
        }

        Ok(XAdd { key, id, values })
    }
    pub async fn apply(
        self,
        db: &crate::db::Db,
        dst: &mut crate::connection::Connection,
    ) -> anyhow::Result<()> {
        db.xadd(self.key, self.id.clone(), self.values);
        dst.write_frame(&Frame::BulkString(self.id)).await?;
        Ok(())
    }
}
