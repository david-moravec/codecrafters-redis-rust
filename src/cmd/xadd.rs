use crate::frame::Frame;
use crate::parser::{Parse, StreamEntryIDOpt};
use crate::stream::StreamEntry;

use bytes::Bytes;

pub struct XAdd {
    key: String,
    id: StreamEntryIDOpt,
    values: StreamEntry,
}

impl XAdd {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let key = parse.next_string()?;
        let id = parse.next_stream_id()?;

        let mut values = vec![];

        loop {
            values.push((parse.next_bytes()?, parse.next_bytes()?));

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
        use crate::stream::StreamError;

        let frame = match db.xadd(self.key, self.id, self.values) {
            Ok(id) => Frame::BulkString(Bytes::from(format!("{:}", id))),
            Err(StreamError::Other(e)) => Err(e)?,
            Err(e) => Frame::Error(format!("{:}", e)),
        };
        dst.write_frame(&frame).await?;
        Ok(())
    }
}
