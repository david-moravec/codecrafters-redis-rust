use crate::db::StreamEntry;
use crate::frame::Frame;
use crate::parser::{Parse, StreamIDOpt};

use bytes::Bytes;

pub struct XAdd {
    key: String,
    id: StreamIDOpt,
    values: StreamEntry,
}

impl XAdd {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let key = parse.next_string()?;
        let id = parse.next_stream_id()?;

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
        use crate::db::StreamError;

        let frame = match db.xadd(self.key, self.id, self.values) {
            Ok(id) => Frame::BulkString(Bytes::copy_from_slice(format!("{:}", id).as_bytes())),
            Err(StreamError::ZeroZeroID) => Frame::Error(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                    .to_string(),
            ),
            Err(StreamError::InvalidId) => {
                Frame::Error("ERR The ID specified in XADD must be greater than 0-0".to_string())
            }
            Err(StreamError::Other(e)) => Err(e)?,
            Err(_) => unreachable!(),
        };
        dst.write_frame(&frame).await?;
        Ok(())
    }
}
