use crate::frame::Frame;
use crate::parser::Parse;
use crate::stream::StreamEntry;

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
            values.push((parse.next_bytes()?, parse.next_bytes()?));

            if parse.end_of_stream_reached() {
                break;
            }
        }

        Ok(XAdd {
            key,
            id,
            values: StreamEntry::new(values),
        })
    }
    pub fn apply(self, db: &crate::db::Db) -> anyhow::Result<Frame> {
        use crate::stream::StreamError;

        let frame = match db.xadd(self.key, self.id, self.values) {
            Ok(id) => Frame::BulkString(Bytes::from(format!("{:}", id))),
            Err(StreamError::Other(e)) => Err(e)?,
            Err(e) => Frame::Error(format!("{:}", e)),
        };
        Ok(frame)
    }
}
