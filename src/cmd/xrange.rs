use crate::frame::Frame;
use crate::parser::{Parse, StreamEntryIDOpt};

use bytes::Bytes;

pub struct XRange {
    key: String,
    start: StreamEntryIDOpt,
    stop: StreamEntryIDOpt,
}

impl XRange {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let key = parse.next_string()?;
        let start = parse.next_stream_id()?;
        let stop = parse.next_stream_id()?;

        Ok(XRange { key, start, stop })
    }
    pub async fn apply(
        self,
        db: &crate::db::Db,
        dst: &mut crate::connection::Connection,
    ) -> anyhow::Result<()> {
        let vec_id_vec_key_value = db.xrange(self.key, self.start, self.stop)?;
        let frame = Frame::Array(Some(
            vec_id_vec_key_value
                .into_iter()
                .map(|id_vec_key_value| {
                    Frame::Array(Some({
                        vec![
                            Frame::BulkString(Bytes::from(format!("{:}", id_vec_key_value.0))),
                            Frame::Array(Some(
                                id_vec_key_value
                                    .1
                                    .into_iter()
                                    .flat_map(|(key, value)| {
                                        [Frame::BulkString(key), Frame::BulkString(value)]
                                            .into_iter()
                                    })
                                    .collect(),
                            )),
                        ]
                    }))
                })
                .collect(),
        ));

        dst.write_frame(&frame).await?;
        Ok(())
    }
}
