use bytes::Bytes;
use std::time::Duration;
use tokio::time::timeout;

use crate::frame::Frame;
use crate::parser::Parse;

pub struct BLPop {
    key: String,
    timeout: u64,
}

impl BLPop {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let key = parse.next_string()?;
        let timeout = parse.next_u64()?;

        Ok(BLPop { key, timeout })
    }
    pub async fn apply(
        self,
        db: &crate::db::Db,
        dst: &mut crate::connection::Connection,
    ) -> anyhow::Result<()> {
        let (value, rx) = db.blpop(self.key.clone());

        let value = match (value, rx) {
            (Some(value), None) => Some(value),
            (None, Some(rx)) => {
                if self.timeout == 0 {
                    Some(rx.await?)
                } else {
                    let res = timeout(Duration::from_secs(self.timeout), rx).await;
                    match res {
                        Ok(v) => Some(v?),
                        Err(_) => None,
                    }
                }
            }
            _ => unreachable!(),
        };

        let frame = Frame::Array(Some(vec![
            Frame::BulkString(Bytes::copy_from_slice(self.key.as_bytes())),
            {
                if let Some(bytes) = value {
                    Frame::BulkString(bytes)
                } else {
                    Frame::NullBulkString
                }
            },
        ]));

        dst.write_frame(&frame).await?;
        Ok(())
    }
}
