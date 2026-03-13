use anyhow::anyhow;
use tokio::time::timeout;

use crate::frame::{Frame, IntoFrame};
use crate::parser::Parse;
use std::time::Duration;

use bytes::Bytes;

pub struct XRead {
    timeout: Option<u64>,
    keys: Vec<String>,
    ids: Vec<Bytes>,
}

impl XRead {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let timeout_opt = match parse.next_string()?.to_lowercase().as_str() {
            "block" => {
                let timeout = Some(parse.next_u64()?);
                // parse 'streams' option
                parse.next_string()?;
                timeout
            }
            "streams" => None,
            s => return Err(anyhow!("protocol error; unknown option {:} for XREAD", s)),
        };

        let mut bytes_vec = vec![];

        while let Ok(bytes) = parse.next_bytes() {
            bytes_vec.push(bytes);
            bytes_vec.push(parse.next_bytes()?);
        }

        let bytes_vec_len = bytes_vec.len();

        let keys: Vec<String> = bytes_vec
            .drain(0..bytes_vec_len / 2)
            .map(|b| String::from_utf8(b.to_vec()).unwrap())
            .collect();

        let ids: Vec<Bytes> = bytes_vec;

        Ok(XRead {
            timeout: timeout_opt,
            keys,
            ids,
        })
    }
    pub async fn apply(self, db: &crate::db::Db) -> anyhow::Result<Frame> {
        let xread = match db.xread(self.timeout, self.keys, self.ids)? {
            (Some(xread), None) => Some(xread),
            (None, Some(rx)) => {
                if self.timeout.unwrap() == 0 {
                    Some(rx.await?)
                } else {
                    let res = timeout(Duration::from_millis(self.timeout.unwrap()), rx).await;

                    match res {
                        Ok(xread) => Some(xread?),
                        Err(_) => None,
                    }
                }
            }
            _ => unreachable!(),
        };

        let frame = match xread {
            Some(xread) => xread.into_frame(),
            None => Frame::Array(None),
        };

        Ok(frame)
    }
}
