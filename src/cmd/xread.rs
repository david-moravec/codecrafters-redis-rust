use anyhow::anyhow;
use tokio::time::timeout;

use crate::frame::{Frame, ToFrame};
use crate::parser::{Parse, StreamEntryIDOpt};
use std::any::Any;
use std::time::Duration;

pub struct XRead {
    timeout: Option<u64>,
    keys: Vec<String>,
    ids: Vec<StreamEntryIDOpt>,
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
            println!("{:?}", bytes_vec[bytes_vec.len() - 1]);
            bytes_vec.push(parse.next_bytes()?);
            println!("{:?}", bytes_vec[bytes_vec.len() - 1]);
        }

        let bytes_vec_len = bytes_vec.len();

        let keys: Vec<String> = bytes_vec[..bytes_vec_len / 2]
            .iter()
            .map(|b| String::from_utf8(b.to_vec()).unwrap())
            .collect();
        let ids: Vec<StreamEntryIDOpt> = bytes_vec[bytes_vec_len / 2..bytes_vec_len]
            .into_iter()
            .map(|b| StreamEntryIDOpt::try_from(b.clone()).unwrap())
            .collect();

        Ok(XRead {
            timeout: timeout_opt,
            keys,
            ids,
        })
    }
    pub async fn apply(
        self,
        db: &crate::db::Db,
        dst: &mut crate::connection::Connection,
    ) -> anyhow::Result<()> {
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
            Some(xread) => xread.to_frame(),
            None => Frame::Array(None),
        };

        dst.write_frame(&frame).await?;
        Ok(())
    }
}
