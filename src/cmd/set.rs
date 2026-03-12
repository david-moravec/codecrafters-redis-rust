use std::time::Duration;

use crate::frame::Frame;
use crate::parser::Parse;

use anyhow::anyhow;
use bytes::Bytes;

pub struct Set {
    key: String,
    value: Bytes,
    expire: Option<Duration>,
}

impl Set {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        use crate::parser::ParseError::{EndOfStream, Other};

        let key = parse.next_string()?;
        let value = parse.next_bytes()?;
        let mut expire = None;

        match parse.next_string() {
            Ok(s) if s == "EX" => {
                expire = Some(Duration::from_secs(parse.next_u64()?));
            }
            Ok(s) if s == "PX" => {
                expire = Some(Duration::from_millis(parse.next_u64()?));
            }
            Ok(_) => {
                return Err(anyhow!(
                    "currently 'SET' supports only the expiration option"
                ));
            }
            Err(EndOfStream) => {}
            Err(Other(e)) => return Err(e),
        }

        Ok(Set { key, value, expire })
    }
    pub fn apply(self, db: &crate::db::Db) -> anyhow::Result<Frame> {
        db.set(self.key, self.value, self.expire);
        Ok(Frame::Simple("OK".to_string()))
    }
}
