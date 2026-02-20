use crate::frame::Frame;
use anyhow::{Result, anyhow};
use bytes::Bytes;

use std::{fmt, str, vec};

#[derive(Debug)]
pub(crate) struct Parse {
    parts: vec::IntoIter<Frame>,
}

impl Parse {
    pub(crate) fn new(frame: Frame) -> Result<Self> {
        let array = match frame {
            Frame::Array(Some(array)) => array,
            frame => {
                return Err(anyhow!(
                    "protocol error; expected non-null array, got {:?}",
                    frame
                ));
            }
        };

        Ok(Parse {
            parts: array.into_iter(),
        })
    }

    fn next(&mut self) -> Result<Frame> {
        self.parts.next().ok_or(anyhow!("End of stream"))
    }

    pub(crate) fn next_string(&mut self) -> Result<String> {
        match self.next()? {
            Frame::Simple(s) => Ok(s),
            Frame::BulkString(data) => str::from_utf8(&data[..])
                .map(|s| s.to_string())
                .map_err(|_| anyhow!("protocol error; invalid string")),
            frame => Err(anyhow!(
                "protocol error; expected simple string or bulk string, got {:?}",
                frame
            )),
        }
    }

    pub(crate) fn finish(&mut self) -> Result<()> {
        if self.parts.next().is_none() {
            Ok(())
        } else {
            Err(anyhow!("protocol error; expected end of command"))
        }
    }

    fn next_bytes(&mut self) -> Result<Bytes> {
        match self.next()? {
            Frame::Simple(s) => Ok(Bytes::from(s.into_bytes())),
            Frame::BulkString(data) => Ok(data),
            frame => Err(anyhow!(
                "protocol error; expected simple string or bulk string, got {:?}",
                frame
            )),
        }
    }

    fn next_integer(&mut self) -> Result<i64> {
        use atoi::atoi;

        match self.next()? {
            Frame::Integer(v) => Ok(v),
            Frame::Simple(s) => {
                atoi::<i64>(s.as_bytes()).ok_or_else(|| anyhow!("protocol error; invalid number"))
            }
            Frame::BulkString(data) => {
                atoi::<i64>(&data).ok_or_else(|| anyhow!("protocol error; invalid number"))
            }
            frame => Err(anyhow!(
                "protocol error; expected simple string or bulk string, got {:?}",
                frame
            )),
        }
    }
}
