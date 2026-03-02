use crate::frame::Frame;
use anyhow::anyhow;
use bytes::Bytes;
use thiserror::Error;

use std::{str, vec};

#[derive(Debug)]
pub(crate) struct Parse {
    parts: vec::IntoIter<Frame>,
}

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("end of stream reached")]
    EndOfStream,
    #[error("during parsing following error occured")]
    Other(#[from] anyhow::Error),
}

type ParseResult<T> = Result<T, ParseError>;

impl Parse {
    pub(crate) fn new(frame: Frame) -> ParseResult<Self> {
        let array = match frame {
            Frame::Array(Some(array)) => array,
            frame => {
                return Err(ParseError::Other(anyhow!(
                    "protocol error; expected non-null array, got {:?}",
                    frame
                )));
            }
        };

        Ok(Parse {
            parts: array.into_iter(),
        })
    }

    fn next(&mut self) -> ParseResult<Frame> {
        self.parts.next().ok_or(ParseError::EndOfStream)
    }

    pub(crate) fn next_string(&mut self) -> ParseResult<String> {
        match self.next()? {
            Frame::Simple(s) => Ok(s),
            Frame::BulkString(data) => str::from_utf8(&data[..])
                .map(|s| s.to_string())
                .map_err(|_| anyhow!("protocol error; invalid string").into()),
            frame => Err(anyhow!(
                "protocol error; expected simple string or bulk string, got {:?}",
                frame
            )
            .into()),
        }
    }

    pub(crate) fn finish(&mut self) -> ParseResult<()> {
        if self.parts.next().is_none() {
            Ok(())
        } else {
            Err(anyhow!("protocol error; expected end of command").into())
        }
    }

    pub(crate) fn next_bytes(&mut self) -> ParseResult<Bytes> {
        match self.next()? {
            Frame::Simple(s) => Ok(Bytes::from(s.into_bytes())),
            Frame::BulkString(data) => Ok(data),
            frame => Err(anyhow!(
                "protocol error; expected simple string or bulk string, got {:?}",
                frame
            )
            .into()),
        }
    }

    pub(crate) fn next_integer(&mut self) -> ParseResult<u64> {
        use atoi::atoi;

        match self.next()? {
            Frame::Integer(v) => Ok(v),
            Frame::Simple(s) => atoi::<u64>(s.as_bytes())
                .ok_or_else(|| anyhow!("protocol error; invalid number").into()),
            Frame::BulkString(data) => {
                atoi::<u64>(&data).ok_or_else(|| anyhow!("protocol error; invalid number").into())
            }
            frame => Err(anyhow!(
                "protocol error; expected simple string or bulk string, got {:?}",
                frame
            )
            .into()),
        }
    }
}
