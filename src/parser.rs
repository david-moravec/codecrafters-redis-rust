use crate::frame::Frame;
use anyhow::anyhow;
use bytes::Bytes;
use thiserror::Error;

use std::{fmt::Display, str, vec};

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

#[derive(Debug, Clone, Copy)]
pub(crate) struct StreamEntryIDOpt {
    pub(crate) miliseconds: Option<u128>,
    pub(crate) sequence: Option<u64>,
}

impl StreamEntryIDOpt {
    pub(crate) fn new(miliseconds: Option<u128>, sequence: Option<u64>) -> Self {
        Self {
            miliseconds,
            sequence,
        }
    }
}

impl TryFrom<Bytes> for StreamEntryIDOpt {
    type Error = ParseError;

    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        use atoi::atoi;

        if value.len() == 1 && value[0] == b'*' {
            return Ok(Self::new(None, None));
        }

        let dash_index = value.iter().position(|b| *b == b'-').unwrap_or(value.len());

        let miliseconds = {
            if value[0] == b'*' {
                None
            } else {
                Some(
                    atoi::<u128>(&value[..dash_index]).ok_or(ParseError::Other(anyhow!(
                        "protocol error; expected u64 bytes as miliseconds"
                    )))?,
                )
            }
        };
        let sequence = {
            if value.len() == dash_index {
                None
            } else if value[dash_index + 1] == b'*' {
                None
            } else {
                Some(
                    atoi::<u64>(&value[dash_index + 1..]).ok_or(ParseError::Other(anyhow!(
                        "protocol error; expected u64 bytes as sequence"
                    )))?,
                )
            }
        };

        Ok(Self::new(miliseconds, sequence))
    }
}

impl Display for StreamEntryIDOpt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.miliseconds.is_some() {
            write!(f, "{:}", self.miliseconds.unwrap())?;
        } else {
            write!(f, "*")?;
        }
        write!(f, "-")?;

        if self.sequence.is_some() {
            write!(f, "{:}", self.sequence.unwrap())
        } else {
            write!(f, "*")
        }
    }
}

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

    pub(crate) fn end_of_stream_reached(&mut self) -> bool {
        return self.parts.next().is_none();
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

    pub(crate) fn next_u64(&mut self) -> ParseResult<u64> {
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

    pub(crate) fn next_f64(&mut self) -> ParseResult<f64> {
        match self.next()? {
            // Frame::Integer(v) => Ok(v),
            Frame::Simple(s) => s
                .parse::<f64>()
                .map_err(|_| anyhow!("protocol error; invalid number").into()),
            Frame::BulkString(data) => String::from_utf8(data.to_vec())
                .map_err(|e| ParseError::Other(e.into()))?
                .parse::<f64>()
                .map_err(|_| anyhow!("protocol error; invalid number").into()),
            frame => Err(anyhow!(
                "protocol error; expected simple string or bulk string, got {:?}",
                frame
            )
            .into()),
        }
    }

    pub(crate) fn next_stream_id(&mut self) -> ParseResult<StreamEntryIDOpt> {
        StreamEntryIDOpt::try_from(self.next_bytes()?)
    }

    pub(crate) fn next_i64(&mut self) -> ParseResult<i64> {
        use atoi::atoi;

        match self.next()? {
            Frame::Integer(v) => v.try_into().map_err(|_| anyhow!("Could not parse").into()),
            Frame::Simple(s) => atoi::<i64>(s.as_bytes())
                .ok_or_else(|| anyhow!("protocol error; invalid number").into()),
            Frame::BulkString(data) => {
                atoi::<i64>(&data).ok_or_else(|| anyhow!("protocol error; invalid number").into())
            }
            frame => Err(anyhow!(
                "protocol error; expected simple string or bulk string, got {:?}",
                frame
            )
            .into()),
        }
    }
}
