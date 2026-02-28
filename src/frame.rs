use anyhow::anyhow;
use std::collections::HashMap;
use std::io::Cursor;
use thiserror::Error;

use bytes::{Buf, Bytes};

type FrameResult<T> = Result<T, FrameError>;

#[derive(Debug, Clone, Hash, PartialEq, PartialOrd, Eq)]
pub enum Encoding {
    TXT { bytes: Bytes },
}

impl Encoding {
    pub fn new(encoding: &[u8]) -> FrameResult<Self> {
        if encoding.iter().zip("txt".bytes()).all(|(b1, b2)| *b1 == b2) {
            Ok(Self::TXT {
                bytes: Bytes::copy_from_slice(encoding),
            })
        } else {
            Err(FrameError::Other(anyhow!(
                "Unknown encoding {:?}",
                encoding
            )))
        }
    }

    pub fn to_bytes(&self) -> &[u8] {
        match self {
            Self::TXT { bytes: b } => &b,
        }
    }
}

#[derive(Debug)]
pub(crate) enum Frame {
    Simple(String),
    Error(String),
    Integer(u64),
    BulkString(Bytes),
    Array(Option<Vec<Frame>>),
    Null,
    Bool(bool),
    Double(f64),
    BigNumber(Bytes),
    BulkError(Bytes),
    VerbatimString(Encoding, Bytes),
    Map(HashMap<String, Frame>),
    Attribute(HashMap<String, Frame>),
    Set(Vec<Frame>),
    Push(Vec<Frame>),
}

#[derive(Debug, Error)]
pub(crate) enum FrameError {
    #[error("Incomplete frame data")]
    Incomplete,
    #[error("During framing error encountered: {0}")]
    Other(anyhow::Error),
}

impl Frame {
    pub fn check(buf: &mut Cursor<&[u8]>) -> FrameResult<()> {
        match get_u8(buf)? {
            b':' => {
                get_decimal(buf)?;
                Ok(())
            }
            b'#' => {
                get_bool(buf)?;
                Ok(())
            }
            b',' => {
                get_double(buf)?;
                Ok(())
            }
            b'$' | b'!' => {
                let len: usize = get_decimal(buf)?
                    .try_into()
                    .map_err(|_| FrameError::Other(anyhow!("Conversion to usize failed")))?;

                skip(buf, len + 2)
            }
            b'%' | b'|' => {
                let len = get_decimal(buf)?;

                for _ in 0..len {
                    Self::check(buf)?;
                    Self::check(buf)?;
                }

                Ok(())
            }
            b'*' | b'>' | b'~' => {
                if peek_u8(buf)? == b'-' {
                    skip(buf, 4)
                } else {
                    let len = get_decimal(buf)?;

                    for _ in 0..len {
                        Self::check(buf)?;
                    }

                    Ok(())
                }
            }
            b'_' | b'+' | b'-' | b'(' => {
                get_line(buf)?;
                Ok(())
            }
            b'=' => {
                let len: usize = get_decimal(buf)?
                    .try_into()
                    .map_err(|_| FrameError::Other(anyhow!("Conversion to usize failed")))?;
                get_encoding(buf)?;

                skip(buf, len + 2)
            }
            _ => Err(FrameError::Other(anyhow!("wrong format"))),
        }
    }

    pub fn parse(buf: &mut Cursor<&[u8]>) -> FrameResult<Self> {
        match get_u8(buf)? {
            b'+' => {
                let bytes = get_line(buf)?;
                let string =
                    String::from_utf8(bytes.to_vec()).map_err(|e| FrameError::Other(e.into()))?;

                Ok(Self::Simple(string))
            }
            b'-' => {
                let bytes = get_line(buf)?;
                let string =
                    String::from_utf8(bytes.to_vec()).map_err(|e| FrameError::Other(e.into()))?;

                Ok(Self::Simple(string))
            }
            b':' => {
                let dec = get_decimal(buf)?;
                Ok(Self::Integer(dec))
            }
            b'$' => Ok(Self::BulkString(get_bytes(buf)?)),
            b'*' => {
                if peek_u8(buf)? == b'-' {
                    skip(buf, 4)?;
                    Ok(Self::Array(None))
                } else {
                    let array = get_array(buf)?;
                    Ok(Self::Array(Some(array)))
                }
            }
            b'_' => {
                skip(buf, 2)?;
                Ok(Self::Null)
            }
            b'#' => {
                let b = get_bool(buf)?;
                Ok(Self::Bool(b))
            }
            b',' => {
                let d = get_double(buf)?;
                Ok(Self::Double(d))
            }
            b'(' => {
                let bytes = get_bytes(buf)?;
                Ok(Self::BigNumber(bytes))
            }
            b'!' => Ok(Self::BulkError(get_bytes(buf)?)),
            b'=' => {
                let encoding = get_encoding(buf)?;
                let bytes = get_bytes(buf)?;

                Ok(Self::VerbatimString(encoding, bytes))
            }
            b'%' => {
                let map = get_map(buf)?;
                Ok(Self::Map(map))
            }
            b'|' => {
                let map = get_map(buf)?;
                Ok(Self::Attribute(map))
            }
            b'~' => {
                let array = get_array(buf)?;
                Ok(Self::Set(array))
            }
            b'>' => {
                let array = get_array(buf)?;
                Ok(Self::Push(array))
            }
            _ => Err(FrameError::Other(anyhow!("wrong format"))),
        }
    }

    pub(crate) fn frame_symbol(&self) -> u8 {
        match self {
            Self::Simple(_) => b'+',
            Self::Error(_) => b'-',
            Self::Integer(_) => b':',
            Self::BulkString(_) => b'$',
            Self::Array(_) => b'*',
            Self::Null => b'_',
            Self::Bool(_) => b'#',
            Self::Double(_) => b',',
            Self::BigNumber(_) => b'(',
            Self::BulkError(_) => b'!',
            Self::VerbatimString(_, _) => b'=',
            Self::Map(_) => b'%',
            Self::Attribute(_) => b'|',
            Self::Set(_) => b'~',
            Self::Push(_) => b'>',
        }
    }
}

fn get_map(buf: &mut Cursor<&[u8]>) -> FrameResult<HashMap<String, Frame>> {
    let len = get_decimal(buf)?;

    let mut map = HashMap::new();
    for _ in 0..len {
        map.insert(
            String::from_utf8(get_line(buf)?.iter().map(|c| *c).collect())
                .map_err(|e| FrameError::Other(e.into()))?,
            Frame::parse(buf)?,
        );
    }

    Ok(map)
}

fn get_array(buf: &mut Cursor<&[u8]>) -> FrameResult<Vec<Frame>> {
    let len = get_decimal(buf)?;

    let mut array: Vec<Frame> = vec![];
    for _ in 0..len {
        array.push(Frame::parse(buf)?);
    }

    Ok(array)
}

fn get_bytes(buf: &mut Cursor<&[u8]>) -> FrameResult<Bytes> {
    let len: usize = get_decimal(buf)?
        .try_into()
        .map_err(|_| FrameError::Other(anyhow!("Converions to usize failed")))?;
    let n = len + 2;

    if buf.remaining() < n {
        return Err(FrameError::Incomplete);
    }

    let data = Bytes::copy_from_slice(&buf.chunk()[..len]);

    skip(buf, n)?;

    Ok(data)
}

fn skip(buf: &mut Cursor<&[u8]>, n: usize) -> FrameResult<()> {
    if buf.remaining() < n {
        return Err(FrameError::Incomplete);
    }

    buf.advance(n);
    Ok(())
}

fn peek_u8(buf: &Cursor<&[u8]>) -> FrameResult<u8> {
    if !buf.has_remaining() {
        return Err(FrameError::Incomplete);
    }

    Ok(buf.chunk()[0])
}

fn get_decimal(buf: &mut Cursor<&[u8]>) -> FrameResult<u64> {
    let line = get_line(buf)?;

    use atoi::atoi;

    atoi::<u64>(line).ok_or(FrameError::Other(anyhow!("invalid frame format")))
}

fn get_double(buf: &mut Cursor<&[u8]>) -> FrameResult<f64> {
    todo!()
}

fn get_encoding(buf: &mut Cursor<&[u8]>) -> FrameResult<Encoding> {
    let start = buf.position() as usize;
    let end = buf.get_ref().len() - 1;

    for i in start..end {
        if buf.get_ref()[i] == b':' {
            buf.set_position((i + 1) as u64);
            return Encoding::new(&buf.get_ref()[start..i]);
        }
    }

    Err(FrameError::Incomplete)
}

fn get_u8(buf: &mut Cursor<&[u8]>) -> FrameResult<u8> {
    if !buf.has_remaining() {
        Err(FrameError::Incomplete)
    } else {
        Ok(buf.get_u8())
    }
}

fn get_bool(buf: &mut Cursor<&[u8]>) -> FrameResult<bool> {
    let line = get_line(buf)?;

    if line.len() != 1 {
        return Err(FrameError::Other(anyhow!(
            "invalid frame format; expected only one byte"
        )));
    }

    match line[0] {
        b't' => Ok(true),
        b'f' => Ok(false),
        _ => Err(FrameError::Other(anyhow!(
            "invalid frame format; expected 't' or 'f'"
        ))),
    }
}

fn get_line<'a>(buf: &mut Cursor<&'a [u8]>) -> FrameResult<&'a [u8]> {
    let start = buf.position() as usize;
    let end = buf.get_ref().len() - 1;

    for i in start..end {
        if buf.get_ref()[i] == b'\r' && buf.get_ref()[i + 1] == b'\n' {
            buf.set_position((i + 2) as u64);
            return Ok(&buf.get_ref()[start..i]);
        }
    }

    Err(FrameError::Incomplete)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn test_parse_frame() {
        let bytes = b"+PING\r\n";
        let mut buf = Cursor::new(&bytes[..]);

        Frame::check(&mut buf).unwrap();

        let mut buf = Cursor::new(&bytes[..]);

        if let Frame::Simple(s) = Frame::parse(&mut buf).unwrap() {
            assert!(s == "PING");
        } else {
            assert!(false);
        }
    }
}
