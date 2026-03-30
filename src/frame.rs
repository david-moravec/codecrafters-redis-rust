use anyhow::anyhow;
use std::collections::HashMap;
use std::io::Cursor;
use thiserror::Error;

use bytes::{Buf, BufMut, Bytes, BytesMut};

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

#[derive(Debug, Clone)]
pub(crate) enum Frame {
    Simple(String),
    Error(String),
    Integer(u64),
    BulkString(Bytes),
    Array(Option<Vec<Frame>>),
    NullBulkString,
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
    RdbFile(Bytes),
}

#[derive(Debug, Error)]
pub(crate) enum FrameError {
    #[error("Incomplete frame data")]
    Incomplete,
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub fn parse_rdb_file(buf: &mut Cursor<&[u8]>) -> FrameResult<()> {
    if get_u8(buf)? != b'$' {
        return Err(FrameError::Other(anyhow!(
            "protocol error; expected '$' at the start of RDB file"
        )));
    }

    let len: usize = get_decimal(buf)?
        .try_into()
        .map_err(|_| anyhow!("Conversion to usize failed"))?;
    skip(buf, len)?;
    Ok(())
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
                    .map_err(|_| anyhow!("Conversion to usize failed"))?;

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
                    .map_err(|_| anyhow!("Conversion to usize failed"))?;
                get_encoding(buf)?;

                skip(buf, len + 2)
            }
            _ => Err(anyhow!("wrong format").into()),
        }
    }

    pub fn check_rdb(buf: &mut Cursor<&[u8]>) -> FrameResult<()> {
        if get_u8(buf)? != b'$' {
            return Err(FrameError::Other(anyhow!(
                "protocol error; expected '$' at the start of RDB file"
            )));
        }

        let len: usize = get_decimal(buf)?
            .try_into()
            .map_err(|_| anyhow!("Conversion to usize failed"))?;
        if buf.remaining() < len {
            return Err(FrameError::Incomplete);
        }
        skip(buf, len)?;
        Ok(())
    }

    pub fn parse_rdb(buf: &mut Cursor<&[u8]>) -> FrameResult<Self> {
        get_u8(buf)?;
        let len: usize = get_decimal(buf)?
            .try_into()
            .map_err(|_| anyhow!("Converions to usize failed"))?;
        let n = len;

        if buf.remaining() < n {
            return Err(FrameError::Incomplete);
        }

        let data = Bytes::copy_from_slice(&buf.chunk()[..len]);

        Ok(Frame::RdbFile(data))
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
            b'$' => {
                if b'-' == peek_u8(buf)? {
                    let line = get_line(buf)?;

                    if line != b"-1" {
                        return Err(anyhow!("protocol error; invalid frame format").into());
                    }

                    Ok(Self::NullBulkString)
                } else {
                    let bytes = get_bytes(buf)?;
                    Ok(Self::BulkString(bytes))
                }
            }
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
            _ => Err(anyhow!("wrong format").into()),
        }
    }

    pub fn bulk_strings_array(args: Vec<Bytes>) -> Frame {
        Frame::Array(Some(
            args.into_iter().map(|b| Frame::BulkString(b)).collect(),
        ))
    }
    pub fn bulk_strings_array_from_str(args: Vec<&str>) -> Frame {
        Frame::Array(Some(
            args.into_iter()
                .map(|b| Frame::BulkString(Bytes::from(b.to_string())))
                .collect(),
        ))
    }

    pub(crate) fn frame_symbol(&self) -> u8 {
        match self {
            Self::Simple(_) => b'+',
            Self::Error(_) => b'-',
            Self::Integer(_) => b':',
            Self::BulkString(_) | Self::RdbFile(_) => b'$',
            Self::Array(_) => b'*',
            Self::Null => b'_',
            Self::NullBulkString => b'$',
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

    fn value_to_bytes(&self) -> Bytes {
        let mut bytes_mut = BytesMut::new();
        bytes_mut.put_u8(self.frame_symbol());

        match self {
            Self::Simple(val) => {
                bytes_mut.put_slice(val.as_bytes());
            }
            Self::Error(val) => {
                bytes_mut.put_slice(val.as_bytes());
            }
            Self::Integer(val) => bytes_mut.put_slice(format!("{:}", val).as_bytes()),
            Self::BulkString(bytes) => {
                bytes_mut.put_slice(format!("{:}", bytes.len()).as_bytes());
                bytes_mut.put_slice(b"\r\n");
                bytes_mut.put_slice(&bytes);
            }
            Self::NullBulkString => bytes_mut.put_slice(b"-1"),
            Self::Null => bytes_mut.put_u8(b'_'),
            Self::Bool(b) => {
                if *b {
                    bytes_mut.put_u8(b't')
                } else {
                    bytes_mut.put_u8(b'f')
                }
            }
            Self::Double(d) => bytes_mut.put_f64(*d),
            Self::BigNumber(bytes) => bytes_mut.put_slice(&bytes),
            Self::BulkError(bytes) => bytes_mut.put_slice(&bytes),
            Self::VerbatimString(encoding, bytes) => {
                bytes_mut.put_slice(encoding.to_bytes());
                bytes_mut.put_u8(b':');
                bytes_mut.put_slice(&bytes);
            }
            _ => unreachable!(),
        }

        bytes_mut.put_slice(b"\r\n");

        bytes_mut.freeze()
    }

    pub(crate) fn to_bytes(&self) -> Bytes {
        let mut bytes_mut = BytesMut::new();

        match self {
            Self::Array(array_opt) => match array_opt {
                Some(array) => {
                    bytes_mut.put_u8(self.frame_symbol());
                    bytes_mut.put_slice(&array_to_bytes(array));
                }
                None => {
                    bytes_mut.put_u8(self.frame_symbol());
                    bytes_mut.put_slice(b"-1\r\n");
                }
            },
            Self::Map(map) | Self::Attribute(map) => {
                bytes_mut.put_u8(self.frame_symbol());
                bytes_mut.put_slice(&map_to_bytes(map));
            }
            Self::Push(array) | Self::Set(array) => {
                bytes_mut.put_u8(self.frame_symbol());
                bytes_mut.put_slice(&array_to_bytes(array));
            }
            _ => bytes_mut.put_slice(&self.value_to_bytes()),
        };

        bytes_mut.freeze()
    }
}

fn array_to_bytes(array: &[Frame]) -> Bytes {
    let mut bytes_mut = BytesMut::new();
    let len = array.len();
    bytes_mut.put_slice(format!("{:}", len).as_bytes());
    bytes_mut.put_slice(b"\r\n");

    for frame in array.into_iter() {
        bytes_mut.put_slice(&frame.to_bytes());
    }

    bytes_mut.freeze()
}

fn map_to_bytes(map: &HashMap<String, Frame>) -> Bytes {
    let mut bytes_mut = BytesMut::new();
    let len = map.len();
    bytes_mut.put_u64(len as u64);
    bytes_mut.put_slice(b"\r\n");

    for (key, frame) in map.iter() {
        bytes_mut.put_u64(key.len() as u64);
        bytes_mut.put_slice(b"\r\n");
        bytes_mut.put_slice(key.as_bytes());
        bytes_mut.put_slice(b"\r\n");
        bytes_mut.put_slice(&frame.to_bytes());
    }

    bytes_mut.freeze()
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
        .map_err(|_| anyhow!("Converions to usize failed"))?;
    let n = len + 2;

    if buf.remaining() < n {
        return Err(FrameError::Incomplete);
    }

    let data = Bytes::copy_from_slice(&buf.chunk()[..len]);

    skip(buf, n)?;

    Ok(data)
}

pub(crate) fn skip(buf: &mut Cursor<&[u8]>, n: usize) -> FrameResult<()> {
    if buf.remaining() < n {
        return Err(FrameError::Incomplete);
    }

    buf.advance(n);
    Ok(())
}

pub(crate) fn peek_u8(buf: &Cursor<&[u8]>) -> FrameResult<u8> {
    if !buf.has_remaining() {
        return Err(FrameError::Incomplete);
    }

    Ok(buf.chunk()[0])
}

fn get_decimal(buf: &mut Cursor<&[u8]>) -> FrameResult<u64> {
    let line = get_line(buf)?;

    use atoi::atoi;

    atoi::<u64>(line).ok_or(anyhow!("invalid frame format").into())
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

pub(crate) fn get_u8(buf: &mut Cursor<&[u8]>) -> FrameResult<u8> {
    if !buf.has_remaining() {
        Err(FrameError::Incomplete)
    } else {
        Ok(buf.get_u8())
    }
}

fn get_bool(buf: &mut Cursor<&[u8]>) -> FrameResult<bool> {
    let line = get_line(buf)?;

    if line.len() != 1 {
        return Err(anyhow!("invalid frame format; expected only one byte").into());
    }

    match line[0] {
        b't' => Ok(true),
        b'f' => Ok(false),
        _ => Err(anyhow!("invalid frame format; expected 't' or 'f'").into()),
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

pub trait IntoFrame {
    fn into_frame(self) -> Frame;
}
pub trait ToFrame {
    fn to_frame(&self) -> Frame;
}
