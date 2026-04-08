use anyhow::anyhow;
use bytes::{Buf, Bytes};
use std::{
    collections::HashMap,
    io::Cursor,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;

use crate::{
    db::DbEntry,
    frame::{FrameError, get_u8, peek_u8, skip},
};

#[derive(Debug)]
pub(crate) enum StringEncoding {
    Bit8,
    Bit16,
    Bit32,
    LZF,
    Unknown,
}

#[derive(Debug, Error)]
pub(crate) enum RdbFileError {
    #[error("special format encoding")]
    StringEncoding(StringEncoding),
    #[error("unexpected section: {0:#04x}")]
    UnexpectedSection(u8),
    #[error("not a length encoding")]
    NotLengthEncoding,
    #[error(transparent)]
    Framing(#[from] FrameError),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

type RdbFileResult<T> = Result<T, RdbFileError>;

#[derive(Debug)]
pub(crate) struct RdbFile {
    pub(crate) db: HashMap<String, DbEntry>,
    pub(crate) expiry: HashMap<String, Duration>,
}

impl RdbFile {
    pub(crate) fn new() -> Self {
        Self {
            db: HashMap::new(),
            expiry: HashMap::new(),
        }
    }

    pub(crate) fn parse(&mut self, file_path: &std::path::Path) -> RdbFileResult<()> {
        match std::fs::read(file_path) {
            Ok(bytes) => {
                let bytes = Bytes::from(bytes);
                let mut buf = Cursor::new(&bytes[..]);

                self.parse_magic_number(&mut buf)?;
                // skip redis version for now
                skip(&mut buf, 4)?;

                self.parse_metadata_section(&mut buf)?;
                self.parse_database_section(&mut buf)?;
            }
            Err(err) => {
                eprintln!(
                    "reading RDB file at {:?} failed: {:}, proceeding with empty DB",
                    file_path, err
                );
            }
        }

        Ok(())
    }

    fn parse_magic_number(&self, buf: &mut Cursor<&[u8]>) -> RdbFileResult<()> {
        if &buf.chunk()[..5] == b"REDIS" {
            Ok(skip(buf, 5)?)
        } else {
            Err(anyhow!("expected magic number at the start of RDB file").into())
        }
    }

    fn parse_metadata_section(&self, buf: &mut Cursor<&[u8]>) -> RdbFileResult<()> {
        let section_op_code = get_u8(buf)?;

        if section_op_code != 0xfa {
            return Err(RdbFileError::UnexpectedSection(section_op_code));
        }

        // eagerly parse all strings
        loop {
            get_string(buf)?;
            get_string(buf)?;
            if peek_u8(buf)? != 0xfa {
                break;
            } else {
                get_u8(buf)?;
            }
        }

        Ok(())
    }

    fn parse_expiry(&self, buf: &mut Cursor<&[u8]>) -> RdbFileResult<Option<Duration>> {
        match peek_u8(buf)? {
            0xfd => {
                get_u8(buf)?;
                let secs = u32::from_le_bytes(get_next_4_bytes(buf)?);
                Ok(Some(Duration::from_secs(secs as u64)))
            }
            0xfc => {
                get_u8(buf)?;
                let millis = u64::from_le_bytes(get_next_8_bytes(buf)?);
                Ok(Some(Duration::from_millis(millis as u64)))
            }
            _ => Ok(None),
        }
    }

    fn parse_db_entry(&self, buf: &mut Cursor<&[u8]>) -> RdbFileResult<(String, Bytes)> {
        match peek_u8(buf)? {
            0x00 => {
                get_u8(buf)?;
                Ok((get_string(buf)?, Bytes::from(get_string(buf)?)))
            }
            _ => todo!("not yet supported type {:}", peek_u8(buf)?),
        }
    }

    fn parse_database_section(&mut self, buf: &mut Cursor<&[u8]>) -> RdbFileResult<()> {
        let section_op_code = get_u8(buf)?;

        if section_op_code != 0xfe {
            return Err(RdbFileError::UnexpectedSection(section_op_code));
        }

        let _db_index = get_u8(buf)?;

        if get_u8(buf)? != 0xfb {
            return Err(RdbFileError::Other(anyhow!("expected a 0xFB section")));
        }

        let _hash_table_size = get_u8(buf)?;
        let _expiry_table_size = get_u8(buf)?;

        loop {
            let expiry = self.parse_expiry(buf)?;

            // discard expired keys
            if expiry.is_some_and(|e| e < SystemTime::now().duration_since(UNIX_EPOCH).unwrap()) {
                self.parse_db_entry(buf)?;
                continue;
            }

            let (key, value) = self.parse_db_entry(buf)?;
            self.db.insert(key.clone(), DbEntry::Single(value));

            if let Some(expiry) = expiry {
                self.expiry.insert(key, expiry);
            }

            if peek_u8(buf)? == 0xff {
                break;
            }
        }

        Ok(())
    }
}

fn get_string(buf: &mut Cursor<&[u8]>) -> RdbFileResult<String> {
    match decode_length(buf) {
        Ok(length) => {
            let result = String::from_utf8(buf.chunk()[..length].iter().map(|b| *b).collect())
                .map_err(|_| anyhow!("protocol error; cannot convert to string").into());
            skip(buf, length)?;
            result
        }
        Err(RdbFileError::StringEncoding(encoding)) => match encoding {
            StringEncoding::Bit8 => Ok(format!("{:}", get_u8(buf)?)),
            StringEncoding::Bit16 => Ok(format!(
                "{:}",
                u16::from_le_bytes([get_u8(buf)?, get_u8(buf)?])
            )),
            StringEncoding::Bit32 => Ok(format!(
                "{:}",
                u32::from_le_bytes([get_u8(buf)?, get_u8(buf)?, get_u8(buf)?, get_u8(buf)?])
            )),
            StringEncoding::LZF => todo!(),
            StringEncoding::Unknown => Err(anyhow!("Unknown string encoding").into()),
        },
        Err(e) => Err(e),
    }
}

fn decode_length(buf: &mut Cursor<&[u8]>) -> RdbFileResult<usize> {
    if peek_u8(buf)? >> 6 > 3 {
        return Err(RdbFileError::NotLengthEncoding);
    }

    let byte = get_u8(buf)?;
    let next_6_bits = byte & 0b00111111;

    match byte >> 6 {
        0 => Ok(usize::from(next_6_bits)),
        1 => Ok(usize::from_be_bytes([
            0,
            0,
            0,
            0,
            0,
            0,
            next_6_bits,
            get_u8(buf)?,
        ])),
        2 => Ok(usize::from_be_bytes([
            0,
            0,
            0,
            0,
            get_u8(buf)?,
            get_u8(buf)?,
            get_u8(buf)?,
            get_u8(buf)?,
        ])),
        3 => Err(RdbFileError::StringEncoding(match next_6_bits {
            0 => StringEncoding::Bit8,
            1 => StringEncoding::Bit16,
            2 => StringEncoding::Bit32,
            3 => StringEncoding::LZF,
            _ => StringEncoding::Unknown,
        })),
        _ => unreachable!(),
    }
}

fn get_next_4_bytes(buf: &mut Cursor<&[u8]>) -> RdbFileResult<[u8; 4]> {
    Ok([get_u8(buf)?, get_u8(buf)?, get_u8(buf)?, get_u8(buf)?])
}

fn get_next_8_bytes(buf: &mut Cursor<&[u8]>) -> RdbFileResult<[u8; 8]> {
    Ok([
        get_u8(buf)?,
        get_u8(buf)?,
        get_u8(buf)?,
        get_u8(buf)?,
        get_u8(buf)?,
        get_u8(buf)?,
        get_u8(buf)?,
        get_u8(buf)?,
    ])
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[test]
    fn test_equality() {
        let bytes = Bytes::from_static(b"REDISAHOJLELE");
        let buf = Cursor::new(&bytes[..]);
        assert!(&buf.chunk()[..5] == b"REDIS");
    }

    #[test]
    fn test_decode_length() {
        let bytes = Bytes::from(vec![0b00001100, 0b11001101]);
        let mut buf = Cursor::new(&bytes[..]);
        let length = decode_length(&mut buf).unwrap();

        assert!(length == 12);

        let bytes = Bytes::from(vec![0b01001100, 0b11001101]);
        let mut buf = Cursor::new(&bytes[..]);
        let length = decode_length(&mut buf).unwrap();

        assert!(length == 3277);

        let bytes = Bytes::from(vec![
            0b10001100, 0b11001101, 0b11010011, 0b11110000, 0b10101010,
        ]);
        let mut buf = Cursor::new(&bytes[..]);
        let length = decode_length(&mut buf).unwrap();

        assert!(length == 3453218986);
    }

    #[test]
    fn test_get_string() {
        fn compare_bytes_with_string(bytes: impl Iterator<Item = u8>, expected_str: &str) {
            let bytes = Bytes::from_iter(bytes);
            let mut buf = Cursor::new(&bytes[..]);
            let s = get_string(&mut buf).unwrap();

            assert!(s == expected_str)
        }
        compare_bytes_with_string([0xc0, 0x7b].into_iter(), "123");
        compare_bytes_with_string(
            [
                0x0D, 0x48, 0x65, 0x6c, 0x6c, 0x6f, 0x2c, 0x20, 0x57, 0x6f, 0x72, 0x6c, 0x64, 0x21,
            ]
            .into_iter(),
            "Hello, World!",
        );
        compare_bytes_with_string([0xc1, 0x39, 0x30].into_iter(), "12345");
        compare_bytes_with_string([0xc2, 0x87, 0xd6, 0x12, 0x00].into_iter(), "1234567");
    }

    #[test]
    fn test_parse_rdb_file() {
        let mut rdb_file = RdbFile::new();
        rdb_file.parse(Path::new("dump.rdb")).unwrap();

        eprintln!("{:?}", rdb_file);
        assert!(rdb_file.db.get("foo").unwrap().get().unwrap() == Bytes::from("bar".to_string()));
    }
}
