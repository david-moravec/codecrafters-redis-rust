use bytes::Bytes;
use std::collections::BTreeMap;
use std::fmt::Display;
use thiserror::Error;

use crate::parser::StreamEntryIDOpt;

#[derive(Debug, Error)]
pub enum StreamError {
    #[error("ERR The ID specified in XADD is equal or smaller than the target stream top item")]
    NotGreaterThanLastId,
    #[error("ERR The ID specified in XADD must be greater than 0-0")]
    ZeroZeroID,
    #[error("ERR The ID has sequence or miliseconds missing")]
    MissingValue,
    #[error("other error occured")]
    Other(#[from] anyhow::Error),
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Hash, Clone, Ord, Copy)]
pub struct StreamEntryID {
    miliseconds: u64,
    sequence: u64,
}

impl TryFrom<StreamEntryIDOpt> for StreamEntryID {
    type Error = StreamError;

    fn try_from(value: StreamEntryIDOpt) -> Result<Self, Self::Error> {
        let miliseconds = value.miliseconds.ok_or(StreamError::MissingValue)?;
        let sequence = value.sequence.ok_or(StreamError::MissingValue)?;

        if sequence == 0 && miliseconds == 0 {
            Err(StreamError::ZeroZeroID)
        } else {
            Ok(Self {
                miliseconds,
                sequence,
            })
        }
    }
}

impl Display for StreamEntryID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:}-{:}", self.miliseconds, self.sequence)
    }
}

pub type StreamEntry = Vec<(String, Bytes)>;

pub struct Stream {
    entries: BTreeMap<StreamEntryID, StreamEntry>,
}

impl Stream {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    fn generate_id(&self, id_opt: StreamEntryIDOpt) -> Result<StreamEntryID, StreamError> {
        StreamEntryID::try_from(id_opt)
    }

    pub fn insert(
        &mut self,
        id_opt: StreamEntryIDOpt,
        values: StreamEntry,
    ) -> Result<StreamEntryID, StreamError> {
        let id = self.generate_id(id_opt)?;

        if let Some(e) = self.entries.last_entry() {
            let key = *e.key();
            if id <= key {
                return Err(StreamError::NotGreaterThanLastId);
            }
        }

        self.entries.insert(id, values);
        Ok(id)
    }
}
