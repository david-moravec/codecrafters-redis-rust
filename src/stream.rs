use bytes::Bytes;
use std::collections::BTreeMap;
use std::fmt::Display;
use std::ops::Bound::Included;
use std::time::{SystemTime, UNIX_EPOCH};
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
    miliseconds: u128,
    sequence: u64,
}

impl StreamEntryID {
    pub(crate) fn new(miliseconds: u128, sequence: u64) -> Self {
        Self {
            miliseconds,
            sequence,
        }
    }
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

pub type StreamEntry = Vec<(Bytes, Bytes)>;

pub struct Stream {
    entries: BTreeMap<StreamEntryID, StreamEntry>,
}

impl Stream {
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
        }
    }

    fn generate_id(&self, mut id_opt: StreamEntryIDOpt) -> Result<StreamEntryID, StreamError> {
        if id_opt.miliseconds.is_none() {
            id_opt.miliseconds = Some(
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
            )
        }
        if id_opt.sequence.is_none() {
            id_opt.sequence = match self.entries.last_key_value() {
                Some((key, _)) if key.miliseconds == id_opt.miliseconds.unwrap() => {
                    Some(key.sequence + 1)
                }
                _ => {
                    if id_opt.miliseconds.unwrap() == 0 {
                        Some(1)
                    } else {
                        Some(0)
                    }
                }
            }
        }

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

    pub fn xrange(
        &self,
        start: Option<StreamEntryIDOpt>,
        end: Option<StreamEntryIDOpt>,
    ) -> Result<Vec<(StreamEntryID, StreamEntry)>, StreamError> {
        let start_id = match start {
            Some(mut id) => {
                if id.sequence.is_none() {
                    id.sequence = Some(0)
                }

                StreamEntryID::try_from(id)?
            }
            None => StreamEntryID::new(0, 0),
        };
        let end_id = match end {
            Some(id) => self.generate_id(id)?,
            None => self.entries.last_key_value().unwrap().0.clone(),
        };

        Ok(self
            .entries
            .range((Included(&start_id), Included(&end_id)))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect())
    }
}
