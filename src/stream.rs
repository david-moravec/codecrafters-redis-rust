use bytes::Bytes;
use std::collections::BTreeMap;
use std::fmt::Display;
use std::ops::Bound::{Excluded, Included};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::oneshot;

use crate::frame::{Frame, ToFrame};
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

        Ok(Self {
            miliseconds,
            sequence,
        })
    }
}

impl Display for StreamEntryID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:}-{:}", self.miliseconds, self.sequence)
    }
}

#[derive(Debug, Clone)]
pub struct StreamEntry {
    values: Vec<(Bytes, Bytes)>,
}

impl StreamEntry {
    pub(crate) fn new(values: Vec<(Bytes, Bytes)>) -> Self {
        Self { values }
    }
}

impl ToFrame for StreamEntry {
    fn to_frame(self) -> crate::frame::Frame {
        Frame::Array(Some(
            self.values
                .into_iter()
                .flat_map(|(id, entry)| {
                    [Frame::BulkString(id), Frame::BulkString(entry)].into_iter()
                })
                .collect(),
        ))
    }
}

#[derive(Debug, Clone)]
pub struct XRange {
    entries: Vec<(StreamEntryID, StreamEntry)>,
}

impl XRange {
    pub(crate) fn entries_len(&self) -> usize {
        self.entries.len()
    }
}

impl ToFrame for XRange {
    fn to_frame(self) -> Frame {
        Frame::Array(Some(
            self.entries
                .into_iter()
                .map(|(id, vec_entries)| {
                    Frame::Array(Some({
                        vec![
                            Frame::BulkString(Bytes::from(format!("{:}", id))),
                            vec_entries.to_frame(),
                        ]
                    }))
                })
                .collect(),
        ))
    }
}

#[derive(Debug, Clone)]
pub struct XRead {
    entries: Vec<(String, XRange)>,
}

impl XRead {
    pub fn new(entries: Vec<(String, XRange)>) -> Self {
        Self { entries }
    }
}

impl ToFrame for XRead {
    fn to_frame(self) -> Frame {
        Frame::Array(Some(
            self.entries
                .into_iter()
                .map(|(key, xrange)| {
                    Frame::Array(Some(vec![
                        Frame::BulkString(Bytes::from(key)),
                        xrange.to_frame(),
                    ]))
                })
                .collect(),
        ))
    }
}

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

        if id.miliseconds == 0 && id.sequence == 0 {
            return Err(StreamError::ZeroZeroID);
        }

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
        start_included: bool,
    ) -> Result<XRange, StreamError> {
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

        let start_bound = {
            if start_included {
                Included(&start_id)
            } else {
                Excluded(&start_id)
            }
        };

        Ok(XRange {
            entries: self
                .entries
                .range((start_bound, Included(&end_id)))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        })
    }
}
