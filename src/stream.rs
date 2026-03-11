use anyhow::anyhow;
use bytes::Bytes;
use std::collections::BTreeMap;
use std::fmt::Display;
use std::ops::Bound::{self, Excluded, Included};
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::oneshot;

use crate::frame::{Frame, ToFrame};

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

    fn last_stream_entry_id(&self) -> StreamEntryID {
        self.entries
            .last_key_value()
            .map(|(key, _)| *key)
            .unwrap_or(StreamEntryID::new(0, 0))
    }

    fn get_next_sequence_number(&self, miliseconds: u128) -> u64 {
        let last_stream_entry_id = self.last_stream_entry_id();

        if miliseconds == last_stream_entry_id.miliseconds {
            last_stream_entry_id.sequence + 1
        } else {
            0
        }
    }

    pub fn generate_id(&self, id: &Bytes) -> Result<StreamEntryID, StreamError> {
        if id.len() == 1 {
            match id[0] {
                b'*' | b'+' => {
                    let miliseconds = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis();
                    let sequence = self.get_next_sequence_number(miliseconds);

                    return Ok(StreamEntryID::new(miliseconds, sequence));
                }
                b'-' => return Ok(StreamEntryID::new(0, 0)),
                c => {
                    return Err(StreamError::Other(anyhow!(
                        "protocol error; unknown ID special symbol {:}",
                        c
                    )));
                }
            }
        }

        let miliseconds = {
            if id[0] == b'*' {
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis()
            } else {
                use atoi::atoi;
                let dash_index = id
                    .iter()
                    .position(|b| *b == b'-')
                    .ok_or(StreamError::Other(anyhow!("Missing '-' in ID")))?;
                atoi::<u128>(&id[..dash_index]).ok_or(StreamError::Other(anyhow!(
                    "protocol error; expected u128 bytes for miliseconds time"
                )))?
            }
        };

        let sequence = {
            let dash_index = id
                .iter()
                .position(|b| *b == b'-')
                .ok_or(StreamError::Other(anyhow!("Missing '-' in ID")))?;

            if id[dash_index + 1] == b'*' {
                self.get_next_sequence_number(miliseconds)
            } else {
                use atoi::atoi;
                atoi::<u64>(&id[dash_index + 1..]).ok_or(StreamError::Other(anyhow!(
                    "protocol error; expected u64 bytes for sequence"
                )))?
            }
        };

        Ok(StreamEntryID::new(miliseconds, sequence))
    }

    fn validate_new_entry_id(&self, entry_id: StreamEntryID) -> Result<StreamEntryID, StreamError> {
        let last_key_value = self.entries.last_key_value();

        if entry_id.miliseconds == 0 && entry_id.sequence == 0 {
            return Err(StreamError::ZeroZeroID);
        } else if let Some((key, _)) = last_key_value {
            if key >= &entry_id {
                return Err(StreamError::NotGreaterThanLastId);
            }
        }

        Ok(entry_id)
    }

    pub fn insert(&mut self, id: Bytes, values: StreamEntry) -> Result<StreamEntryID, StreamError> {
        let id = self.generate_id(&id)?;
        self.validate_new_entry_id(id)?;

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

    pub fn xrange_bounds(
        &self,
        start: Bound<&StreamEntryID>,
        end: Bound<&StreamEntryID>,
    ) -> XRange {
        XRange {
            entries: self
                .entries
                .range((start, end))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        }
    }

    pub fn xread(&self, start_id: &StreamEntryID) -> Result<XRange, StreamError> {
        let end_id = StreamEntryID::new(u128::MAX, u64::MAX);

        Ok(self.xrange_bounds(Excluded(&start_id), Included(&end_id)))
    }

    pub fn xrange(&self, start_id: &Bytes, end_id: &Bytes) -> Result<XRange, StreamError> {
        let start_id = self.generate_id(start_id)?;
        let end_id = self.generate_id(end_id)?;

        Ok(self.xrange_bounds(Included(&start_id), Included(&end_id)))
    }
}
