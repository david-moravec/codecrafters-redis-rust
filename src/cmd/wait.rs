use crate::frame::Frame;
use crate::parser::Parse;
use bytes::Bytes;

pub struct Wait {
    replica_count: u64,
    timeout: u64,
}

impl Wait {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let replica_count = parse.next_u64()?;
        let timeout = parse.next_u64()?;
        Ok(Wait {
            replica_count,
            timeout,
        })
    }
    pub fn apply(self, replica_count: u64) -> anyhow::Result<Frame> {
        let frame = { Frame::Integer(replica_count) };
        Ok(frame)
    }
}
