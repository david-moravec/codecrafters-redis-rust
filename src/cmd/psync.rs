use bytes::Bytes;

use crate::frame::Frame;
use crate::parser::Parse;

pub struct Psync {}

impl Psync {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        while let Ok(_) = parse.next_string() {}
        Ok(Psync {})
    }
    pub fn apply(self, dst: &crate::connection::Connection) -> anyhow::Result<Frame> {
        let frame = match &dst.server_info.replication.role {
            crate::server::info::Role::Master {
                repl_id,
                repl_offset,
            } => Frame::Simple(format!("FULLRESYNC {} {:}", repl_id, repl_offset)),
            crate::server::info::Role::Slave(_) => {
                Frame::Error("ERR slave does not currently support psync".to_string())
            }
        };
        Ok(frame)
    }
}
