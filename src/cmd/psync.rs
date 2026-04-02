use crate::frame::Frame;
use crate::parser::Parse;
use crate::server::info::ServerInfo;

#[derive(Debug)]
pub struct Psync {}

impl Psync {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        for _ in 0..2 {
            let _ = parse.next_string();
        }
        Ok(Psync {})
    }
    pub fn apply(self, server_info: ServerInfo) -> anyhow::Result<Frame> {
        let frame = match server_info.replication_role() {
            crate::server::info::Role::Master { repl_id } => {
                Frame::Simple(format!("FULLRESYNC {} {}", repl_id, server_info.offset()?))
            }
            crate::server::info::Role::Slave(_) => {
                Frame::Error("ERR slave does not currently support psync".to_string())
            }
        };
        Ok(frame)
    }
}
