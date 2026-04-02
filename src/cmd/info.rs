use crate::frame::{Frame, ToFrame};
use crate::parser::Parse;
use crate::server::info::ServerInfo;

#[derive(Debug)]
pub struct Info {
    arg: Option<String>,
}

impl Info {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        Ok(Info {
            arg: parse.next_string().ok(),
        })
    }
    pub fn apply(self, server_info: ServerInfo) -> anyhow::Result<Frame> {
        Ok(server_info.to_frame())
    }
}
