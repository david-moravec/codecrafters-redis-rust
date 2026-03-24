use crate::frame::Frame;
use crate::parser::Parse;

#[derive(Debug)]
pub struct Multi {}

impl Multi {
    pub fn parse(_: &mut Parse) -> anyhow::Result<Self> {
        Ok(Multi {})
    }
    pub fn apply(self, dst: &mut crate::connection::Connection) -> anyhow::Result<Frame> {
        dst.is_queueing_commands = true;
        let frame = Frame::Simple("OK".to_string());
        Ok(frame)
    }
}
