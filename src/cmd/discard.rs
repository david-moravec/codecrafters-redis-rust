use crate::frame::Frame;
use crate::parser::Parse;

pub struct Discard {}

impl Discard {
    pub fn parse(_: &mut Parse) -> anyhow::Result<Self> {
        Ok(Discard {})
    }
    pub fn apply(self, dst: &mut crate::connection::Connection) -> anyhow::Result<Frame> {
        let frame: Frame;
        if !dst.is_queueing_commands {
            frame = Frame::Error("ERR DISCARD without MULTI".to_string());
        } else {
            dst.is_queueing_commands = false;
            dst.command_queue.drain(..);
            frame = Frame::Simple("OK".to_string())
        }
        Ok(frame)
    }
}
