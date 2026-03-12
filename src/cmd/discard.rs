use crate::frame::Frame;
use crate::parser::Parse;

pub struct Discard {}

impl Discard {
    pub fn parse(_: &mut Parse) -> anyhow::Result<Self> {
        Ok(Discard {})
    }
    pub fn apply(self, dst: &mut crate::connection::Connection) -> anyhow::Result<Frame> {
        let frame: Frame;
        if !dst.is_multi {
            frame = Frame::Error("ERR DISCARD without MULTI".to_string());
        } else {
            dst.is_multi = false;
            dst.multi_queue.drain(..);
            frame = Frame::Simple("OK".to_string())
        }
        Ok(frame)
    }
}
