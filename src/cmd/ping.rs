use crate::frame::Frame;
use crate::parser::Parse;

pub struct Ping {}

impl Ping {
    pub fn parse(_: &mut Parse) -> anyhow::Result<Self> {
        Ok(Ping {})
    }
    pub fn apply(self, _: &crate::db::Db) -> anyhow::Result<Frame> {
        let frame = Frame::Simple("PONG".to_string());
        Ok(frame)
    }
}
