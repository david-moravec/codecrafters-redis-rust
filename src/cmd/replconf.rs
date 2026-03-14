use crate::frame::Frame;
use crate::parser::Parse;

pub struct Replconf {}

impl Replconf {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        while let Ok(_) = parse.next_string() {}
        Ok(Replconf {})
    }
    pub fn apply(self, dst: &mut crate::connection::Connection) -> anyhow::Result<Frame> {
        let frame = Frame::Simple("OK".to_string());
        Ok(frame)
    }
}
