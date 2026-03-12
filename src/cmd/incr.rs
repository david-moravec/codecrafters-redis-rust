use crate::frame::Frame;
use crate::parser::Parse;

pub struct Incr {
    key: String,
}

impl Incr {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let key = parse.next_string()?;
        Ok(Incr { key })
    }
    pub fn apply(self, db: &crate::db::Db) -> anyhow::Result<Frame> {
        let frame = match db.incr(self.key) {
            Ok(number) => Frame::Integer(number),
            Err(err) => Frame::Error(format!("{}", err)),
        };
        Ok(frame)
    }
}
