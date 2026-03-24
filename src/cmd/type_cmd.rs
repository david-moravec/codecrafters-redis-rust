use crate::frame::Frame;
use crate::parser::Parse;

#[derive(Debug)]
pub struct Type {
    key: String,
}

impl Type {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let key = parse.next_string()?;

        Ok(Type { key })
    }
    pub fn apply(self, db: &crate::db::Db) -> anyhow::Result<Frame> {
        let type_str = db.value_type(&self.key);

        let frame = Frame::Simple(type_str);

        Ok(frame)
    }
}
