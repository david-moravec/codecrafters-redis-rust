use crate::frame::Frame;
use crate::parser::Parse;

pub struct Type {
    key: String,
}

impl Type {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let key = parse.next_string()?;

        Ok(Type { key })
    }
    pub async fn apply(
        self,
        db: &crate::db::Db,
        dst: &mut crate::connection::Connection,
    ) -> anyhow::Result<()> {
        let type_str = db.value_type(&self.key);

        let frame = Frame::Simple(type_str);

        dst.write_frame(&frame).await?;
        Ok(())
    }
}
