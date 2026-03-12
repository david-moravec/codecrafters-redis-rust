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
    pub async fn apply(
        self,
        db: &crate::db::Db,
        dst: &mut crate::connection::Connection,
    ) -> anyhow::Result<()> {
        let frame = match db.incr(self.key) {
            Ok(number) => Frame::Integer(number),
            Err(err) => Frame::Error(format!("{}", err)),
        };
        dst.write_frame(&frame).await?;
        Ok(())
    }
}
