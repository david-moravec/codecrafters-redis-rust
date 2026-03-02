use crate::frame::Frame;
use crate::parser::Parse;

pub struct LRange {
    key: String,
    start: i64,
    stop: i64,
}

impl LRange {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let key = parse.next_string()?;
        let start = parse.next_i64()?;
        let stop = parse.next_i64()?;

        Ok(LRange { key, start, stop })
    }
    pub async fn apply(
        self,
        db: &crate::db::Db,
        dst: &mut crate::connection::Connection,
    ) -> anyhow::Result<()> {
        let values = db.lrange(&self.key, self.start, self.stop);

        let frame = Frame::Array(Some(
            values
                .into_iter()
                .map(|bytes| Frame::BulkString(bytes))
                .collect(),
        ));

        dst.write_frame(&frame).await?;
        Ok(())
    }
}
