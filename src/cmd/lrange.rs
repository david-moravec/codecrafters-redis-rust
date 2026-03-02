use crate::frame::Frame;
use crate::parser::Parse;

pub struct LRange {
    key: String,
    start: u64,
    stop: u64,
}

impl LRange {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let key = parse.next_string()?;
        let start = parse.next_integer()?;
        let stop = parse.next_integer()?;

        Ok(LRange { key, start, stop })
    }
    pub async fn apply(
        self,
        db: &crate::db::Db,
        dst: &mut crate::connection::Connection,
    ) -> anyhow::Result<()> {
        let values = db.lrange(&self.key, self.start as usize, self.stop as usize);

        let frame = Frame::Array(Some(
            values
                .into_iter()
                .map(|bytes| Frame::BulkString(bytes))
                .collect(),
        ));

        dst.write_frame(&frame).await?;
        Ok(())
    }

    pub fn get_name(&self) -> &str {
        "lrange"
    }
}
