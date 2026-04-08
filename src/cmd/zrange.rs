use crate::frame::Frame;
use crate::parser::Parse;

#[derive(Debug)]
pub struct ZRange {
    key: String,
    start: i64,
    stop: i64,
}

impl ZRange {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let key = parse.next_string()?;
        let start = parse.next_i64()?;
        let stop = parse.next_i64()?;

        Ok(ZRange { key, start, stop })
    }
    pub fn apply(self, db: &crate::db::Db) -> anyhow::Result<Frame> {
        let values = db.zrange(self.key, self.start, self.stop);

        let frame = Frame::Array(Some(
            values
                .into_iter()
                .map(|bytes| Frame::BulkString(bytes))
                .collect(),
        ));

        Ok(frame)
    }
}
