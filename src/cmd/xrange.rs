use crate::frame::ToFrame;
use crate::parser::Parse;

use bytes::Bytes;

pub struct XRange {
    key: String,
    start: Bytes,
    stop: Bytes,
}

impl XRange {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let key = parse.next_string()?;
        let start = parse.next_bytes()?;
        let stop = parse.next_bytes()?;

        Ok(XRange { key, start, stop })
    }
    pub async fn apply(
        self,
        db: &crate::db::Db,
        dst: &mut crate::connection::Connection,
    ) -> anyhow::Result<()> {
        let xrange = db.xrange(self.key, &self.start, &self.stop)?;

        dst.write_frame(&xrange.to_frame()).await?;
        Ok(())
    }
}
