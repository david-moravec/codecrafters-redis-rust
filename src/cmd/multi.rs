use crate::frame::Frame;
use crate::parser::Parse;

pub struct Multi {}

impl Multi {
    pub fn parse(_: &mut Parse) -> anyhow::Result<Self> {
        Ok(Multi {})
    }
    pub async fn apply(
        self,
        _: &crate::db::Db,
        dst: &mut crate::connection::Connection,
    ) -> anyhow::Result<()> {
        let frame = Frame::Simple("OK".to_string());
        dst.write_frame(&frame).await?;
        Ok(())
    }
}
