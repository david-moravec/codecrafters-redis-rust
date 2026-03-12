use crate::frame::Frame;
use crate::parser::Parse;

pub struct Exec {}

impl Exec {
    pub fn parse(_: &mut Parse) -> anyhow::Result<Self> {
        Ok(Exec {})
    }
    pub async fn apply(
        self,
        _: &crate::db::Db,
        dst: &mut crate::connection::Connection,
    ) -> anyhow::Result<()> {
        let frame: Frame;
        if !dst.is_multi {
            frame = Frame::Error("ERR EXEC without MULTI".to_string())
        } else {
            dst.is_multi = false;
            frame = Frame::Error("EXEC not implemented yet".to_string());
        }
        dst.write_frame(&frame).await?;
        Ok(())
    }
}
