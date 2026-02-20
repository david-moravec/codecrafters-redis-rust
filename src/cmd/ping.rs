use crate::frame::Frame;
use crate::parser::Parse;

pub struct Ping {}

impl Ping {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        Ok(Ping {})
    }
    pub async fn apply(
        &self,
        db: &crate::db::Db,
        dst: &mut crate::connection::Connection,
    ) -> anyhow::Result<()> {
        let frame = Frame::Simple("PONG".to_string());
        dst.write_frame(&frame).await?;
        Ok(())
    }

    pub fn get_name(&self) -> &str {
        "ping"
    }
}
