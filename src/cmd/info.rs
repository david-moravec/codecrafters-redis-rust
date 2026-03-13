use crate::frame::Frame;
use crate::parser::Parse;

pub struct Info {
    arg: Option<String>,
}

impl Info {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        Ok(Info {
            arg: parse.next_string().ok(),
        })
    }
    pub async fn apply(self, dst: &mut crate::connection::Connection) -> anyhow::Result<Frame> {
        Ok(dst.info_to_frame())
    }
}
