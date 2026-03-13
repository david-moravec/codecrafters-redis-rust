use crate::cmd::Command;
use crate::frame::Frame;
use crate::parser::Parse;

use std::boxed::Box;

pub struct Exec {}

impl Exec {
    pub fn parse(_: &mut Parse) -> anyhow::Result<Self> {
        Ok(Exec {})
    }
    pub async fn apply(
        self,
        db: &crate::db::Db,
        dst: &mut crate::connection::Connection,
    ) -> anyhow::Result<Frame> {
        let frame: Frame;
        if !dst.is_multi {
            frame = Frame::Error("ERR EXEC without MULTI".to_string());
        } else {
            dst.is_multi = false;
            let command_queue: Vec<Command> = dst.multi_queue.drain(..).collect();
            let mut responses: Vec<Frame> = vec![];

            for cmd in command_queue {
                responses.push(Box::pin(cmd.apply_queueble(db, dst)).await?);
            }

            frame = Frame::Array(Some(responses))
        }
        Ok(frame)
    }
}
