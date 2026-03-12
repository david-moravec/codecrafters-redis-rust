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
    ) -> anyhow::Result<()> {
        if !dst.is_multi {
            let frame = Frame::Error("ERR EXEC without MULTI".to_string());
            dst.write_frame(&frame).await?;
        } else {
            dst.is_multi = false;

            let command_queue: Vec<Command> = dst.multi_queue.drain(..).collect();

            if command_queue.len() == 0 {
                dst.write_frame(&Frame::Array(Some(vec![]))).await?;
            }

            for cmd in command_queue {
                Box::pin(cmd.apply(db, dst)).await?;
            }
        }
        Ok(())
    }
}
