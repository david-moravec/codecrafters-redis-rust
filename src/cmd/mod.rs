mod get;
mod ping;
use crate::connection::Connection;
use crate::db::Db;
use crate::frame::Frame;
use crate::parser::Parse;

use anyhow::{Result, anyhow};
use get::Get;
use ping::Ping;

pub enum Command {
    Ping(Ping),
    Get(Get),
}

impl Command {
    pub async fn from_frame(frame: Frame) -> Result<Command> {
        let mut parse = Parse::new(frame)?;

        let command_name = parse.next_string()?.to_lowercase();

        let command = match &command_name[..] {
            "ping" => Command::Ping(Ping::parse(&mut parse)?),
            "get" => Command::Get(Get::parse(&mut parse)?),
            _ => return Err(anyhow!("protocol error; unknown command {:}", command_name)),
        };

        parse.finish()?;

        Ok(command)
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> Result<()> {
        match self {
            Self::Ping(cmd) => cmd.apply(db, dst).await,
            Self::Get(cmd) => cmd.apply(db, dst).await,
        }
    }
}
