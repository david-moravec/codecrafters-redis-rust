mod blpop;
mod echo;
mod get;
mod llen;
mod lpop;
mod lpush;
mod lrange;
mod ping;
mod rpush;
mod set;

use crate::connection::Connection;
use crate::db::Db;
use crate::frame::Frame;
use crate::parser::Parse;

use anyhow::{Result, anyhow};
use blpop::BLPop;
use echo::Echo;
use get::Get;
use llen::LLen;
use lpop::LPop;
use lpush::LPush;
use lrange::LRange;
use ping::Ping;
use rpush::RPush;
use set::Set;

pub enum Command {
    Ping(Ping),
    Get(Get),
    Echo(Echo),
    Set(Set),
    RPush(RPush),
    LRange(LRange),
    LPush(LPush),
    LLen(LLen),
    LPop(LPop),
    BLPop(BLPop),
}

impl Command {
    pub async fn from_frame(frame: Frame) -> Result<Command> {
        let mut parse = Parse::new(frame)?;

        let command_name = parse.next_string()?.to_lowercase();

        let command = match &command_name[..] {
            "ping" => Command::Ping(Ping::parse(&mut parse)?),
            "get" => Command::Get(Get::parse(&mut parse)?),
            "echo" => Command::Echo(Echo::parse(&mut parse)?),
            "set" => Command::Set(Set::parse(&mut parse)?),
            "rpush" => Command::RPush(RPush::parse(&mut parse)?),
            "lrange" => Command::LRange(LRange::parse(&mut parse)?),
            "lpush" => Command::LPush(LPush::parse(&mut parse)?),
            "llen" => Command::LLen(LLen::parse(&mut parse)?),
            "lpop" => Command::LPop(LPop::parse(&mut parse)?),
            "blpop" => Command::BLPop(BLPop::parse(&mut parse)?),
            _ => return Err(anyhow!("protocol error; unknown command {:}", command_name)),
        };

        parse.finish()?;

        Ok(command)
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> Result<()> {
        match self {
            Self::Ping(cmd) => cmd.apply(db, dst).await,
            Self::Get(cmd) => cmd.apply(db, dst).await,
            Self::Echo(cmd) => cmd.apply(db, dst).await,
            Self::Set(cmd) => cmd.apply(db, dst).await,
            Self::RPush(cmd) => cmd.apply(db, dst).await,
            Self::LRange(cmd) => cmd.apply(db, dst).await,
            Self::LPush(cmd) => cmd.apply(db, dst).await,
            Self::LLen(cmd) => cmd.apply(db, dst).await,
            Self::LPop(cmd) => cmd.apply(db, dst).await,
            Self::BLPop(cmd) => cmd.apply(db, dst).await,
        }
    }
}
