mod blpop;
mod discard;
mod echo;
mod exec;
mod get;
mod incr;
mod info;
mod llen;
mod lpop;
mod lpush;
mod lrange;
mod multi;
mod ping;
mod psync;
mod replconf;
mod rpush;
mod set;
mod type_cmd;
mod xadd;
mod xrange;
mod xread;

use crate::connection::{Connection, ConnectionType};
use crate::db::Db;
use crate::frame::Frame;
use crate::parser::Parse;

use anyhow::{Result, anyhow};
use blpop::BLPop;
use discard::Discard;
use echo::Echo;
use exec::Exec;
use get::Get;
use incr::Incr;
use info::Info;
use llen::LLen;
use lpop::LPop;
use lpush::LPush;
use lrange::LRange;
use multi::Multi;
use ping::Ping;
use psync::Psync;
use replconf::Replconf;
use rpush::RPush;
use set::Set;
use type_cmd::Type;
use xadd::XAdd;
use xrange::XRange;
use xread::XRead;

pub enum Command {
    Ping(Ping),
    Get(Get),
    Echo(Echo),
    Set(Set),
    Incr(Incr),
    RPush(RPush),
    LRange(LRange),
    LPush(LPush),
    LLen(LLen),
    LPop(LPop),
    BLPop(BLPop),
    Type(Type),
    XAdd(XAdd),
    XRange(XRange),
    XRead(XRead),
    Multi(Multi),
    Exec(Exec),
    Discard(Discard),
    Info(Info),
    Replconf(Replconf),
    Psync(Psync),
}

impl Command {
    pub fn from_frame(frame: Frame) -> Result<Command> {
        let mut parse = Parse::new(frame)?;

        let command_name = parse.next_string()?.to_lowercase();

        let command = match &command_name[..] {
            "ping" => Command::Ping(Ping::parse(&mut parse)?),
            "get" => Command::Get(Get::parse(&mut parse)?),
            "echo" => Command::Echo(Echo::parse(&mut parse)?),
            "set" => Command::Set(Set::parse(&mut parse)?),
            "incr" => Command::Incr(Incr::parse(&mut parse)?),
            "rpush" => Command::RPush(RPush::parse(&mut parse)?),
            "lrange" => Command::LRange(LRange::parse(&mut parse)?),
            "lpush" => Command::LPush(LPush::parse(&mut parse)?),
            "llen" => Command::LLen(LLen::parse(&mut parse)?),
            "lpop" => Command::LPop(LPop::parse(&mut parse)?),
            "blpop" => Command::BLPop(BLPop::parse(&mut parse)?),
            "type" => Command::Type(Type::parse(&mut parse)?),
            "xadd" => Command::XAdd(XAdd::parse(&mut parse)?),
            "xrange" => Command::XRange(XRange::parse(&mut parse)?),
            "xread" => Command::XRead(XRead::parse(&mut parse)?),
            "multi" => Command::Multi(Multi::parse(&mut parse)?),
            "exec" => Command::Exec(Exec::parse(&mut parse)?),
            "discard" => Command::Discard(Discard::parse(&mut parse)?),
            "info" => Command::Info(Info::parse(&mut parse)?),
            "replconf" => Command::Replconf(Replconf::parse(&mut parse)?),
            "psync" => Command::Psync(Psync::parse(&mut parse)?),
            _ => return Err(anyhow!("protocol error; unknown command {:}", command_name)),
        };

        parse.finish()?;

        Ok(command)
    }

    pub async fn apply_queueble(self, db: &Db, dst: &mut Connection) -> Result<Frame> {
        match self {
            Self::Ping(cmd) => cmd.apply(db),
            Self::Get(cmd) => cmd.apply(db),
            Self::Echo(cmd) => cmd.apply(db),
            Self::Set(cmd) => cmd.apply(db, dst),
            Self::Incr(cmd) => cmd.apply(db),
            Self::RPush(cmd) => cmd.apply(db),
            Self::LRange(cmd) => cmd.apply(db),
            Self::LPush(cmd) => cmd.apply(db),
            Self::LLen(cmd) => cmd.apply(db),
            Self::LPop(cmd) => cmd.apply(db),
            Self::BLPop(cmd) => cmd.apply(db).await,
            Self::Type(cmd) => cmd.apply(db),
            Self::XAdd(cmd) => cmd.apply(db),
            Self::XRange(cmd) => cmd.apply(db),
            Self::XRead(cmd) => cmd.apply(db).await,
            Self::Info(cmd) => cmd.apply(dst).await,
            Self::Multi(_) => unreachable!(),
            Self::Discard(_) => unreachable!(),
            Self::Exec(_) => unreachable!(),
            Self::Replconf(_) => unreachable!(),
            Self::Psync(_) => unreachable!(),
        }
    }

    pub async fn apply(self, db: &Db, dst: &mut Connection) -> Result<()> {
        if let Self::Psync(cmd) = self {
            dst.write_frame(&cmd.apply(dst)?).await?;
            dst.write_rdb_file(db.to_rdb_file()).await?;
            dst.change_to_replica_connection()
        } else if let Self::Replconf(cmd) = self {
            let frame = cmd.apply(dst)?;
            dst.write_frame(&frame).await
        } else {
            let frame = {
                match self {
                    Self::Exec(cmd) => cmd.apply(db, dst).await?,
                    Self::Discard(cmd) => cmd.apply(dst)?,
                    Self::Multi(cmd) => cmd.apply(dst)?,
                    Self::Replconf(_) => unreachable!(),
                    Self::Psync(_) => unreachable!(),
                    _ => {
                        if dst.is_queueing_commands {
                            dst.command_queue.push_back(self);
                            Frame::Simple("QUEUED".to_string())
                        } else {
                            self.apply_queueble(db, dst).await?
                        }
                    }
                }
            };

            if let ConnectionType::Client(_) = dst.connection_type {
                dst.write_frame(&frame).await
            } else {
                Ok(())
            }
        }
    }
}
