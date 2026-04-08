pub mod psync;
pub mod server_inquiry;
pub mod subscribe;

mod blpop;
mod config;
mod discard;
mod echo;
mod exec;
mod get;
mod incr;
mod info;
mod keys;
mod llen;
mod lpop;
mod lpush;
mod lrange;
mod multi;
mod ping;
mod publish;
mod replconf;
mod rpush;
mod set;
mod type_cmd;
mod unsubscribe;
mod wait;
mod xadd;
mod xrange;
mod xread;
mod zadd;
mod zrange;
mod zrank;

use self::server_inquiry::ServerInquiry;
use crate::db::Db;
use crate::frame::Frame;
use crate::parser::Parse;
use crate::server::subscription_channels::SubscriptionMessage;
use crate::{connection::Connection, server::info::ServerInfo};
use tokio_stream::StreamMap;
use tokio_stream::wrappers::BroadcastStream;

use anyhow::{Result, anyhow};
use blpop::BLPop;
use config::Config;
use discard::Discard;
use echo::Echo;
use exec::Exec;
use get::Get;
use incr::Incr;
use info::Info;
use keys::Keys;
use llen::LLen;
use lpop::LPop;
use lpush::LPush;
use lrange::LRange;
use multi::Multi;
use ping::Ping;
use psync::Psync;
use publish::Publish;
use replconf::Replconf;
use rpush::RPush;
use set::Set;
use subscribe::Subscribe;
use tokio::sync::mpsc;
use type_cmd::Type;
use unsubscribe::Unsubscribe;
use wait::Wait;
use xadd::XAdd;
use xrange::XRange;
use xread::XRead;
use zadd::ZAdd;
use zrange::ZRange;
use zrank::ZRank;

#[derive(Debug)]
pub enum ReplCommand {
    Replconf(Replconf),
    Psync(Psync),
}

#[derive(Debug)]
pub enum SubscriptionCommand {
    Subscribe(Subscribe),
    Unsubscribe(Unsubscribe),
    Publish(Publish),
}

impl SubscriptionCommand {
    pub async fn apply(
        self,
        query_tx: &mut mpsc::Sender<ServerInquiry>,
        streams: &mut StreamMap<String, BroadcastStream<SubscriptionMessage>>,
    ) -> Result<Frame> {
        match self {
            Self::Subscribe(cmd) => cmd.apply(query_tx, streams).await,
            Self::Unsubscribe(cmd) => cmd.apply(streams),
            Self::Publish(cmd) => cmd.apply(query_tx).await,
        }
    }
}

#[derive(Debug)]
pub enum ServerCommand {
    Wait(Wait),
}

impl ServerCommand {
    pub async fn apply(self, query_tx: &mut mpsc::Sender<ServerInquiry>) -> Result<Frame> {
        match self {
            Self::Wait(cmd) => cmd.apply(query_tx).await,
        }
    }
}

#[derive(Debug)]
pub enum ServerInfoCommand {
    Info(Info),
    Config(Config),
}

impl ServerInfoCommand {
    pub fn apply(self, server_info: ServerInfo) -> Result<Frame> {
        match self {
            Self::Info(cmd) => cmd.apply(server_info),
            Self::Config(cmd) => cmd.apply(server_info),
        }
    }
}

#[derive(Debug)]
pub enum TransactionCommand {
    Multi(Multi),
    Exec(Exec),
    Discard(Discard),
}

impl TransactionCommand {
    pub async fn apply(self, db: &Db, dst: &mut Connection) -> Result<Frame> {
        match self {
            Self::Exec(cmd) => cmd.apply(db, dst).await,
            Self::Discard(cmd) => cmd.apply(dst),
            Self::Multi(cmd) => cmd.apply(dst),
        }
    }
}

#[derive(Debug)]
pub enum DbCommand {
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
    ZAdd(ZAdd),
    ZRank(ZRank),
    ZRange(ZRange),
    Keys(Keys),
}

impl DbCommand {
    pub async fn apply(self, db: &Db, dst: &mut Connection) -> Result<Frame> {
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
            Self::Keys(cmd) => cmd.apply(db),
            Self::XRead(cmd) => cmd.apply(db).await,
            Self::ZAdd(cmd) => cmd.apply(db),
            Self::ZRank(cmd) => cmd.apply(db),
            Self::ZRange(cmd) => cmd.apply(db),
        }
    }
}

#[derive(Debug)]
pub enum Command {
    Db(DbCommand),
    ServerInfo(ServerInfoCommand),
    Transaction(TransactionCommand),
    Repl(ReplCommand),
    Server(ServerCommand),
    Subscription(SubscriptionCommand),
}

impl Command {
    pub fn from_frame(frame: Frame) -> Result<Self> {
        let mut parse = Parse::new(frame)?;

        let command_name = parse.next_string()?.to_lowercase();

        let command = match &command_name[..] {
            "ping" => Command::Db(DbCommand::Ping(Ping::parse(&mut parse)?)),
            "get" => Command::Db(DbCommand::Get(Get::parse(&mut parse)?)),
            "echo" => Command::Db(DbCommand::Echo(Echo::parse(&mut parse)?)),
            "set" => Command::Db(DbCommand::Set(Set::parse(&mut parse)?)),
            "incr" => Command::Db(DbCommand::Incr(Incr::parse(&mut parse)?)),
            "rpush" => Command::Db(DbCommand::RPush(RPush::parse(&mut parse)?)),
            "lrange" => Command::Db(DbCommand::LRange(LRange::parse(&mut parse)?)),
            "lpush" => Command::Db(DbCommand::LPush(LPush::parse(&mut parse)?)),
            "llen" => Command::Db(DbCommand::LLen(LLen::parse(&mut parse)?)),
            "lpop" => Command::Db(DbCommand::LPop(LPop::parse(&mut parse)?)),
            "blpop" => Command::Db(DbCommand::BLPop(BLPop::parse(&mut parse)?)),
            "type" => Command::Db(DbCommand::Type(Type::parse(&mut parse)?)),
            "xadd" => Command::Db(DbCommand::XAdd(XAdd::parse(&mut parse)?)),
            "zadd" => Command::Db(DbCommand::ZAdd(ZAdd::parse(&mut parse)?)),
            "zrank" => Command::Db(DbCommand::ZRank(ZRank::parse(&mut parse)?)),
            "zrange" => Command::Db(DbCommand::ZRange(ZRange::parse(&mut parse)?)),
            "xrange" => Command::Db(DbCommand::XRange(XRange::parse(&mut parse)?)),
            "xread" => Command::Db(DbCommand::XRead(XRead::parse(&mut parse)?)),
            "keys" => Command::Db(DbCommand::Keys(Keys::parse(&mut parse)?)),
            "multi" => Command::Transaction(TransactionCommand::Multi(Multi::parse(&mut parse)?)),
            "exec" => Command::Transaction(TransactionCommand::Exec(Exec::parse(&mut parse)?)),
            "discard" => {
                Command::Transaction(TransactionCommand::Discard(Discard::parse(&mut parse)?))
            }
            "info" => Command::ServerInfo(ServerInfoCommand::Info(Info::parse(&mut parse)?)),
            "config" => Command::ServerInfo(ServerInfoCommand::Config(Config::parse(&mut parse)?)),
            "replconf" => Command::Repl(ReplCommand::Replconf(Replconf::parse(&mut parse)?)),
            "psync" => Command::Repl(ReplCommand::Psync(Psync::parse(&mut parse)?)),
            "wait" => Command::Server(ServerCommand::Wait(Wait::parse(&mut parse)?)),
            "subscribe" => Command::Subscription(SubscriptionCommand::Subscribe(Subscribe::parse(
                &mut parse,
            )?)),
            "unsubscribe" => Command::Subscription(SubscriptionCommand::Unsubscribe(
                Unsubscribe::parse(&mut parse)?,
            )),
            "publish" => {
                Command::Subscription(SubscriptionCommand::Publish(Publish::parse(&mut parse)?))
            }
            _ => return Err(anyhow!("protocol error; unknown command {:}", command_name)),
        };

        parse.finish()?;

        Ok(command)
    }

    pub async fn apply(
        self,
        db: &Db,
        dst: &mut Connection,
        query_tx: &mut mpsc::Sender<ServerInquiry>,
        server_info: ServerInfo,
    ) -> Result<Frame> {
        match self {
            Self::Repl(_) => unreachable!(),
            Self::Subscription(_) => unreachable!(),
            Self::Server(cmd) => cmd.apply(query_tx).await,
            Self::ServerInfo(cmd) => cmd.apply(server_info),
            Self::Transaction(cmd) => cmd.apply(db, dst).await,
            Self::Db(cmd) => {
                if dst.is_queueing_commands {
                    dst.command_queue.push_back(cmd);
                    Ok(Frame::Simple("QUEUED".to_string()))
                } else {
                    cmd.apply(db, dst).await
                }
            }
        }
    }

    pub fn name(&self) -> &str {
        match self {
            Command::Db(DbCommand::Ping(_)) => "ping",
            Command::Db(DbCommand::Get(_)) => "get",
            Command::Db(DbCommand::Echo(_)) => "echo",
            Command::Db(DbCommand::Set(_)) => "set",
            Command::Db(DbCommand::Incr(_)) => "incr",
            Command::Db(DbCommand::RPush(_)) => "rpush",
            Command::Db(DbCommand::LRange(_)) => "lrange",
            Command::Db(DbCommand::LPush(_)) => "lpush",
            Command::Db(DbCommand::LLen(_)) => "llen",
            Command::Db(DbCommand::LPop(_)) => "lpop",
            Command::Db(DbCommand::BLPop(_)) => "blpop",
            Command::Db(DbCommand::Type(_)) => "type",
            Command::Db(DbCommand::XAdd(_)) => "xadd",
            Command::Db(DbCommand::XRange(_)) => "xrange",
            Command::Db(DbCommand::XRead(_)) => "xread",
            Command::Db(DbCommand::Keys(_)) => "keys",
            Command::Transaction(TransactionCommand::Multi(_)) => "multi",
            Command::Transaction(TransactionCommand::Exec(_)) => "exec",
            Command::Transaction(TransactionCommand::Discard(_)) => "discard",
            Command::ServerInfo(ServerInfoCommand::Info(_)) => "info",
            Command::ServerInfo(ServerInfoCommand::Config(_)) => "config",
            Command::Repl(ReplCommand::Replconf(_)) => "replconf",
            Command::Repl(ReplCommand::Psync(_)) => "psync",
            Command::Server(ServerCommand::Wait(_)) => "wait",
            Command::Subscription(SubscriptionCommand::Subscribe(_)) => "subscribe",
            Command::Subscription(SubscriptionCommand::Publish(_)) => "publish",
            _ => unimplemented!("name not implemented for {:?}", self),
        }
    }
}
