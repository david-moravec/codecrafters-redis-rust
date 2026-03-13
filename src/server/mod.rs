pub mod info;

use anyhow::Result;
use std::{net::SocketAddr, sync::Arc};
use tokio::net::TcpListener;

use crate::db::Db;
use bytes::Bytes;
use tokio::net::TcpStream;

use crate::cmd::Command;
use crate::connection::Connection;
use info::{Role, ServerInfo};

async fn check_for_replication(info: Arc<ServerInfo>, local_addres: SocketAddr) -> Result<()> {
    use crate::frame::Frame;

    if let Role::Slave(ref addr) = info.replication.role {
        let mut connection = Connection::new(TcpStream::connect(addr).await?, info);

        connection
            .write_frame(&Frame::Array(Some(vec![Frame::BulkString(Bytes::from(
                "PING",
            ))])))
            .await?;

        let response = connection.read_frame().await?;

        connection
            .write_frame(&Frame::Array(Some(vec![
                Frame::BulkString(Bytes::from("REPLCONF")),
                Frame::BulkString(Bytes::from("listening-port")),
                Frame::BulkString(Bytes::from(format!("{:}", local_addres.port()))),
            ])))
            .await?;
        let response = connection.read_frame().await?;

        connection
            .write_frame(&Frame::Array(Some(vec![
                Frame::BulkString(Bytes::from("REPLCONF")),
                Frame::BulkString(Bytes::from("capa")),
                Frame::BulkString(Bytes::from("psync2")),
            ])))
            .await?;
        let response = connection.read_frame().await?;
    }
    Ok(())
}

pub struct Server {
    db: Db,
    info: Arc<ServerInfo>,
}

impl Server {
    pub fn new(replica_of: Option<String>) -> Self {
        let info = ServerInfo::new(replica_of);

        Self {
            db: Db::new(),
            info: Arc::new(info),
        }
    }

    pub async fn run(&self, listener: TcpListener) -> Result<()> {
        check_for_replication(self.info.clone(), listener.local_addr()?).await?;

        loop {
            let socket = listener.accept().await?;
            let mut handler = Handle::new(self.db.clone(), socket.0, self.info.clone());

            tokio::spawn(async move {
                if let Err(err) = handler.run().await {
                    eprint!("{:}", err);
                }
            });
        }
    }
}

pub struct Handle {
    pub(crate) db: Db,
    connection: Connection,
}

impl Handle {
    pub fn new(db: Db, stream: TcpStream, info: Arc<ServerInfo>) -> Self {
        Handle {
            db,
            connection: Connection::new(stream, info),
        }
    }

    pub(crate) async fn run(&mut self) -> Result<()> {
        loop {
            let maybe_frame = self.connection.read_frame().await?;

            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            Command::from_frame(frame)
                .await?
                .apply(&self.db, &mut self.connection)
                .await?
        }
    }
}
