pub mod info;

use anyhow::Result;
use std::sync::Arc;
use tokio::net::TcpListener;

use crate::db::Db;
use tokio::net::TcpStream;

use crate::cmd::Command;
use crate::connection::Connection;
use info::ServerInfo;

pub struct Server {
    db: Db,
    info: Arc<ServerInfo>,
}

impl Server {
    pub fn new(replica_of: Option<String>) -> Self {
        Self {
            db: Db::new(),
            info: Arc::new(ServerInfo::new(replica_of)),
        }
    }

    pub async fn run(&self, listener: TcpListener) -> Result<()> {
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
