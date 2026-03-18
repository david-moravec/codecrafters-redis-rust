pub mod info;

use anyhow::Result;
use std::{net::SocketAddr, sync::Arc};
use tokio::{net::TcpListener, sync::broadcast::Sender};

use crate::{connection::ConnectionType, db::Db};
use bytes::Bytes;
use tokio::net::TcpStream;
use tokio::sync::broadcast;

use crate::cmd::Command;
use crate::connection::Connection;
use crate::frame::Frame;
use info::{Role, ServerInfo};

async fn check_for_replication(
    info: Arc<ServerInfo>,
    local_addres: SocketAddr,
    replication_tx: Arc<Sender<Frame>>,
) -> Result<Option<Connection>> {
    match info.replication.role {
        Role::Slave(ref addr) => {
            let mut connection =
                Connection::new(TcpStream::connect(addr).await?, info, replication_tx);

            connection
                .write_frame(&Frame::Array(Some(vec![Frame::BulkString(Bytes::from(
                    "PING",
                ))])))
                .await?;
            let _ = connection.read_frame().await?;

            connection
                .send_command(&[
                    "REPLCONF",
                    "listening-port",
                    format!("{:}", local_addres.port()).as_str(),
                ])
                .await?;
            let _ = connection.read_frame().await?;

            connection
                .send_command(&["REPLCONF", "capa", "psync2"])
                .await?;
            let _ = connection.read_frame().await?;

            connection.send_command(&["PSYNC", "?", "-1"]).await?;
            connection.read_frame().await?;
            connection.read_rdb_file().await?;
            Ok(Some(connection))
        }
        _ => Ok(None),
    }
}

#[derive(Clone)]
pub struct ReplicationBroadcast {
    tx: Arc<Sender<Frame>>,
}

pub struct Server {
    db: Db,
    info: Arc<ServerInfo>,
    replication_broadcast: ReplicationBroadcast,
}

impl Server {
    pub fn new(replica_of: Option<String>) -> Self {
        let info = ServerInfo::new(replica_of);

        let (tx, _) = broadcast::channel(16);

        Self {
            db: Db::new(),
            info: Arc::new(info),
            replication_broadcast: ReplicationBroadcast { tx: Arc::new(tx) },
        }
    }

    async fn replicate(&self, listener: &TcpListener) -> Result<()> {
        if let Some(repl_connection) = check_for_replication(
            self.info.clone(),
            listener.local_addr()?,
            self.replication_broadcast.tx.clone(),
        )
        .await?
        {
            let mut handler = Handle::new(self.db.clone(), repl_connection, true);
            tokio::spawn(async move {
                if let Err(err) = handler.run().await {
                    eprint!("replication error {:}", err);
                }
            });
        }

        Ok(())
    }

    pub async fn run(&self, listener: TcpListener) -> Result<()> {
        self.replicate(&listener).await?;
        self.run_db(listener).await
    }

    async fn run_db(&self, listener: TcpListener) -> Result<()> {
        loop {
            let socket = listener.accept().await?;
            let connection = Connection::new(
                socket.0,
                self.info.clone(),
                self.replication_broadcast.tx.clone(),
            );
            let mut handler = Handle::new(self.db.clone(), connection, false);

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
    offset: usize,
    silent: bool,
}

impl Handle {
    pub fn new(db: Db, connection: Connection, silent: bool) -> Self {
        Handle {
            db,
            connection,
            offset: 0,
            silent,
        }
    }

    pub(crate) async fn run(&mut self) -> Result<()> {
        loop {
            match self.connection.connection_type {
                ConnectionType::Client(_) => {
                    let maybe_frame = self.connection.read_frame().await?;

                    let frame = match maybe_frame {
                        Some(frame) => frame,
                        None => return Ok(()),
                    };

                    let offset = frame.to_bytes().len();

                    Command::from_frame(frame)?
                        .apply(&self.db, &mut self.connection, self.offset, self.silent)
                        .await?;
                    self.offset += offset;
                }
                ConnectionType::FromReplica(_) => self.connection.propagate_to_replica().await?,
            }
        }
    }
}
