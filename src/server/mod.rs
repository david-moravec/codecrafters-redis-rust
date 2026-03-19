pub mod info;

use anyhow::{Result, anyhow};
use std::{net::SocketAddr, sync::Arc};
use tokio::{net::TcpListener, sync::broadcast::Sender};

use crate::db::Db;
use bytes::Bytes;
use tokio::net::TcpStream;
use tokio::sync::broadcast;

use crate::cmd::Command;
use crate::connection::Connection;
use crate::frame::Frame;
use info::{Role, ServerInfo};

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

    async fn replicate(&self, listener: &TcpListener, addr: &str) -> Result<()> {
        let connection = Connection::new(
            TcpStream::connect(addr).await?,
            self.info.clone(),
            self.replication_broadcast.tx.clone(),
        );
        let mut handler = Handle::new_slave_handler(self.db.clone(), connection);

        let local_address = listener.local_addr()?;

        tokio::spawn(async move {
            if let Err(err) = handler.replicate(local_address).await {
                eprint!("replication error {:}", err);
            }
        });

        Ok(())
    }

    pub async fn run(&self, listener: TcpListener) -> Result<()> {
        if let Role::Slave(ref addr) = self.info.replication.role {
            self.replicate(&listener, addr).await?;
        }

        loop {
            let socket = listener.accept().await?;
            let connection = Connection::new(
                socket.0,
                self.info.clone(),
                self.replication_broadcast.tx.clone(),
            );
            let handler = Handle::new(self.db.clone(), connection);

            tokio::spawn(async move {
                if let Err(err) = handler.run().await {
                    eprint!("{:}", err);
                }
            });
        }
    }
}

#[derive(Debug)]
enum ReplicationEnd {
    Master(broadcast::Receiver<Frame>),
    Slave,
}

#[derive(Debug)]
enum HandlerState {
    Client,
    Replication(ReplicationEnd),
}

pub struct Handle {
    pub(crate) db: Db,
    connection: Connection,
    offset: usize,
    state: HandlerState,
}

impl Handle {
    pub fn new(db: Db, connection: Connection) -> Self {
        Handle {
            db,
            connection,
            offset: 0,
            state: HandlerState::Client,
        }
    }

    pub fn new_slave_handler(db: Db, connection: Connection) -> Self {
        Handle {
            db,
            connection,
            offset: 0,
            state: HandlerState::Replication(ReplicationEnd::Slave),
        }
    }

    async fn replicate(&mut self, local_address: SocketAddr) -> Result<()> {
        match &self.state {
            HandlerState::Replication(ReplicationEnd::Slave) => {
                self.connection
                    .write_frame(&Frame::Array(Some(vec![Frame::BulkString(Bytes::from(
                        "PING",
                    ))])))
                    .await?;
                let _ = self.connection.read_frame().await?;

                self.connection
                    .send_command(&[
                        "REPLCONF",
                        "listening-port",
                        format!("{:}", local_address.port()).as_str(),
                    ])
                    .await?;
                let _ = self.connection.read_frame().await?;

                self.connection
                    .send_command(&["REPLCONF", "capa", "psync2"])
                    .await?;
                let _ = self.connection.read_frame().await?;

                self.connection.send_command(&["PSYNC", "?", "-1"]).await?;
                self.connection.read_frame().await?;
                self.connection.read_rdb_file().await?;

                loop {
                    let maybe_frame = self.connection.read_frame().await?;

                    let frame = match maybe_frame {
                        Some(frame) => frame,
                        None => return Ok(()),
                    };

                    let frame_bytes_len = frame.to_bytes().len();
                    let command = Command::from_frame(frame)?;

                    if let Command::Replconf(cmd) = command {
                        let frame = cmd.apply(&mut self.connection, self.offset)?;
                        self.connection.write_frame(&frame).await?;
                    } else {
                        command.apply(&self.db, &mut self.connection).await?;
                    }

                    self.offset += frame_bytes_len;
                }
            }
            s => Err(anyhow!(
                "Run replication can be invoked only on slave end of replication handle; current state is {:?}",
                s
            )),
        }
    }

    pub(crate) async fn run(mut self) -> Result<()> {
        if let HandlerState::Replication(ReplicationEnd::Slave) = self.state {
            return Err(anyhow!(
                "for running replication handler use 'replicate' method"
            ));
        }

        loop {
            match self.state {
                HandlerState::Client => {
                    let maybe_frame = self.connection.read_frame().await?;

                    let frame = match maybe_frame {
                        Some(frame) => frame,
                        None => return Ok(()),
                    };

                    let command = Command::from_frame(frame)?;

                    if let Command::Psync(cmd) = command {
                        let dst = &mut self.connection;
                        dst.write_frame(&cmd.apply(dst)?).await?;
                        dst.write_rdb_file(self.db.to_rdb_file()).await?;

                        self.state = HandlerState::Replication(ReplicationEnd::Master(
                            dst.frame_broadcast.subscribe(),
                        ));
                    } else if let Command::Replconf(cmd) = command {
                        let frame = cmd.apply(&mut self.connection, self.offset)?;
                        self.connection.write_frame(&frame).await?;
                    } else if let Command::Wait(cmd) = command {
                        let frame = cmd.apply()?;
                        self.connection.write_frame(&frame).await?;
                    } else {
                        let response = command.apply(&self.db, &mut self.connection).await?;
                        self.connection.write_frame(&response).await?;
                    }
                }
                HandlerState::Replication(ReplicationEnd::Master(ref mut rx)) => {
                    tokio::select! {
                        _result = self.connection.read_frame() => {}
                        result = rx.recv() => {
                            self.connection.write_frame(&result?).await?;
                        }
                    }
                }
                _ => unreachable!(),
            }
        }
    }
}
