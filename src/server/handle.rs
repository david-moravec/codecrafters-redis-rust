use anyhow::{Result, anyhow};
use std::net::SocketAddr;
use std::time::Instant;
use thiserror::Error;
use tokio::sync::mpsc;

use crate::db::Db;
use bytes::Bytes;
use tokio::sync::broadcast;

use super::Query;
use super::info::{ServerCommand, ServerInfo};
use crate::cmd::Command;
use crate::connection::Connection;
use crate::frame::Frame;

pub struct SlaveReplicationHandle {
    connection: Connection,
    db: Db,
}

impl SlaveReplicationHandle {
    pub fn new(db: Db, connection: Connection) -> Self {
        Self { connection, db }
    }

    pub(super) async fn run(&mut self, local_address: SocketAddr) -> Result<()> {
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

        let mut offset = 0;

        loop {
            let maybe_frame = self.connection.read_frame().await?;

            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            let frame_bytes_len = frame.to_bytes().len();
            let command = Command::from_frame(frame)?;

            if let Command::Replconf(cmd) = command {
                let frame = cmd.apply(&mut self.connection, offset)?;
                self.connection.write_frame(&frame).await?;
            } else {
                command.apply(&self.db, &mut self.connection).await?;
            }

            offset += frame_bytes_len;
        }
    }
}

pub struct MasterReplicationHandle {
    connection: Connection,
    repl_frame_propagation: broadcast::Receiver<Frame>,
    server_command_propagation: broadcast::Receiver<ServerCommand>,
}

impl MasterReplicationHandle {
    pub(super) fn new(
        connection: Connection,
        repl_frame_propagation: broadcast::Receiver<Frame>,
        server_command_propagation: broadcast::Receiver<ServerCommand>,
    ) -> Self {
        Self {
            connection,
            repl_frame_propagation,
            server_command_propagation,
        }
    }

    pub(super) async fn run(mut self) -> Result<()> {
        loop {
            tokio::select! {
                _ = self.connection.read_frame() => {}
                propagated_frame = self.repl_frame_propagation.recv() => {
                    self.connection.write_frame(&propagated_frame?).await?;
                }
                server_cmd = self.server_command_propagation.recv() => {
                    let server_cmd = server_cmd?;
                    self.connection.write_frame(&server_cmd.cmd).await?;
                    let response = self.connection.read_frame().await?;

                    if response.is_some() {
                        server_cmd.response_channel.send(response.unwrap()).await?
                    }

                }
            }
        }
    }
}

#[derive(Error, Debug)]
pub(super) enum HandleError {
    #[error("replication started on this handle")]
    ReplicationStarted(Connection),
    #[error("while running following error occured; {0}")]
    Other(#[from] anyhow::Error),
}

pub(super) type HandleResult<T> = Result<T, HandleError>;

pub(super) struct Handle {
    pub(crate) db: Db,
    connection: Connection,
    server_info: ServerInfo,
    query_tx: mpsc::Sender<Query>,
}

impl Handle {
    pub fn new(
        db: Db,
        connection: Connection,
        server_info: ServerInfo,
        query_tx: mpsc::Sender<Query>,
    ) -> Self {
        Handle {
            db,
            connection,
            server_info,
            query_tx,
        }
    }

    pub(crate) async fn run(mut self) -> HandleResult<()> {
        loop {
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

                // self.state = HandlerState::Replication(ReplicationEnd::Master {
                //     command_propagation_rx: dst.frame_broadcast.subscribe(),
                //     server_command_rx: self.server_info.server_broadcast_subscribe(),
                // });

                self.server_info.increment_replica_count()?;

                return Err(HandleError::ReplicationStarted(self.connection));
            } else if let Command::Replconf(cmd) = command {
                let frame = cmd.apply(&mut self.connection, 0)?;
                self.connection.write_frame(&frame).await?;
            } else if let Command::Wait(cmd) = command {
                let frame = cmd.apply(&mut self.query_tx).await?;
                self.connection.write_frame(&frame).await?;
            } else {
                let response = command.apply(&self.db, &mut self.connection).await?;
                self.connection.write_frame(&response).await?;
            }
        }
    }
}

pub(super) struct ServerQueryHandle {
    info: ServerInfo,
    query_rx: mpsc::Receiver<Query>,
    server_cmd_tx: broadcast::Sender<ServerCommand>,
}

impl ServerQueryHandle {
    pub(super) fn new(
        info: ServerInfo,
        query_rx: mpsc::Receiver<Query>,
        server_cmd_tx: broadcast::Sender<ServerCommand>,
    ) -> Self {
        Self {
            info,
            query_rx,
            server_cmd_tx,
        }
    }

    pub(crate) async fn run(mut self) -> Result<()> {
        loop {
            match self
                .query_rx
                .recv()
                .await
                .ok_or(anyhow!("query channel closed"))?
            {
                Query::Wait {
                    count,
                    timeout,
                    response,
                } => {
                    let replica_count = self.info.replica_count()?;

                    if replica_count == 0 {
                        if let Err(_) = response.send(0) {};
                    } else {
                        let (tx, mut rx) = mpsc::channel(100);
                        let server_cmd = ServerCommand {
                            cmd: Frame::bulk_strings_array_from_str(vec![
                                "REPLCONF", "GETACK", "*",
                            ]),
                            response_channel: tx.clone(),
                        };

                        self.server_cmd_tx.send(server_cmd).map_err(|e| anyhow!("during sending cmd to replia conneciton following error occured; {:}", e))?;

                        let mut hit_count = 0;
                        let deadline = Instant::now() + timeout;

                        loop {
                            if hit_count == count {
                                break;
                            }
                            let remaining = deadline.saturating_duration_since(Instant::now());

                            if remaining.is_zero() {
                                break;
                            }

                            match tokio::time::timeout(remaining, rx.recv()).await {
                                Ok(_) => hit_count += 1,
                                Err(_) => break,
                            };
                        }

                        if let Err(_) = response.send(hit_count) {};
                    }
                }
            }
        }
    }
}
