use anyhow::Result;
use std::net::SocketAddr;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_stream::{StreamExt, StreamMap};

use crate::cmd::psync::Psync;
use crate::cmd::subscribe::Subscribe;
use crate::db::Db;
use bytes::Bytes;
use tokio::sync::broadcast;

use super::info::{HandleInquiry, ServerInfo};
use super::subscription_channels::SubscriptionChannels;
use super::{ServerInquiry, subscription_channels};
use crate::cmd::{Command, ReplCommand, SubscriptionCommand};
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
            eprintln!(
                "[Replica@{:?}] recieved {:?}",
                self.connection.local_addr().port(),
                frame
            );
            let command = Command::from_frame(frame)?;

            match command {
                Command::Repl(cmd) => match cmd {
                    ReplCommand::Replconf(cmd) => {
                        let frame = cmd.apply(offset)?;
                        eprintln!(
                            "[Replica@{:?}] sending {:?} to {:?}",
                            self.connection.local_addr().port(),
                            frame,
                            self.connection.peer_addr(),
                        );
                        self.connection.write_frame(&frame).await?;
                    }
                    _ => unreachable!(),
                },
                Command::Db(cmd) => {
                    cmd.apply(&self.db, &mut self.connection).await?;
                }
                _ => unreachable!(),
            }

            offset += frame_bytes_len;
        }
    }
}

pub struct MasterReplicationHandle {
    info: ServerInfo,
    connection: Connection,
    repl_frame_propagation: broadcast::Receiver<Frame>,
    server_command_propagation: broadcast::Receiver<HandleInquiry>,
}

impl MasterReplicationHandle {
    pub(super) fn new(
        info: ServerInfo,
        connection: Connection,
        repl_frame_propagation: broadcast::Receiver<Frame>,
        server_command_propagation: broadcast::Receiver<HandleInquiry>,
    ) -> Self {
        Self {
            info,
            connection,
            repl_frame_propagation,
            server_command_propagation,
        }
    }

    async fn send_and_recieve(&mut self, server_cmd: &HandleInquiry) -> Result<()> {
        self.connection.write_frame(&server_cmd.cmd).await?;
        eprintln!(
            "[master:replica@{:?}]: sending     {:?}",
            self.connection.peer_addr().port(),
            server_cmd.cmd
        );
        let response = self.connection.read_frame().await?;
        eprintln!(
            "[master:replica@{:?}]: got         {:?}",
            self.connection.peer_addr().port(),
            response,
        );

        if response.is_some() {
            if let Err(_) = server_cmd.response_channel.send(response.unwrap()).await {};
        }

        Ok(())
    }

    pub(super) async fn run(mut self) -> Result<()> {
        loop {
            eprintln!(
                "[master:replica@{:?}]: ready to propagate or write",
                self.connection.peer_addr().port(),
            );
            tokio::select! {
                biased;
                propagated_frame = self.repl_frame_propagation.recv() => {
                    let frame = propagated_frame?;
                    eprintln!("[master:replica@{:?}]: propagating {:?}",   self.connection.peer_addr().port(),frame);
                    self.connection.write_frame(&frame).await?;
                    self.info.incement_offset(frame.to_bytes().len())?;
                    eprintln!("[master:replica@{:?}]: propagated",   self.connection.peer_addr().port());
                }
                server_cmd = self.server_command_propagation.recv() => {
                    let server_cmd = server_cmd?;
                    eprintln!("[master:replica@{:?}]: writing     {:?}",   self.connection.peer_addr().port(),server_cmd.cmd);
                    if let Err(_elapsed) = tokio::time::timeout(server_cmd.timeout, self.send_and_recieve(&server_cmd)).await {};
                    eprintln!("[master:replica@{:?}]: written",   self.connection.peer_addr().port());
                }
            }
        }
    }
}

#[derive(Error, Debug)]
pub(super) enum HandleError {
    #[error("replication started on this handle")]
    ReplicationStarted(Connection),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub(super) type HandleResult<T> = Result<T, HandleError>;

pub(super) struct Handle {
    pub(crate) db: Db,
    connection: Connection,
    server_info: ServerInfo,
    server_inquiry_tx: mpsc::Sender<ServerInquiry>,
}

impl Handle {
    pub fn new(
        db: Db,
        connection: Connection,
        server_info: ServerInfo,
        server_inquiry_tx: mpsc::Sender<ServerInquiry>,
    ) -> Self {
        Handle {
            db,
            connection,
            server_info,
            server_inquiry_tx,
        }
    }

    async fn start_replication(mut self, cmd: Psync) -> HandleResult<()> {
        let dst = &mut self.connection;
        dst.write_frame(&cmd.apply(self.server_info.clone())?)
            .await?;
        dst.write_rdb_file(self.db.to_rdb_file()).await?;
        self.server_info.increment_replica_count()?;

        return Err(HandleError::ReplicationStarted(self.connection));
    }

    async fn start_subscription(mut self, cmd: Subscribe) -> HandleResult<Self> {
        eprintln!("subscription starts");
        let mut streams = StreamMap::new();
        let frame = cmd.apply(&mut self.server_inquiry_tx, &mut streams).await?;
        eprintln!("{:?}", frame);
        self.connection.write_frame(&frame).await?;

        loop {
            tokio::select! {
                Some((channel_name, Ok(msg))) = streams.next() => {

                }
                result_maybe_frame = self.connection.read_frame() => {
                    let frame = match result_maybe_frame {
                        Ok(Some(frame)) => frame,
                        Ok(None) => {return Ok(self);},
                        Err(err) => return Err(err.into()),
                    };

                    let command = Command::from_frame(frame)?;

                    if let Command::Subscription(subscription_command) = command {

                    } else {
                        eprintln!("Not supported command in subscription mode")
                    }

                }

            }
        }
        // Ok(self)
    }

    pub(crate) async fn run(mut self) -> HandleResult<()> {
        loop {
            let maybe_frame = self.connection.read_frame().await?;

            let frame = match maybe_frame {
                Some(frame) => frame,
                None => return Ok(()),
            };

            let command = Command::from_frame(frame)?;

            match command {
                Command::Repl(cmd) => match cmd {
                    ReplCommand::Psync(cmd) => {
                        return self.start_replication(cmd).await;
                    }
                    ReplCommand::Replconf(cmd) => {
                        let frame = cmd.apply(0)?;
                        self.connection.write_frame(&frame).await?;
                    }
                },
                Command::Subscription(cmd) => match cmd {
                    SubscriptionCommand::Subscribe(cmd) => {
                        self = self.start_subscription(cmd).await?;
                    }
                },
                cmd => {
                    let frame = cmd
                        .apply(
                            &self.db,
                            &mut self.connection,
                            &mut self.server_inquiry_tx,
                            self.server_info.clone(),
                        )
                        .await?;
                    self.connection.write_frame(&frame).await?;
                }
            };
        }
    }
}

pub(super) struct ServerInquiryHandle {
    info: ServerInfo,
    query_rx: mpsc::Receiver<ServerInquiry>,
    handle_inquiry_tx: broadcast::Sender<HandleInquiry>,
}

impl ServerInquiryHandle {
    pub(super) fn new(
        info: ServerInfo,
        query_rx: mpsc::Receiver<ServerInquiry>,
        handle_inquiry_tx: broadcast::Sender<HandleInquiry>,
    ) -> Self {
        Self {
            info,
            query_rx,
            handle_inquiry_tx,
        }
    }

    pub(crate) async fn run(mut self) -> Result<()> {
        let mut subscription_channels = SubscriptionChannels::new();
        loop {
            let query = self.query_rx.recv().await;
            if let Some(query) = query {
                eprintln!("{:?}", query);
                query
                    .apply(
                        &mut self.handle_inquiry_tx,
                        self.info.clone(),
                        &mut subscription_channels,
                    )
                    .await?;
            } else {
                eprintln!("server inquiry channel closed");
                return Ok(());
            }
        }
    }
}
