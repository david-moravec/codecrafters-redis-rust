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
                eprintln!("starting handshake");
                self.connection
                    .write_frame(&Frame::Array(Some(vec![Frame::BulkString(Bytes::from(
                        "PING",
                    ))])))
                    .await?;
                let frame = self.connection.read_frame().await?;
                eprintln!("{:?}", frame);

                self.connection
                    .send_command(&[
                        "REPLCONF",
                        "listening-port",
                        format!("{:}", local_address.port()).as_str(),
                    ])
                    .await?;
                let frame = self.connection.read_frame().await?;
                eprintln!("{:?}", frame);

                self.connection
                    .send_command(&["REPLCONF", "capa", "psync2"])
                    .await?;
                let frame = self.connection.read_frame().await?;
                eprintln!("{:?}", frame);

                self.connection.send_command(&["PSYNC", "?", "-1"]).await?;
                eprintln!("{:?}", self.connection.read_frame().await?);
                self.connection.read_rdb_file().await?;
                eprintln!("completing handshake");

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
                        eprintln!(
                            "responding to {:?} with \n {:?}",
                            self.connection.stream, frame
                        );
                        tokio::join!(self.connection.write_frame(&frame)).0?;
                        eprintln!("replconf response written {:?}", self.connection.stream);
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
                        _result = self.connection.read_frame() => {
                            // eprintln!("{:?}", _result);
                        }
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

#[cfg(test)]
mod test {
    use std::time::Duration;

    use tokio::io::AsyncWriteExt;

    use super::*;

    async fn start_master(port: &str) -> SocketAddr {
        let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{:}", port))
            .await
            .unwrap();

        let server = Server::new(None);
        let local_addres = listener.local_addr().unwrap();

        tokio::spawn(async move {
            if let Err(e) = server.run(listener).await {
                eprintln!(
                    "when running server on port {:} following error occured; {}",
                    local_addres, e
                );
            }
        });

        local_addres
    }

    async fn start_replica(master_addr: String, port: &str) -> SocketAddr {
        let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{:}", port))
            .await
            .unwrap();

        let server = Server::new(Some(master_addr));
        let local_addres = listener.local_addr().unwrap();

        tokio::spawn(async move {
            if let Err(e) = server.run(listener).await {
                eprintln!(
                    "when running server on port {:} following error occured; {}",
                    local_addres, e
                );
            }
        });

        local_addres
    }

    async fn replication_handshake(master_replica_connection: &mut Connection) -> Result<()> {
        master_replica_connection.read_frame().await?;
        master_replica_connection.send_command(&["PONG"]).await?;
        // REPLCONF 1,
        master_replica_connection.read_frame().await?;
        master_replica_connection
            .write_frame(&Frame::Simple("OK".to_string()))
            .await?;
        // REPLCONF2,
        master_replica_connection.read_frame().await?;
        master_replica_connection
            .write_frame(&Frame::Simple("OK".to_string()))
            .await?;
        master_replica_connection.read_frame().await?;
        master_replica_connection
            .write_frame(&Frame::Simple(
                "FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0".to_string(),
            ))
            .await?;
        let rdb_file = Bytes::from(hex::decode(
            "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2",
        ).unwrap());
        master_replica_connection.write_rdb_file(rdb_file).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_replica() {
        // let master_address = start_master(None, "0").await;
        let master_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let master_address = master_listener.local_addr().unwrap();

        let replica_address = start_replica(
            format!("{} {}", master_address.ip(), master_address.port()),
            "0",
        )
        .await;

        let replication_socket = master_listener.accept().await.unwrap();

        let master_replica_stream = replication_socket.0;

        let tx: Sender<Frame> = broadcast::channel(16).0;

        let mut master_replica_connection = Connection::new(
            master_replica_stream,
            Arc::new(ServerInfo::new(None)),
            Arc::new(tx),
        );

        replication_handshake(&mut master_replica_connection)
            .await
            .unwrap();

        master_replica_connection
            .send_command(&["REPLCONF", "GETACK", "*"])
            .await
            .unwrap();

        match tokio::time::timeout(
            Duration::from_millis(500),
            master_replica_connection.read_frame(),
        )
        .await
        {
            Ok(frame_result) => {
                eprintln!("{:?}", frame_result);
                assert!(
                    frame_result.unwrap().unwrap().to_bytes().to_vec()
                        == b"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$1\r\n0\r\n"
                );
            }
            Err(_) => {
                eprintln!("timeout reached");
                assert!(false)
            }
        }
        master_replica_connection
            .send_command(&["PING"])
            .await
            .unwrap();
        master_replica_connection
            .send_command(&["REPLCONF", "GETACK", "*"])
            .await
            .unwrap();

        match tokio::time::timeout(
            Duration::from_millis(500),
            master_replica_connection.read_frame(),
        )
        .await
        {
            Ok(frame_result) => {
                eprint!("{:?}", frame_result);
                assert!(
                    frame_result.unwrap().unwrap().to_bytes().to_vec()
                        == b"*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$2\r\n51\r\n"
                );
            }
            Err(_) => {
                eprintln!("timeout reached");
                assert!(false)
            }
        }
    }
}
