pub mod info;

use anyhow::{Result, anyhow};
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    thread,
    time::{Duration, Instant},
};
use tokio::sync::{broadcast::Sender, mpsc, oneshot};

use crate::db::Db;
use bytes::Bytes;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;

use crate::cmd::Command;
use crate::connection::Connection;
use crate::frame::Frame;
use info::{Role, ServerCommand, ServerInfo};

#[derive(Clone)]
struct ReplicationBroadcast {
    command_propagation: Arc<Sender<Frame>>,
}

pub struct Server {
    db: Db,
    info: ServerInfo,
    replication_broadcast: ReplicationBroadcast,
}

impl Server {
    pub fn new(master_address: Option<String>) -> Self {
        let info = ServerInfo::new(master_address);
        let (tx, _) = broadcast::channel(16);

        Self {
            db: Db::new(),
            info,
            replication_broadcast: ReplicationBroadcast {
                command_propagation: Arc::new(tx),
            },
        }
    }
    async fn respond_to_queries(
        info: ServerInfo,
        mut query_rx: mpsc::Receiver<Query>,
        mut handle_response_rx: mpsc::Receiver<HandleResponse>,
        handle_frame_tx: Arc<broadcast::Sender<Frame>>,
    ) -> Result<()> {
        loop {
            match query_rx
                .recv()
                .await
                .ok_or(anyhow!("query channel closed"))?
            {
                Query::Wait {
                    count,
                    timeout,
                    response,
                } => {
                    let replica_count = info.replica_count()?;

                    if replica_count == 0 {
                        if let Err(_) = response.send(0) {};
                    } else {
                        handle_frame_tx.send(Frame::bulk_strings_array_from_str(vec![
                            "REPLCONF", "GETACK", "*",
                        ]))?;

                        let mut hit_count = 0;
                        let deadline = Instant::now() + timeout;

                        loop {
                            let remaining = deadline.saturating_duration_since(Instant::now());

                            if remaining.is_zero() {
                                break;
                            }

                            match tokio::time::timeout(remaining, handle_response_rx.recv()).await {
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

    async fn replicate(
        &self,
        listener: &TcpListener,
        addr: &str,
        query_tx: mpsc::Sender<Query>,
    ) -> Result<()> {
        let connection = Connection::new(
            TcpStream::connect(addr).await?,
            self.info.clone(),
            self.replication_broadcast.command_propagation.clone(),
        );
        let mut handler =
            Handle::new_slave_handler(self.db.clone(), connection, self.info.clone(), query_tx);

        let local_address = listener.local_addr()?;

        tokio::spawn(async move {
            if let Err(err) = handler.replicate(local_address).await {
                eprint!("replication error {:}", err);
            }
        });

        Ok(())
    }

    pub async fn run(&self, listener: TcpListener) -> Result<()> {
        // let (handle_response_tx, handle_response_rx) = mpsc::channel(100);
        let (query_tx, query_rx) = mpsc::channel(100);

        let master_addres = match self.info.replication_role() {
            &Role::Slave(ref addr) => Some(addr.clone()),
            &Role::Master { .. } => None,
        };

        if master_addres.is_some() {
            self.replicate(&listener, &master_addres.unwrap(), query_tx.clone())
                .await?;
        }

        let info = self.info.clone();
        let handle_frame_tx = self.replication_broadcast.command_propagation.clone();

        tokio::spawn(async move {
            if let Err(e) = Self::respond_to_queries(info, query_rx, handle_frame_tx).await {
                eprintln!("during handling queries followign error occured; {:}", e);
            }
        });

        loop {
            let socket = listener.accept().await?;
            let connection = Connection::new(
                socket.0,
                self.info.clone(),
                self.replication_broadcast.command_propagation.clone(),
            );
            let handler = Handle::new(
                self.db.clone(),
                connection,
                self.info.clone(),
                query_tx.clone(),
            );

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
    Master {
        command_propagation_rx: broadcast::Receiver<Frame>,
        server_command_rx: broadcast::Receiver<ServerCommand>,
    },
    Slave,
}

type HandleResponse = Frame;

#[derive(Debug)]
enum HandlerState {
    Client,
    Replication(ReplicationEnd),
}

pub enum Query {
    Wait {
        count: u64,
        timeout: Duration,
        response: oneshot::Sender<u64>,
    },
}

pub struct Handle {
    pub(crate) db: Db,
    connection: Connection,
    offset: usize,
    state: HandlerState,
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
            offset: 0,
            state: HandlerState::Client,
            server_info,
            query_tx,
        }
    }

    pub fn new_slave_handler(
        db: Db,
        connection: Connection,
        server_info: ServerInfo,
        query_tx: mpsc::Sender<Query>,
    ) -> Self {
        Handle {
            db,
            connection,
            offset: 0,
            state: HandlerState::Replication(ReplicationEnd::Slave),
            server_info,
            query_tx,
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

                        self.state = HandlerState::Replication(ReplicationEnd::Master {
                            command_propagation_rx: dst.frame_broadcast.subscribe(),
                            server_command_rx: self.server_info.server_broadcast_subscribe(),
                        });
                        self.server_info.increment_replica_count()?;
                    } else if let Command::Replconf(cmd) = command {
                        let frame = cmd.apply(&mut self.connection, self.offset)?;
                        self.connection.write_frame(&frame).await?;
                    } else if let Command::Wait(cmd) = command {
                        let frame = cmd.apply(&mut self.query_tx).await?;
                        self.connection.write_frame(&frame).await?;
                    } else {
                        let response = command.apply(&self.db, &mut self.connection).await?;
                        self.connection.write_frame(&response).await?;
                    }
                }
                // once changed to master end replica connection
                // it cannot go back so we can loop to avoid unnecessary
                // outer loop
                HandlerState::Replication(ReplicationEnd::Master {
                    ref mut command_propagation_rx,
                    ref mut server_command_rx,
                }) => loop {
                    tokio::select! {
                        result = self.connection.read_frame() => {}
                        propagated_frame = command_propagation_rx.recv() => {
                            self.connection.write_frame(&propagated_frame?).await?;
                        }
                        server_cmd = server_command_rx.recv() => {
                            let server_cmd = server_cmd?;
                            self.connection.write_frame(&server_cmd.cmd).await?;
                            let response = self.connection.read_frame().await?;

                            if response.is_some() {
                                server_cmd.response_channel.send(response.unwrap()).await?
                            }

                        }
                    }
                },
                _ => unreachable!(),
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::time::Duration;

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

        let mut master_replica_connection =
            Connection::new(master_replica_stream, ServerInfo::new(None), Arc::new(tx));

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

    #[tokio::test]
    async fn test_wait_no_replicas() {
        let addr = start_master("0").await;
        let stream = TcpStream::connect(addr).await.unwrap();
        let tx: Sender<Frame> = broadcast::channel(16).0;
        let mut connection = Connection::new(stream, ServerInfo::new(None), Arc::new(tx));

        connection
            .send_command(&["WAIT", "0", "500"])
            .await
            .unwrap();

        let maybe_frame = connection.read_frame().await.unwrap();
        let f = match maybe_frame {
            Some(f) => f,
            None => panic!("nothing to read"),
        };

        eprintln!("{:?}", f);
        eprintln!("{:?}", f.to_bytes());

        assert!(f.to_bytes().to_vec() == b":0\r\n");
    }
}
