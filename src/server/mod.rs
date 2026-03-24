mod handle;
pub mod info;
mod replicationbroadcast;

use anyhow::Result;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

use self::handle::{Handle, MasterReplicationHandle, SlaveReplicationHandle};
use self::replicationbroadcast::ReplicationBroadcast;
use crate::{
    db::Db,
    server::handle::{HandleError, ServerQueryHandle},
};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;

use crate::connection::Connection;
use crate::frame::Frame;
use info::{Role, ServerInfo};

pub enum ReplicationCommand {
    Wait {
        count: u64,
        timeout: Duration,
        response: oneshot::Sender<u64>,
    },
}

pub struct Server {
    db: Db,
    info: ServerInfo,
    replication_broadcast: ReplicationBroadcast,
}

impl Server {
    pub fn new(master_address: Option<String>) -> Self {
        let info = ServerInfo::new(master_address);

        Self {
            db: Db::new(),
            info,
            replication_broadcast: ReplicationBroadcast::new(),
        }
    }

    async fn replicate(&self, listener: &TcpListener, addr: &str) -> Result<()> {
        let connection = Connection::new(
            TcpStream::connect(addr).await?,
            self.info.clone(),
            self.replication_broadcast.transmiter(),
        );
        let mut handler = SlaveReplicationHandle::new(self.db.clone(), connection);

        let local_address = listener.local_addr()?;

        tokio::spawn(async move {
            if let Err(err) = handler.run(local_address).await {
                eprint!("replication error {:}", err);
            }
        });

        Ok(())
    }

    async fn respond_to_queries(
        &self,
        info: ServerInfo,
        query_rx: mpsc::Receiver<ReplicationCommand>,
        server_cmd_tx: broadcast::Sender<info::ServerCommand>,
    ) -> Result<()> {
        let handler = ServerQueryHandle::new(info, query_rx, server_cmd_tx);

        tokio::spawn(async move {
            if let Err(err) = handler.run().await {
                eprintln!("during quering server following error occured; {:}", err);
            }
        });

        Ok(())
    }

    pub async fn run(&self, listener: TcpListener) -> Result<()> {
        let (query_tx, query_rx) = mpsc::channel(100);
        let (server_cmd_tx, _) = broadcast::channel(16);

        let master_addres = match self.info.replication_role() {
            &Role::Slave(ref addr) => Some(addr.clone()),
            &Role::Master { .. } => None,
        };

        if master_addres.is_some() {
            self.replicate(&listener, &master_addres.unwrap()).await?;
        }

        let info = self.info.clone();

        self.respond_to_queries(info, query_rx, server_cmd_tx.clone())
            .await?;

        loop {
            let socket = listener.accept().await?;
            let connection = Connection::new(
                socket.0,
                self.info.clone(),
                self.replication_broadcast.transmiter(),
            );
            let handler = Handle::new(
                self.db.clone(),
                connection,
                self.info.clone(),
                query_tx.clone(),
            );
            let handle_frame_tx = self.replication_broadcast.transmiter();
            let server_cmd_tx_cloned = server_cmd_tx.clone();
            let info = self.info.clone();

            tokio::spawn(async move {
                match handler.run().await {
                    Ok(_) => {}
                    Err(HandleError::ReplicationStarted(connection)) => {
                        let handler = MasterReplicationHandle::new(
                            info,
                            connection,
                            handle_frame_tx.subscribe(),
                            server_cmd_tx_cloned.subscribe(),
                        );

                        if let Err(err) = handler.run().await {
                            eprintln!("error occured on master end replicaiton: {:}", err);
                        }
                    }
                    Err(HandleError::Other(err)) => {
                        eprintln!("{:}", err);
                    }
                }
            });
        }
    }
}

#[cfg(test)]
mod test {
    use bytes::Bytes;
    use std::net::SocketAddr;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::broadcast;

    use crate::frame::Frame;

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

        let _ = start_replica(
            format!("{} {}", master_address.ip(), master_address.port()),
            "0",
        )
        .await;

        let replication_socket = master_listener.accept().await.unwrap();

        let master_replica_stream = replication_socket.0;

        let tx: broadcast::Sender<Frame> = broadcast::channel(16).0;

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
        let tx: broadcast::Sender<Frame> = broadcast::channel(16).0;
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
