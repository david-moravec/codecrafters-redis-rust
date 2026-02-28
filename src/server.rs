use anyhow::Result;
use tokio::net::TcpListener;

use crate::db::Db;
use tokio::net::TcpStream;

use crate::cmd::Command;
use crate::connection::Connection;

pub struct Server {
    db: Db,
}

impl Server {
    pub fn new() -> Self {
        Self { db: Db::new() }
    }

    pub async fn run(&self, listener: TcpListener) -> Result<()> {
        loop {
            let socket = listener.accept().await?;
            let mut handler = Handle::new(self.db.clone(), socket.0);

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
    pub fn new(db: Db, stream: TcpStream) -> Self {
        Handle {
            db,
            connection: Connection::new(stream),
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

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;
    use std::net::SocketAddr;

    pub async fn run(listener: TcpListener) {
        let server = Server::new();

        tokio::spawn(async move {
            if let Err(err) = server.run(listener).await {
                eprintln!("Error: {:}", err);
            }
        });
    }

    async fn start_server() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move { run(listener).await });

        addr
    }

    #[tokio::test]
    async fn test_ping() {
        let addr = start_server().await;

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(b"*1\r\n+PING\r\n").await.unwrap();

        let mut response = [0; 7];
        stream.read_exact(&mut response).await.unwrap();

        assert_eq!(b"+PONG\r\n", &response);
    }
}
