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
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        time::timeout,
    };

    use super::*;
    use std::{net::SocketAddr, time::Duration};

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

    #[tokio::test]
    async fn test_set() {
        let addr = start_server().await;

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"*3\r\n+SET\r\n$3\r\nfoo\r\n$4\r\nbars\r\n")
            .await
            .unwrap();

        let mut response = [0; 5];
        stream.read_exact(&mut response).await.unwrap();

        assert_eq!(b"+OK\r\n", &response);
    }

    #[tokio::test]
    async fn test_get_set() {
        let addr = start_server().await;
        let mut stream = TcpStream::connect(addr).await.unwrap();

        stream
            .write_all(b"*2\r\n+GET\r\n$5\r\nhello\r\n")
            .await
            .unwrap();

        let mut response = [0; 5];
        stream.read_exact(&mut response).await.unwrap();

        assert_eq!(b"$-1\r\n", &response);

        stream
            .write_all(b"*3\r\n+SET\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
            .await
            .unwrap();

        let mut response = [0; 5];
        stream.read_exact(&mut response).await.unwrap();

        assert_eq!(b"+OK\r\n", &response);

        stream
            .write_all(b"*2\r\n+GET\r\n$5\r\nhello\r\n")
            .await
            .unwrap();

        let mut response = [0; 11];
        stream.read_exact(&mut response).await.unwrap();

        assert_eq!(b"$5\r\nworld\r\n", &response);
    }

    #[tokio::test]
    async fn test_lrange() {
        let addr = start_server().await;
        let mut stream = TcpStream::connect(addr).await.unwrap();

        stream
            .write_all(b"*7\r\n+RPUSH\r\n$8\r\nlist_key\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n")
            .await
            .unwrap();

        let mut response = [0; 4];
        stream.read_exact(&mut response).await.unwrap();

        assert_eq!(b":5\r\n", &response);

        stream
            .write_all(b"*4\r\n+LRANGE\r\n$8\r\nlist_key\r\n$2\r\n-2\r\n$2\r\n-1\r\n")
            .await
            .unwrap();

        let mut response = [0; 18];
        stream.read_exact(&mut response).await.unwrap();

        assert_eq!(b"*2\r\n$1\r\nd\r\n$1\r\ne\r\n", &response);
    }

    // #[tokio::test]
    // async fn test_blpop() {
    //     let addr = start_server().await;
    //     let mut stream_blpop = TcpStream::connect(addr).await.unwrap();

    //     let handle_1 = tokio::spawn(async move {
    //         stream_blpop
    //             .write_all(b"*3\r\n+BLPOP\r\n$8\r\nlist_key\r\n$1\r\n0\r\n")
    //             .await
    //             .unwrap();

    //         let mut response = [0; 25];
    //         stream_blpop.read_exact(&mut response).await.unwrap();
    //         assert_eq!(b"*2\r\n$8\r\nlist_key\r\n$1\r\na\r\n", &response);
    //     });

    //     let mut stream_blpop_2 = TcpStream::connect(addr).await.unwrap();

    //     let handle_2 = tokio::spawn(async move {
    //         stream_blpop_2
    //             .write_all(b"*3\r\n+BLPOP\r\n$8\r\nlist_key\r\n$1\r\n0\r\n")
    //             .await
    //             .unwrap();

    //         let mut response = [0; 25];
    //         stream_blpop_2.read_exact(&mut response).await.unwrap();
    //         assert_eq!(b"*2\r\n$8\r\nlist_key\r\n$1\r\nd\r\n", &response);
    //     });

    //     tokio::time::sleep(Duration::from_millis(50)).await;

    //     let mut stream_rpush = TcpStream::connect(addr).await.unwrap();

    //     stream_rpush
    //         .write_all(b"*7\r\n+RPUSH\r\n$8\r\nlist_key\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n")
    //         .await
    //         .unwrap();

    //     let mut response = [0; 4];
    //     stream_rpush.read_exact(&mut response).await.unwrap();

    //     assert_eq!(b":5\r\n", &response);

    //     handle_1.await.unwrap();
    //     handle_2.await.unwrap();
    // }

    #[tokio::test]
    async fn test_xadd() {
        let addr = start_server().await;
        let mut stream = TcpStream::connect(addr).await.unwrap();

        stream
            .write_all(b"*5\r\n+XADD\r\n$8\r\nsome_key\r\n$3\r\n1-1\r\n$3\r\nfoo\r\n$3\r\nbar\r\n")
            .await
            .unwrap();

        let mut response = [0; 9];
        stream.read_exact(&mut response).await.unwrap();

        assert_eq!(b"$3\r\n1-1\r\n", &response);

        stream
            .write_all(b"*5\r\n+XADD\r\n$8\r\nsome_key\r\n$3\r\n1-2\r\n$3\r\nfoa\r\n$3\r\nbor\r\n")
            .await
            .unwrap();

        let mut response = [0; 9];
        stream.read_exact(&mut response).await.unwrap();

        assert_eq!(b"$3\r\n1-2\r\n", &response);

        stream
            .write_all(b"*5\r\n+XADD\r\n$8\r\nsome_key\r\n$3\r\n1-2\r\n$3\r\nfoa\r\n$3\r\nbor\r\n")
            .await
            .unwrap();

        let mut response = [0; 83];
        stream.read_exact(&mut response).await.unwrap();

        assert_eq!(b"-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n", &response);

        stream
            .write_all(
                b"*5\r\n+XADD\r\n$8\r\nstream_key\r\n$3\r\n0-0\r\n$3\r\nfoa\r\n$3\r\nbor\r\n",
            )
            .await
            .unwrap();

        let mut response = [0; 56];
        stream.read_exact(&mut response).await.unwrap();

        assert_eq!(
            b"-ERR The ID specified in XADD must be greater than 0-0\r\n",
            &response
        );
    }
}
