mod cmd;
mod connection;
mod db;
mod frame;
mod parser;
mod server;
mod stream;

use crate::server::Server;

#[tokio::main]
async fn main() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:6379")
        .await
        .unwrap();
    let server = Server::new();
    if let Err(e) = server.run(listener).await {
        eprintln!("When running server following error occured: \n{}", e);
    };
}
