mod cmd;
mod connection;
mod db;
mod frame;
mod parser;
mod server;
mod stream;

use crate::server::Server;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about=None)]
struct Args {
    #[arg(short, long, default_value = "6379")]
    port: String,
    #[arg(long)]
    replicaof: Option<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let listener = tokio::net::TcpListener::bind(format!("127.0.0.1:{:}", args.port))
        .await
        .unwrap();
    let server = Server::new({
        if args.replicaof.is_some() {
            server::Role::Slave
        } else {
            server::Role::Master
        }
    });
    if let Err(e) = server.run(listener).await {
        eprintln!("When running server following error occured: \n{}", e);
    };
}
