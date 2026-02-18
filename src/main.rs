#![allow(unused_imports)]
use std::io::{BufReader, Write, prelude::*};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

mod redis_db;
mod redis_parser;
mod redis_vm;

use redis_parser::Parser;

use crate::redis_vm::RedisVM;

struct Server {
    vm: Arc<Mutex<RedisVM>>,
}

impl Server {
    fn new() -> Self {
        Self {
            vm: Arc::new(Mutex::new(RedisVM::new())),
        }
    }

    fn serve(&self, addr: &str) {
        let listener = TcpListener::bind(addr).unwrap();

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    let vm = Arc::clone(&self.vm);
                    thread::spawn(move || handle_connection(stream, vm));
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        }
    }
}

fn handle_connection(mut stream: TcpStream, redis_vm: Arc<Mutex<RedisVM>>) {
    println!("accepted new connection");

    let mut parser = Parser::new();

    loop {
        let mut buf = BufReader::new(&mut stream);
        let mut buff: [u8; 1024] = [0; 1024];

        let size = buf.read(&mut buff);

        match size {
            Ok(s) => {
                if s == 0 {
                    continue;
                } else {
                    let request_parsed = match parser.parse(&String::from_utf8_lossy(&buff[..s])) {
                        Ok(requests) => requests[0].clone(),
                        Err(err) => {
                            eprintln!("Parsing failed: {:}", err);
                            continue;
                        }
                    };

                    let response = {
                        let mut locked = redis_vm.lock().expect("locking failed");
                        locked.handle(request_parsed)
                    };

                    match response {
                        Ok(r) => {
                            stream.write(r.serialize().as_bytes());
                        }
                        Err(err) => eprintln!("{}", err),
                    }
                }
            }

            Err(e) => {
                eprintln!("{}", e);
            }
        };
    }
}

fn main() {
    let server = Server::new();
    server.serve("127.0.0.1:6379");
}
