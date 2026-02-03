#![allow(unused_imports)]
use std::io::{BufReader, Write, prelude::*};
use std::net::{TcpListener, TcpStream};
use std::thread;

mod redis_db;
mod redis_parser;
mod redis_vm;

use crate::redis_vm::RedisVM;

fn handle_connection(mut stream: TcpStream) {
    println!("accepted new connection");

    let mut redis_vm = RedisVM::new();

    loop {
        let mut buf = BufReader::new(&mut stream);
        let mut buff: [u8; 1024] = [0; 1024];

        let size = buf.read(&mut buff);

        match size {
            Ok(s) => {
                if s == 0 {
                    continue;
                } else {
                    match redis_vm.handle(&String::from_utf8_lossy(&buff[..s])) {
                        Ok(_) => {
                            redis_vm.flush_output(&mut stream).unwrap();
                        }
                        Err(e) => eprintln!("{}", e),
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
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");

    // Uncomment the code below to pass the first stage
    //
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(mut _stream) => {
                thread::spawn(|| handle_connection(_stream));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}
