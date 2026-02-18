use std::cell::RefCell;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::{Read, Write};

use anyhow::{Result, anyhow};

use crate::redis_db::RedisDB;
use crate::redis_parser::{Aggregate, NULL_STRING, Parser, RESPData, RESPMap, Simple};

enum Builtin {
    ECHO,
    PING,
    GET,
    SET,
    RPUSH,
    LPUSH,
    LRANGE,
    LLEN,
    LPOP,
}

impl Builtin {
    fn new(command: &str) -> Result<Self> {
        let command = command.to_uppercase();

        if command == "ECHO" {
            return Ok(Self::ECHO);
        } else if command == "PING" {
            return Ok(Self::PING);
        } else if command == "GET" {
            return Ok(Self::GET);
        } else if command == "SET" {
            return Ok(Self::SET);
        } else if command == "RPUSH" {
            return Ok(Self::RPUSH);
        } else if command == "LPUSH" {
            return Ok(Self::LPUSH);
        } else if command == "LRANGE" {
            return Ok(Self::LRANGE);
        } else if command == "LLEN" {
            return Ok(Self::LLEN);
        } else if command == "LPOP" {
            return Ok(Self::LPOP);
        }

        Err(anyhow!("Unknown command {:}", command))
    }
}

pub struct RedisVM {
    db: RedisDB,
}

impl RedisVM {
    pub fn new() -> Self {
        RedisVM { db: RedisDB::new() }
    }

    pub fn handle(&mut self, request: RESPData) -> Result<RESPData> {
        self.handle_request(request)
    }

    fn handle_request(&mut self, request: RESPData) -> Result<RESPData> {
        match request {
            RESPData::Aggregate(Aggregate::Array(Some(array))) => self.handle_request_array(array),
            RESPData::Simple(s) => self.handle_request_simple(s),
            _ => todo!(),
        }
    }

    fn handle_request_simple(&self, request: Simple) -> Result<RESPData> {
        match request {
            Simple::String(ref command) => match Builtin::new(command)? {
                Builtin::PING => Ok(RESPData::Simple(Simple::String("PONG".to_string()))),
                _ => Ok(RESPData::bulk_error(format!(
                    "Uknown simple command {:?}",
                    request
                ))),
            },
            _ => Err(anyhow!("Unknown command {:?}", request)),
        }
    }

    fn handle_request_array(&mut self, mut array: Vec<RESPData>) -> Result<RESPData> {
        Ok(match array[0] {
            RESPData::Simple(Simple::String(ref command)) => match Builtin::new(command)? {
                Builtin::PING => array.remove(1),
                _ => Err(anyhow!("{} Is not simple command", command))?,
            },
            RESPData::Aggregate(Aggregate::BulkString(ref command)) => match command {
                Some(c) => match Builtin::new(str::from_utf8(c)?)? {
                    Builtin::ECHO => array.remove(1),
                    Builtin::PING => RESPData::Simple(Simple::String("PONG".to_string())),
                    Builtin::SET => {
                        let mut expiry_args: Option<(&RESPData, &RESPData)> = None;

                        if array.len() > 3 {
                            expiry_args = Some((&array[3], &array[4]));
                        }

                        self.db
                            .insert(array[1].clone(), array[2].clone(), expiry_args)?;
                        RESPData::ok()
                    }
                    Builtin::GET => self.db.get(&array[1]).clone(),
                    Builtin::RPUSH => RESPData::from({
                        println!("pushing");
                        if array.len() == 3 {
                            self.db.push(&array[1], array[2].clone())
                        } else {
                            self.db.push_many(
                                &array[1],
                                array[2..].iter().map(|v| v.clone()).collect(),
                            )
                        }
                    }),
                    Builtin::LPUSH => RESPData::from({
                        println!("pushing");
                        if array.len() == 3 {
                            self.db.lpush(&array[1], array[2].clone())
                        } else {
                            self.db.lpush_many(
                                &array[1],
                                array[2..].iter().map(|v| v.clone()).collect(),
                            )
                        }
                    }),
                    Builtin::LRANGE => self.db.list_range(&array[1], &array[2], &array[3])?,
                    Builtin::LLEN => self.db.list_len(&array[1]),
                    Builtin::LPOP => {
                        if array.len() == 2 {
                            self.db.list_pop(&array[1])
                        } else {
                            self.db
                                .list_pop_many(&array[1], array[2].try_bulk_string_to_int()? as u64)
                        }
                    }
                    // Builtin::LRANGE => Some(
                    //     self.db
                    //         .borrow()
                    //         .list_range(&array[1], &array[2], &array[3])?,
                    // ),
                },
                None => Err(anyhow!("Expected command"))?,
            },
            _ => todo!(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_request_response(request: &str, response: &str) {
        let output = RedisVM::new()
            .handle(Parser::new().parse(request).unwrap().pop().unwrap())
            .unwrap();

        if output.serialize() != response {
            eprintln!("Expected: {}\nGot     : {}", response, output.serialize());
            assert!(false);
        }
    }

    fn test_request_response_vm(request: &str, response: &str, vm: &mut RedisVM) {
        let output = vm
            .handle(Parser::new().parse(request).unwrap().pop().unwrap())
            .unwrap();

        if output.serialize() != response {
            eprintln!("Expected: {}\nGot     : {}", response, output.serialize());
            assert!(false);
        }
    }

    #[test]
    fn test_echo_command() {
        test_request_response("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n", "$3\r\nhey\r\n");
    }

    #[test]
    fn test_ping_command() {
        test_request_response("+PING\r\n", "+PONG\r\n");
    }

    #[test]
    fn test_set_command() {
        let mut vm = RedisVM::new();

        test_request_response_vm(
            "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
            "+OK\r\n",
            &mut vm,
        );
    }

    #[test]
    fn test_get_command() {
        let mut vm = RedisVM::new();

        test_request_response_vm("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n", "$-1\r\n", &mut vm);
        test_request_response_vm(
            "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
            "+OK\r\n",
            &mut vm,
        );
        test_request_response_vm("*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n", "$3\r\nbar\r\n", &mut vm);
    }

    #[test]
    fn test_push_command() {
        let mut vm = RedisVM::new();

        test_request_response_vm(
            "*3\r\n$5\r\nRPUSH\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
            ":1\r\n",
            &mut vm,
        );
        test_request_response_vm(
            "*3\r\n$5\r\nRPUSH\r\n$3\r\nfoo\r\n$3\r\nhey\r\n",
            ":2\r\n",
            &mut vm,
        );
    }
}
