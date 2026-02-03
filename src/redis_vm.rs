use std::cell::RefCell;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::{Read, Write};

use anyhow::{Result, anyhow};

use crate::redis_parser::{Aggregate, NULL_STRING, Parser, RESPData, RESPMap, Simple};

enum Builtin {
    ECHO,
    PING,
    GET,
    SET,
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
        }

        Err(anyhow!("Unknown command {:}", command))
    }
}

pub struct RedisVM {
    parser: Parser,
    output: RefCell<Vec<String>>,
    map: RefCell<RESPMap>,
}

impl RedisVM {
    pub fn new() -> Self {
        RedisVM {
            parser: Parser::new(),
            output: RefCell::new(vec![]),
            map: RefCell::new(RESPMap::new()),
        }
    }

    pub fn handle(&mut self, request_buff: &str) -> Result<()> {
        for request_parsed in self.parser.parse(request_buff)?.iter() {
            self.handle_request(request_parsed)?;
        }

        Ok(())
    }

    fn handle_request(&self, request: &RESPData) -> Result<()> {
        match request {
            RESPData::Aggregate(Aggregate::Array(array)) => self.handle_request_array(array),
            RESPData::Simple(s) => self.handle_request_simple(s),
            _ => todo!(),
        }
    }

    fn handle_request_simple(&self, request: &Simple) -> Result<()> {
        match request {
            Simple::String(command) => match Builtin::new(command)? {
                Builtin::PING => self.to_output(format!("+PONG\r\n")),
                _ => Err(anyhow!("{} Is not simple command", command))?,
            },
            _ => todo!(),
        }

        Ok(())
    }

    fn handle_request_array(&self, array: &[RESPData]) -> Result<()> {
        match array[0] {
            RESPData::Simple(Simple::String(ref command)) => match Builtin::new(command)? {
                Builtin::PING => self.to_output(array[1].serialize()),
                _ => Err(anyhow!("{} Is not simple command", command))?,
            },
            RESPData::Aggregate(Aggregate::BulkString(ref command)) => match command {
                Some(c) => match Builtin::new(str::from_utf8(c)?)? {
                    Builtin::ECHO => self.output_data(&array[1]),
                    Builtin::PING => self.to_output(format!("+PONG\r\n")),
                    Builtin::SET => {
                        self.map
                            .borrow_mut()
                            .insert(array[1].clone(), array[2].clone());
                        self.output_ok();
                    }
                    Builtin::GET => {
                        self.output_data_opt(self.map.borrow().get(&array[1]));
                    }
                },
                None => {}
            },
            _ => todo!(),
        }

        Ok(())
    }

    fn to_output(&self, s: String) {
        self.output.borrow_mut().push(s);
    }

    fn output_ok(&self) {
        self.to_output(Simple::ok().serialize());
    }

    fn output_data_opt(&self, data: Option<&RESPData>) {
        match data {
            Some(d) => self.output_data(d),
            None => self.output_data(&NULL_STRING),
        }
    }

    fn output_data(&self, data: &RESPData) {
        self.to_output(data.serialize());
    }

    pub fn flush_output<S: Write>(&self, stream: &mut S) -> Result<usize> {
        let mut written = 0;

        for s in self.output.borrow().iter() {
            written += stream.write(s.as_bytes())?
        }

        self.output.borrow_mut().clear();

        Ok(written)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn redis_vm_handle(request: &str) -> RedisVM {
        let mut vm = RedisVM::new();
        vm.handle(request).unwrap();
        vm
    }

    fn test_request_response(request: &str, response: &str) {
        let vm = redis_vm_handle(request);
        if vm.output.borrow()[0] != response {
            eprintln!(
                "Expected: {}\nGot     : {}",
                response,
                vm.output.borrow()[0]
            );
            assert!(false);
        }
    }

    fn test_request_response_vm(request: &str, response: &str, vm: &mut RedisVM) {
        vm.handle(request).unwrap();

        if vm.output.borrow()[0] != response {
            eprintln!(
                "Expected: {}\nGot     : {}",
                response,
                vm.output.borrow()[0]
            );
            assert!(false);
        }

        vm.output.borrow_mut().clear();
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
}
