use crate::redis_parser::{Aggregate, Parser, RESPData, Simple};
use std::cell::RefCell;
use std::io::{Read, Write};

use anyhow::{Result, anyhow};

enum Builtin {
    ECHO,
    PING,
}

impl Builtin {
    fn new(command: &str) -> Result<Self> {
        if command == "ECHO" {
            return Ok(Self::ECHO);
        } else if command == "PING" {
            return Ok(Self::PING);
        }

        Err(anyhow!("Unknown command {:}", command))
    }
}

pub struct RedisVM {
    parser: Parser,
    output: RefCell<Vec<String>>,
}

impl RedisVM {
    pub fn new() -> Self {
        RedisVM {
            parser: Parser::new(),
            output: RefCell::new(vec![]),
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
                Builtin::ECHO => Err(anyhow!("ECHO must always come with second arg"))?,
                Builtin::PING => self.to_output(format!("+PONG\r\n")),
            },
            _ => todo!(),
        }

        Ok(())
    }

    fn handle_request_array(&self, array: &[RESPData]) -> Result<()> {
        match array[0] {
            RESPData::Simple(Simple::String(ref command)) => match Builtin::new(command)? {
                Builtin::ECHO => self.output_data(&array[1]),
                Builtin::PING => self.to_output(format!("+PONG\r\n")),
            },
            RESPData::Aggregate(Aggregate::BulkString(ref command)) => match command {
                Some(c) => match Builtin::new(str::from_utf8(c)?)? {
                    Builtin::ECHO => self.output_data(&array[1]),
                    Builtin::PING => self.to_output(format!("+PONG\r\n")),
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

    fn output_data(&self, data: &RESPData) {
        self.to_output(data.serialize());
    }

    pub fn flush_output<S: Write>(&self, stream: &mut S) -> Result<usize> {
        let mut written = 0;

        for s in self.output.borrow().iter() {
            written += stream.write(s.as_bytes())?
        }

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

    #[test]
    fn test_echo_command() {
        test_request_response("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n", "$3\r\nhey\r\n");
    }

    #[test]
    fn test_ping_command() {
        test_request_response("+PING\r\n", "+PONG\r\n");
    }
}
