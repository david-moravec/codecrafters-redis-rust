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
            _ => todo!(),
        }
    }

    fn handle_request_array(&self, array: &[RESPData]) -> Result<()> {
        match array[0] {
            RESPData::Simple(Simple::String(ref command)) => match Builtin::new(command)? {
                Builtin::ECHO => self.output_data(&array[1]),
                Builtin::PING => self.to_output(format!("+PONG\t\n")),
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
