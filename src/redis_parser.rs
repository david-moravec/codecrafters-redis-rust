use anyhow::{Result, anyhow};
use std::{
    collections::{HashMap, HashSet, btree_map::Range},
    hash::Hash,
};

#[derive(Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct BigInt {}

#[derive(Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum Sign {
    Plus,
    Minus,
}

impl Sign {
    pub fn new(c: char) -> Option<Self> {
        if c == '-' {
            Some(Self::Minus)
        } else if c == '+' {
            Some(Self::Plus)
        } else {
            None
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct Int {
    sign: Sign,
    value: u64,
}

impl Int {
    fn new(chars: &[char]) -> Self {
        let range: std::ops::RangeFrom<usize>;

        let s = match Sign::new(chars[0]) {
            Some(s) => {
                range = 1..;
                s
            }
            None => {
                range = 0..;
                Sign::Plus
            }
        };

        Self {
            sign: s,
            value: chars[range]
                .iter()
                .collect::<String>()
                .parse::<u64>()
                .expect(&format!("Expected number got {:?}", chars)),
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct Double {
    sign: Sign,
    integral: u64,
    fractional: u64,
}

impl Double {
    fn new(chars: &[char]) -> Self {
        let range: std::ops::RangeFrom<usize>;
        let mut split_at_dot = chars.split(|c| *c == '.');
        let integral_chars = split_at_dot.next().expect(&format!(
            "Expected double ([<+|->]<integral>[.<fractional>][<E|e>[sign]<exponent>]), got {:?}",
            chars
        ));

        let s = match Sign::new(integral_chars[0]) {
            Some(s) => {
                range = 1..;
                s
            }
            None => {
                range = 0..;
                Sign::Plus
            }
        };

        Self {
            sign: s,
            integral: integral_chars[range]
                .iter()
                .collect::<String>()
                .parse::<u64>()
                .expect(&format!("Expected number got {:?}", chars)),
            fractional: {
                match split_at_dot.next() {
                    Some(fractinal_chars) => fractinal_chars[0..]
                        .iter()
                        .collect::<String>()
                        .parse::<u64>()
                        .expect(&format!("Expected number got {:?}", chars)),
                    None => 0,
                }
            },
        }
    }
}

#[derive(Debug, Hash, PartialEq, PartialOrd, Eq)]
enum Encoding {
    TXT,
}

impl Encoding {
    pub fn new(encoding: &[char]) -> Result<Self> {
        if encoding.iter().zip("txt".chars()).all(|(c1, c2)| *c1 == c2) {
            Ok(Self::TXT)
        } else {
            Err(anyhow!("Unknown encoding {:?}", encoding))
        }
    }
}

type RESPMap = HashMap<RESPData, RESPData>;

#[derive(Hash, Debug, PartialEq, PartialOrd, Eq)]
enum Simple {
    String(String),
    Error(String),
    Integer(Int),
    Null,
    Bool(bool),
    Double(Double),
    BigNumber(BigInt),
}

#[derive(Debug, PartialEq, Eq)]
enum Aggregate {
    BulkString(Vec<u8>),
    BulkError(Vec<u8>),
    VerbatimString(Encoding, Vec<u8>),
    Array(Vec<RESPData>),
    Map(RESPMap),
    Attribute(RESPMap),
    Set(HashSet<RESPData>),
    Push(Vec<RESPData>),
}

impl Hash for Aggregate {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Self::Map(_) => panic!("Cannot hash Map"),
            Self::Attribute(_) => panic!("Cannot hash Attribute"),
            Self::Set(_) => panic!("Cannot hash Set"),
            Self::BulkString(d) | Self::BulkError(d) => d.hash(state),
            Self::Array(d) | Self::Push(d) => d.hash(state),
            Self::VerbatimString(encoding, data) => {
                encoding.hash(state);
                data.hash(state);
            }
        }
    }
}

#[derive(Hash, Debug, PartialEq, Eq)]
enum RESPData {
    Simple(Simple),
    Aggregate(Aggregate),
}

struct Parser {
    buff: Vec<char>,
    current: usize,
}

impl Parser {
    fn new(buff: Vec<char>) -> Self {
        Parser { buff, current: 0 }
    }

    fn peek(&self) -> Result<char> {
        self.buff
            .get(self.current)
            .map(|c| *c)
            .ok_or(anyhow!("End of file reached"))
    }

    fn peek_next(&self) -> Result<char> {
        self.buff
            .get(self.current + 1)
            .map(|c| *c)
            .ok_or(anyhow!("End of file reached"))
    }

    fn is_at_end(&self) -> bool {
        self.peek().is_err()
    }

    fn advance(&mut self) -> Result<char> {
        self.current += 1;
        self.buff
            .get(self.current - 1)
            .map(|c| *c)
            .ok_or(anyhow!("End of buff reached."))
    }

    fn consume_CRLF(&mut self) -> Result<()> {
        if self.is_at_crlf() {
            self.advance()?;
            self.advance()?;
            Ok(())
        } else {
            Err(anyhow!(
                "Expected CRLF got {:?}{:?}",
                self.peek(),
                self.peek_next()
            ))
        }
    }

    fn is_at_crlf(&self) -> bool {
        self.peek().is_ok_and(|c| c as u32 == 13) && self.peek_next().is_ok_and(|c| c as u32 == 10)
    }

    fn advance_till_crlf_hit(&mut self) -> Result<()> {
        while !self.is_at_end() && !self.is_at_crlf() {
            self.advance();
        }

        Ok(())
    }

    fn advance_until(&mut self, c: char) -> Result<()> {
        loop {
            if self.advance()? == c {
                break;
            }
        }

        Ok(())
    }

    fn simple(&mut self) -> Result<Simple> {
        let data_symbol = self.advance()?;
        let orig_pos = self.current;
        self.advance_till_crlf_hit()?;

        let type_data = &self.buff[orig_pos..self.current];

        match data_symbol {
            '+' => Ok(Simple::String(String::from_iter(type_data.iter()))),
            '-' => Ok(Simple::Error(String::from_iter(type_data.iter()))),
            ':' => Ok(Simple::Integer(Int::new(type_data))),
            '_' => Ok(Simple::Null),
            '#' => Ok(Simple::Bool({
                if type_data[0] == 't' {
                    Ok(true)
                } else if type_data[0] == 'f' {
                    Ok(false)
                } else {
                    Err(anyhow!("Expected bool value (<t|f>) got {:}", type_data[0]))
                }
            }?)),
            ',' => Ok(Simple::Double(Double::new(type_data))),
            '(' => todo!(),
            c => Err(anyhow!("Unknown data symbol '{:}'", c)),
        }
    }

    fn aggregate(&mut self) -> Result<Aggregate> {
        let data_symbol = self.advance()?;
        let length = self.get_data_length()?;
        let orig_pos = self.current;
        self.advance_till_crlf_hit()?;

        let type_data = &self.buff[orig_pos..self.current];

        match data_symbol {
            '$' => Ok(Aggregate::BulkString(
                type_data.iter().collect::<String>().into_bytes(),
            )),
            '*' => {
                let mut data: Vec<RESPData> = vec![];

                for _ in 0..length {
                    data.push(self.resp_data()?)
                }

                Ok(Aggregate::Array(data))
            }
            '!' => Ok(Aggregate::BulkError(
                type_data.iter().collect::<String>().into_bytes(),
            )),
            '=' => {
                let mut split_at_colon = type_data.split(|c| *c == ':');

                Ok(Aggregate::VerbatimString(
                    Encoding::new(
                        split_at_colon
                            .next()
                            .expect("Expected encoding for verbatim string"),
                    )?,
                    split_at_colon
                        .next()
                        .expect("Expected string data after encoding")
                        .iter()
                        .collect::<String>()
                        .into_bytes(),
                ))
            }
            '%' => {
                let mut data: RESPMap = HashMap::new();

                for _ in 0..length {
                    data.insert(self.resp_data()?, self.resp_data()?);
                }

                Ok(Aggregate::Map(data))
            }
            '|' => {
                let mut data: RESPMap = HashMap::new();

                for _ in 0..length {
                    data.insert(self.resp_data()?, self.resp_data()?);
                }

                Ok(Aggregate::Attribute(data))
            }
            '~' => {
                let mut data = HashSet::new();
                for _ in 0..length {
                    data.insert(self.resp_data()?);
                }

                Ok(Aggregate::Set(data))
            }
            '>' => {
                let mut data: Vec<RESPData> = vec![];

                for _ in 0..length {
                    data.push(self.resp_data()?)
                }

                Ok(Aggregate::Push(data))
            }
            c => Err(anyhow!("Unknown data symbol '{:}'", c)),
        }
    }

    fn get_data_length(&mut self) -> Result<u64> {
        let orig_pos = self.current;
        self.advance_till_crlf_hit()?;
        let length = String::from_iter(self.buff[orig_pos..self.current].iter()).parse::<u64>()?;
        self.consume_CRLF()?;

        Ok(length)
    }

    fn resp_data(&mut self) -> Result<RESPData> {
        match self.peek().unwrap() {
            '$' | '*' | '!' | '=' | '%' | '|' | '~' | '>' => {
                Ok(RESPData::Aggregate(self.aggregate()?))
            }

            '+' | '-' | ':' | '_' | '#' | ',' | '(' => Ok(RESPData::Simple(self.simple()?)),
            c => Err(anyhow!("Unknown data type  '{:}'", c)),
        }
    }

    pub fn parse(&mut self) -> Result<Vec<RESPData>> {
        let mut result = vec![];

        while !self.is_at_end() {
            result.push(self.resp_data()?);
            self.consume_CRLF()?;
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_parse_simple() {
        let s = "+OK\r\n-Error message\r\n:1000\r\n_\r\n#t\r\n,1.23\r\n";

        let parsed = Parser::new(s.chars().collect()).parse().unwrap();

        assert!(parsed[0] == RESPData::Simple(Simple::String(String::from_str("OK").unwrap())));
        assert!(
            parsed[1]
                == RESPData::Simple(Simple::Error(String::from_str("Error message").unwrap()))
        );
        assert!(
            parsed[2]
                == RESPData::Simple(Simple::Integer(Int {
                    sign: Sign::Plus,
                    value: 1000
                }))
        );
        assert!(parsed[3] == RESPData::Simple(Simple::Null));
        assert!(parsed[4] == RESPData::Simple(Simple::Bool(true)));
        assert!(
            parsed[5]
                == RESPData::Simple(Simple::Double(Double {
                    sign: Sign::Plus,
                    integral: 1,
                    fractional: 23
                }))
        );
    }
}
