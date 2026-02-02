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

impl From<u64> for Int {
    fn from(value: u64) -> Self {
        Self {
            sign: Sign::Plus,
            value,
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

impl From<f64> for Double {
    fn from(value: f64) -> Self {
        let s = format!("{}", value);
        let parts: Vec<&str> = s.split('.').collect();

        let integer_part = parts[0].parse::<u64>().unwrap_or(0);
        let fractional_part = if parts.len() > 1 {
            parts[1].parse::<u64>().unwrap_or(0)
        } else {
            0
        };

        Self {
            sign: { if value < 0.0 { Sign::Minus } else { Sign::Plus } },
            integral: integer_part,
            fractional: fractional_part,
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

impl Simple {
    fn error(string: &str) -> Self {
        Self::Error(string.to_string())
    }

    fn string(string: &str) -> Self {
        Self::String(string.to_string())
    }
}

impl From<bool> for Simple {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<u64> for Simple {
    fn from(value: u64) -> Self {
        Self::Integer(Int::from(value))
    }
}

impl From<f64> for Simple {
    fn from(value: f64) -> Self {
        Self::Double(Double::from(value))
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Aggregate {
    BulkString(Option<Vec<u8>>),
    BulkError(Vec<u8>),
    VerbatimString(Encoding, Vec<u8>),
    Array(Vec<RESPData>),
    Map(RESPMap),
    Attribute(RESPMap),
    Set(HashSet<RESPData>),
    Push(Vec<RESPData>),
}

impl Aggregate {
    fn bulk_string(string: Option<&str>) -> Self {
        Aggregate::BulkString(string.map(|s| s.as_bytes().iter().map(|c| *c).collect()))
    }

    fn verbatim_string(encoding: Encoding, string: &str) -> Self {
        Aggregate::VerbatimString(encoding, string.as_bytes().iter().map(|c| *c).collect())
    }
}

impl Hash for Aggregate {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Self::Map(_) => panic!("Cannot hash Map"),
            Self::Attribute(_) => panic!("Cannot hash Attribute"),
            Self::Set(_) => panic!("Cannot hash Set"),
            Self::BulkString(d) => d.hash(state),
            Self::BulkError(d) => d.hash(state),
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

pub struct Parser {
    buff: Vec<char>,
    current: usize,
}

impl Parser {
    pub fn new() -> Self {
        Parser {
            buff: vec![],
            current: 0,
        }
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

        let result = match data_symbol {
            '+' => Simple::String(String::from_iter(type_data.iter())),
            '-' => Simple::Error(String::from_iter(type_data.iter())),
            ':' => Simple::Integer(Int::new(type_data)),
            '_' => Simple::Null,
            '#' => Simple::Bool({
                if type_data[0] == 't' {
                    true
                } else if type_data[0] == 'f' {
                    false
                } else {
                    return Err(anyhow!("Expected bool value (<t|f>) got {:}", type_data[0]));
                }
            }),
            ',' => Simple::Double(Double::new(type_data)),
            '(' => todo!(),
            c => Err(anyhow!("Unknown data symbol '{:}'", c))?,
        };

        self.consume_CRLF()?;

        Ok(result)
    }

    fn aggregate(&mut self) -> Result<Aggregate> {
        let data_symbol = self.advance()?;
        let length = self.get_data_length()?;

        match data_symbol {
            '$' => {
                let inner_string = {
                    if length == u64::MAX {
                        None
                    } else {
                        let orig_pos = self.current;
                        self.advance_till_crlf_hit()?;
                        let type_data = self.buff[orig_pos..self.current]
                            .iter()
                            .collect::<String>()
                            .into_bytes();

                        self.consume_CRLF()?;

                        Some(type_data)
                    }
                };
                Ok(Aggregate::BulkString(inner_string))
            }
            '*' => {
                let mut data: Vec<RESPData> = vec![];

                for _ in 0..length {
                    data.push(self.next_resp_data()?)
                }

                Ok(Aggregate::Array(data))
            }
            '!' => {
                let orig_pos = self.current;
                self.advance_till_crlf_hit()?;
                let type_data = self.buff[orig_pos..self.current]
                    .iter()
                    .collect::<String>()
                    .into_bytes();

                self.consume_CRLF()?;

                Ok(Aggregate::BulkError(type_data))
            }
            '=' => {
                let orig_pos = self.current;
                self.advance_till_crlf_hit()?;
                let type_data = &self.buff[orig_pos..self.current];

                let mut split_at_colon = type_data.split(|c| *c == ':');

                let res = Ok(Aggregate::VerbatimString(
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
                ));
                self.consume_CRLF()?;

                res
            }
            '%' => {
                let mut data: RESPMap = HashMap::new();

                for _ in 0..length {
                    data.insert(self.next_resp_data()?, self.next_resp_data()?);
                }

                Ok(Aggregate::Map(data))
            }
            '|' => {
                let mut data: RESPMap = HashMap::new();

                for _ in 0..length {
                    data.insert(self.next_resp_data()?, self.next_resp_data()?);
                }

                Ok(Aggregate::Attribute(data))
            }
            '~' => {
                let mut data = HashSet::new();
                for _ in 0..length {
                    data.insert(self.next_resp_data()?);
                }

                Ok(Aggregate::Set(data))
            }
            '>' => {
                let mut data: Vec<RESPData> = vec![];

                for _ in 0..length {
                    data.push(self.next_resp_data()?)
                }

                Ok(Aggregate::Push(data))
            }
            c => Err(anyhow!("Unknown data symbol '{:}'", c)),
        }
    }

    fn get_data_length(&mut self) -> Result<u64> {
        let orig_pos = self.current;
        self.advance_till_crlf_hit()?;
        let lenght_str = String::from_iter(self.buff[orig_pos..self.current].iter());
        self.consume_CRLF()?;

        if lenght_str == "-1" {
            return Ok(u64::MAX);
        }

        let length = lenght_str.parse::<u64>()?;

        Ok(length)
    }

    pub fn next_resp_data(&mut self) -> Result<RESPData> {
        let result = match self.peek()? {
            '$' | '*' | '!' | '=' | '%' | '|' | '~' | '>' => RESPData::Aggregate(self.aggregate()?),
            '+' | '-' | ':' | '_' | '#' | ',' | '(' => RESPData::Simple(self.simple()?),
            c => Err(anyhow!("Unknown data symbol '{:}'", c))?,
        };

        Ok(result)
    }

    pub fn parse(&mut self, buff: &str) -> Result<Vec<RESPData>> {
        let mut result = vec![];

        self.buff = buff.chars().collect();
        self.current = 0;

        while !self.is_at_end() {
            result.push(self.next_resp_data()?);
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

        let parsed = Parser::new().parse(s).unwrap();

        assert!(parsed[0] == RESPData::Simple(Simple::string("OK")));
        assert!(parsed[1] == RESPData::Simple(Simple::error("Error message")));
        assert!(parsed[2] == RESPData::Simple(Simple::from(1000)));
        assert!(parsed[3] == RESPData::Simple(Simple::Null));
        assert!(parsed[4] == RESPData::Simple(Simple::from(true)));
        println!("{:?}", parsed[5]);
        println!("{:?}", RESPData::Simple(Simple::from(1.23)));
        assert!(parsed[5] == RESPData::Simple(Simple::from(1.23)));
    }

    #[test]
    fn test_parse_aggregate_bulk_string() {
        let s = "$5\r\nhello\r\n$0\r\n\r\n$-1\r\n";

        let parsed = Parser::new().parse(s).unwrap();

        assert!(parsed[0] == RESPData::Aggregate(Aggregate::bulk_string(Some("hello"))));
        assert!(parsed[1] == RESPData::Aggregate(Aggregate::bulk_string(Some(""))));
        assert!(parsed[2] == RESPData::Aggregate(Aggregate::BulkString(None)));
    }

    #[test]
    fn test_parse_aggregate_array() {
        let s = "*5\r\n:1\r\n:2\r\n:3\r\n:4\r\n$5\r\nhello\r\n";

        let parsed = Parser::new().parse(s).unwrap();

        if let RESPData::Aggregate(Aggregate::Array(array)) = &parsed[0] {
            assert!(array[0] == RESPData::Simple(Simple::from(1)));
            assert!(array[1] == RESPData::Simple(Simple::from(2)));
            assert!(array[2] == RESPData::Simple(Simple::from(3)));
            assert!(array[3] == RESPData::Simple(Simple::from(4)));
            assert!(array[4] == RESPData::Aggregate(Aggregate::bulk_string(Some("hello"))));
        } else {
            assert!(false);
        }

        let s = "*2\r\n*3\r\n:1\r\n:2\r\n:3\r\n*2\r\n+Hello\r\n-World\r\n";

        let parsed = Parser::new().parse(s).unwrap();

        if let RESPData::Aggregate(Aggregate::Array(array)) = &parsed[0] {
            assert!(matches!(array[0], RESPData::Aggregate(Aggregate::Array(_))));
        } else {
            assert!(false);
        }
    }

    #[test]
    fn test_parse_verbatim_string() {
        let s = "=15\r\ntxt:Some string\r\n";

        let parsed = Parser::new().parse(s).unwrap();

        assert!(
            parsed[0]
                == RESPData::Aggregate(Aggregate::verbatim_string(Encoding::TXT, "Some string"))
        );
    }

    #[test]
    fn test_parse_aggregate_map() {
        let s = "%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n";

        let parsed = Parser::new().parse(s).unwrap();

        if let RESPData::Aggregate(Aggregate::Map(map)) = &parsed[0] {
            assert!(
                map.get(&RESPData::Simple(Simple::string("first")))
                    == Some(&RESPData::Simple(Simple::Integer(Int::from(1))))
            );
            assert!(
                map.get(&RESPData::Simple(Simple::string("second")))
                    == Some(&RESPData::Simple(Simple::Integer(Int::from(2))))
            );
        } else {
            assert!(false);
        }
    }
}
