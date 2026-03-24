use crate::parser::Parse;

#[derive(Debug)]
pub struct Wait {
    pub replica_count: u64,
    pub timeout: u64,
}

impl Wait {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let replica_count = parse.next_u64()?;
        let timeout = parse.next_u64()?;
        Ok(Wait {
            replica_count,
            timeout,
        })
    }
}
