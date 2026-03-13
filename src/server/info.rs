use crate::frame::{Frame, ToFrame};
use bytes::Bytes;
use std::fmt::Display;

enum Role {
    Master { repl_id: String, repl_offset: usize },
    Slave(String),
}

impl Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Master { .. } => write!(f, "master"),
            Self::Slave(_) => write!(f, "slave"),
        }
    }
}

pub struct Replication {
    role: Role,
}

impl Replication {
    fn new(replica_of: Option<String>) -> Self {
        let role = match replica_of {
            Some(s) => Role::Slave(s),
            None => Role::Master {
                repl_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
                repl_offset: 0,
            },
        };

        Self { role }
    }
}

impl ToFrame for Replication {
    fn to_frame(&self) -> Frame {
        match self.role {
            Role::Slave(_) => Frame::BulkString(Bytes::from(format!("role:{}", self.role))),
            Role::Master {
                ref repl_id,
                ref repl_offset,
            } => Frame::BulkString(Bytes::from(format!(
                "role:{}\nmaster_replid:{}\nmaster_repl_offset:{:}",
                self.role, repl_id, repl_offset
            ))),
        }
    }
}

pub struct ServerInfo {
    pub replication: Replication,
}

impl ServerInfo {
    pub fn new(replica_of: Option<String>) -> Self {
        let replication = Replication::new(replica_of);

        Self { replication }
    }
}
