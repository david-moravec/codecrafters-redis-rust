use crate::frame::{Frame, ToFrame};
use bytes::Bytes;
use std::fmt::Display;

enum Role {
    Master,
    Slave(String),
}

impl Display for Role {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Master => write!(f, "master"),
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
            None => Role::Master,
        };

        Self { role }
    }
}

impl ToFrame for Replication {
    fn to_frame(&self) -> Frame {
        Frame::BulkString(Bytes::from(format!("role:{}", self.role)))
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
