use crate::frame::{Frame, ToFrame};
use anyhow::{Result, anyhow};
use bytes::Bytes;
use std::fmt::Display;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub(crate) enum Role {
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

#[derive(Debug)]
pub struct ReplicationInfo {
    pub(crate) role: Role,
    pub(crate) count: Mutex<u64>,
}

impl ReplicationInfo {
    fn new(replica_of: Option<String>) -> Self {
        let role = match replica_of {
            Some(s) => {
                let mut split = s.split_whitespace();
                let mut host = split.next().unwrap();
                let port = split.next().unwrap();

                if host == "localhost" {
                    host = "127.0.0.1"
                }

                Role::Slave(format!("{}:{}", host, port))
            }
            None => Role::Master {
                repl_id: "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string(),
                repl_offset: 0,
            },
        };

        Self {
            role,
            count: Mutex::new(0),
        }
    }
}

impl ToFrame for ReplicationInfo {
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

#[derive(Debug, Clone)]
struct Shared {
    replication: Arc<ReplicationInfo>,
}

impl Shared {
    fn new(master_address: Option<String>) -> Self {
        Shared {
            replication: Arc::new(ReplicationInfo::new(master_address)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ServerInfo {
    shared: Shared,
}

impl ServerInfo {
    pub fn new(master_address: Option<String>) -> Self {
        Self {
            shared: Shared::new(master_address),
        }
    }

    pub(crate) fn replication_role(&self) -> &Role {
        &self.shared.replication.role
    }

    pub(crate) fn increment_replica_count(&self) -> Result<()> {
        if let Role::Master { .. } = self.shared.replication.role {
            Ok(*self.shared.replication.count.lock().unwrap() += 1)
        } else {
            Err(anyhow!("slaves cannot have replicas"))
        }
    }

    pub(crate) fn replica_count(&self) -> Result<u64> {
        if let Role::Master { .. } = self.shared.replication.role {
            Ok(*self.shared.replication.count.lock().unwrap())
        } else {
            Err(anyhow!("slaves cannot have replicas"))
        }
    }
}

impl ToFrame for ServerInfo {
    fn to_frame(&self) -> Frame {
        self.shared.replication.to_frame()
    }
}
