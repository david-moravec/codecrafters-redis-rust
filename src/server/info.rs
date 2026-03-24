use crate::frame::{Frame, ToFrame};
use anyhow::{Result, anyhow};
use bytes::Bytes;
use std::collections::HashMap;
use std::fmt::Display;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, mpsc};

#[derive(Debug)]
pub(crate) enum Role {
    Master { repl_id: String },
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
    pub(crate) repl_offset: Mutex<usize>,
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
            },
        };

        Self {
            role,
            count: Mutex::new(0),
            repl_offset: Mutex::new(0),
        }
    }
}

impl ToFrame for ReplicationInfo {
    fn to_frame(&self) -> Frame {
        match self.role {
            Role::Slave(_) => Frame::BulkString(Bytes::from(format!("role:{}", self.role))),
            Role::Master { ref repl_id } => Frame::BulkString(Bytes::from(format!(
                "role:{}\nmaster_replid:{}\nmaster_repl_offset:{:}",
                self.role,
                repl_id,
                *self.repl_offset.lock().unwrap()
            ))),
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct ServerCommand {
    pub(super) cmd: Frame,
    pub(super) response_channel: mpsc::Sender<Frame>,
}

#[derive(Debug, Clone)]
struct Shared {
    replication: Arc<ReplicationInfo>,
    server_broadcast: Arc<broadcast::Sender<ServerCommand>>,
}

impl Shared {
    fn new(master_address: Option<String>) -> Self {
        let (tx, _) = broadcast::channel(16);

        Shared {
            replication: Arc::new(ReplicationInfo::new(master_address)),
            server_broadcast: Arc::new(tx),
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

    pub(super) fn server_broadcast_subscribe(&self) -> broadcast::Receiver<ServerCommand> {
        self.shared.server_broadcast.subscribe()
    }

    pub(super) fn incement_offset(&self, offset_incr: usize) -> Result<()> {
        if let Role::Master { .. } = self.shared.replication.role {
            let mut offset = self.shared.replication.repl_offset.lock().unwrap();
            *offset += offset_incr;
            Ok(())
        } else {
            Err(anyhow!("slaves cannot have replicas"))
        }
    }

    pub(crate) fn offset(&self) -> Result<usize> {
        if let Role::Master { .. } = self.shared.replication.role {
            let offset = self.shared.replication.repl_offset.lock().unwrap();
            Ok(*offset)
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
