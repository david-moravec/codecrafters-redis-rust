use std::time::Duration;

use anyhow::Result;
use tokio::sync::{mpsc, oneshot};

use crate::{
    cmd::server_inquiry::{ServerInquiry, WaitInquiry},
    frame::Frame,
    parser::Parse,
};

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

    pub async fn apply(self, query_tx: &mut mpsc::Sender<ServerInquiry>) -> Result<Frame> {
        let (tx, rx) = oneshot::channel();
        query_tx.send(self.server_inquiry(tx)).await?;
        let response = rx.await?;
        Ok(Frame::Integer(response))
    }

    fn server_inquiry(&self, tx: oneshot::Sender<u64>) -> ServerInquiry {
        ServerInquiry::Wait(WaitInquiry {
            count: self.replica_count,
            timeout: Duration::from_millis(self.timeout),
            response: tx,
        })
    }
}
