use anyhow::{Result, anyhow};
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::{
    frame::Frame,
    server::info::{HandleInquiry, ServerInfo},
};

type SubscriptionMessage = String;

pub struct WaitInquiry {
    pub count: u64,
    pub timeout: Duration,
    pub response: oneshot::Sender<u64>,
}

impl WaitInquiry {
    async fn apply(
        self,
        handle_inquiry_tx: &mut broadcast::Sender<HandleInquiry>,
        info: ServerInfo,
    ) -> Result<()> {
        let replica_count = info.replica_count()?;

        if replica_count == 0 {
            if let Err(_) = self.response.send(0) {};
            Ok(())
        } else if info.offset()? == 0 {
            if let Err(_) = self.response.send(replica_count) {};
            Ok(())
        } else {
            let (tx, mut rx) = mpsc::channel(100);
            let server_cmd = HandleInquiry {
                cmd: Frame::bulk_strings_array_from_str(vec!["REPLCONF", "GETACK", "*"]),
                response_channel: tx.clone(),
                timeout: self.timeout,
            };

            let _ = handle_inquiry_tx.send(server_cmd).map_err(|e| {
                anyhow!(
                    "during sending cmd to replia conneciton following error occured; {:}",
                    e
                )
            })?;

            let mut hit_count = 0;
            let deadline = Instant::now() + self.timeout;

            loop {
                if hit_count == self.count {
                    break;
                }
                let remaining = deadline.saturating_duration_since(Instant::now());

                if remaining.is_zero() {
                    break;
                }

                match tokio::time::timeout(remaining, rx.recv()).await {
                    Ok(_) => {
                        hit_count += 1;
                    }
                    Err(_) => break,
                };
            }

            if let Err(_) = self.response.send(hit_count) {};
            Ok(())
        }
    }
}

struct Subscribe {
    channel_name: String,
    response: oneshot::Sender<broadcast::Receiver<SubscriptionMessage>>,
}

impl Subscribe {
    fn apply(self) -> Result<()> {
        Ok(())
    }
}

pub(crate) enum ServerInquiry {
    Wait(WaitInquiry),
    Subscribe(Subscribe),
}

impl ServerInquiry {
    pub async fn apply(
        self,
        handle_inquiry_tx: &mut broadcast::Sender<HandleInquiry>,
        server_info: ServerInfo,
    ) -> Result<()> {
        match self {
            Self::Wait(cmd) => cmd.apply(handle_inquiry_tx, server_info).await,
            Self::Subscribe(cmd) => cmd.apply(),
        }
    }
}
