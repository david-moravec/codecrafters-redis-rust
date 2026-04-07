use anyhow::Result;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::StreamMap;
use tokio_stream::wrappers::BroadcastStream;

use crate::{
    cmd::server_inquiry::{PublishInquiry, ServerInquiry},
    frame::Frame,
    parser::Parse,
    server::subscription_channels::SubscriptionMessage,
};

#[derive(Debug)]
pub struct Publish {
    pub channel_name: String,
    pub message: String,
}

impl Publish {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let channel_name = parse.next_string()?;
        let message = parse.next_string()?;
        Ok(Publish {
            channel_name,
            message,
        })
    }

    pub async fn apply(self, query_tx: &mut mpsc::Sender<ServerInquiry>) -> Result<Frame> {
        let (tx, rx) = oneshot::channel();
        eprintln!("sending query");
        query_tx.send(self.server_inquiry(tx)).await?;
        let subscriber_count = rx.await?;
        eprintln!("recieved response to qury");
        let frame = Frame::Integer(subscriber_count);

        Ok(frame)
    }

    fn server_inquiry(&self, tx: oneshot::Sender<u64>) -> ServerInquiry {
        ServerInquiry::Publish(PublishInquiry {
            channel_name: self.channel_name.clone(),
            message: self.message.clone(),
            response: tx,
        })
    }
}
