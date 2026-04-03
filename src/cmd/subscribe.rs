use anyhow::Result;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::StreamMap;
use tokio_stream::wrappers::BroadcastStream;

use crate::{
    cmd::server_inquiry::{ServerInquiry, SubscribeInquiry},
    frame::Frame,
    parser::Parse,
    server::subscription_channels::SubscriptionMessage,
};

#[derive(Debug)]
pub struct Subscribe {
    pub channel_name: String,
}

impl Subscribe {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let channel_name = parse.next_string()?;
        Ok(Subscribe { channel_name })
    }

    pub async fn apply(
        self,
        query_tx: &mut mpsc::Sender<ServerInquiry>,
        streams: &mut StreamMap<String, BroadcastStream<SubscriptionMessage>>,
    ) -> Result<Frame> {
        let (tx, rx) = oneshot::channel();
        eprintln!("sending query");
        query_tx.send(self.server_inquiry(tx)).await?;
        let subscription_rx = rx.await?;
        eprintln!("recieved response to qury");
        let broadcast_stream = BroadcastStream::new(subscription_rx);
        let mut frame = Frame::bulk_strings_array_from_str(vec!["subscribe", &self.channel_name]);
        streams.insert(self.channel_name, broadcast_stream);

        if let Frame::Array(Some(ref mut array)) = frame {
            array.push(Frame::Integer(streams.len() as u64));
        }

        Ok(frame)
    }

    fn server_inquiry(
        &self,
        tx: oneshot::Sender<broadcast::Receiver<SubscriptionMessage>>,
    ) -> ServerInquiry {
        ServerInquiry::Subscribe(SubscribeInquiry {
            channel_name: self.channel_name.clone(),
            response: tx,
        })
    }
}
