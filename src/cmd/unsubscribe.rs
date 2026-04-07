use anyhow::Result;
use tokio_stream::StreamMap;
use tokio_stream::wrappers::BroadcastStream;

use crate::{frame::Frame, parser::Parse, server::subscription_channels::SubscriptionMessage};

#[derive(Debug)]
pub struct Unsubscribe {
    pub channel_name: String,
}

impl Unsubscribe {
    pub fn parse(parse: &mut Parse) -> anyhow::Result<Self> {
        let channel_name = parse.next_string()?;
        Ok(Unsubscribe { channel_name })
    }

    pub fn apply(
        self,
        streams: &mut StreamMap<String, BroadcastStream<SubscriptionMessage>>,
    ) -> Result<Frame> {
        let mut frame = Frame::bulk_strings_array_from_str(vec!["unsubscribe", &self.channel_name]);
        streams.remove(&self.channel_name);

        if let Frame::Array(Some(ref mut array)) = frame {
            array.push(Frame::Integer(streams.len() as u64));
        }

        Ok(frame)
    }
}
