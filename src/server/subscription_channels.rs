use anyhow::Result;
use std::collections::HashMap;
use tokio::sync::broadcast;

pub type SubscriptionMessage = String;

pub struct SubscriptionChannels {
    data: HashMap<String, broadcast::Sender<SubscriptionMessage>>,
}

impl SubscriptionChannels {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn new_subscriber(
        &mut self,
        channel_name: String,
    ) -> broadcast::Receiver<SubscriptionMessage> {
        self.data
            .entry(channel_name)
            .or_insert(broadcast::channel(16).0)
            .subscribe()
    }

    pub fn send_message(&self, channel_name: &str, message: SubscriptionMessage) -> Result<u64> {
        if let Some(tx) = self.data.get(channel_name) {
            Ok(tx.send(message)? as u64)
        } else {
            Ok(0)
        }
    }
}
