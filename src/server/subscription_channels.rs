use std::collections::HashMap;
use std::sync::{Arc, Mutex};
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
}
