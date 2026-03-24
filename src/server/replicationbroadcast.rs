use crate::frame::Frame;
use std::sync::Arc;
use tokio::sync::broadcast;

#[derive(Clone)]
pub(super) struct ReplicationBroadcast {
    command_propagation: Arc<broadcast::Sender<Frame>>,
}

impl ReplicationBroadcast {
    pub(super) fn new() -> Self {
        let (tx, _) = broadcast::channel(16);

        Self {
            command_propagation: Arc::new(tx),
        }
    }

    pub(super) fn transmiter(&self) -> Arc<broadcast::Sender<Frame>> {
        self.command_propagation.clone()
    }

    pub(super) fn subscribe(&self) -> broadcast::Receiver<Frame> {
        self.command_propagation.subscribe()
    }
}
