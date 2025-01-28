use rsnano_network::{Channel, ChannelId};
use std::sync::{Arc, Weak};

pub(super) struct PeerScore {
    pub channel_id: ChannelId,
    pub channel: Weak<Channel>,
    /// Number of outstanding requests to a peer
    pub outstanding: usize,
    pub request_count_total: usize,
    pub response_count_total: usize,
}

impl PeerScore {
    pub fn new(channel: &Arc<Channel>) -> Self {
        Self {
            channel_id: channel.channel_id(),
            channel: Arc::downgrade(channel),
            outstanding: 1,
            request_count_total: 1,
            response_count_total: 0,
        }
    }

    pub fn is_alive(&self) -> bool {
        self.channel
            .upgrade()
            .map(|i| i.is_alive())
            .unwrap_or(false)
    }

    pub fn decay(&mut self) {
        if self.outstanding > 0 {
            self.outstanding -= 1;
        }
    }
}
