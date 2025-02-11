use rsnano_network::ChannelId;

pub(super) struct PeerScore {
    pub channel_id: ChannelId,
    /// Number of outstanding requests to a peer
    pub outstanding: usize,
    pub request_count_total: usize,
    pub response_count_total: usize,
}

impl PeerScore {
    pub fn new(channel_id: ChannelId) -> Self {
        Self {
            channel_id,
            outstanding: 1,
            request_count_total: 1,
            response_count_total: 0,
        }
    }

    pub fn decay(&mut self) {
        if self.outstanding > 0 {
            self.outstanding -= 1;
        }
    }
}
