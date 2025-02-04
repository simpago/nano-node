use rsnano_core::PublicKey;
use rsnano_network::{Channel, ChannelId};
use rsnano_nullable_clock::Timestamp;
use std::sync::Arc;

/// A representative to which we have a direct connection
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeeredRep {
    pub account: PublicKey,
    pub channel: Arc<Channel>,
    pub last_request: Timestamp,
}

impl PeeredRep {
    pub fn new(account: PublicKey, channel: Arc<Channel>, last_request: Timestamp) -> Self {
        Self {
            account,
            channel,
            last_request,
        }
    }

    pub fn channel_id(&self) -> ChannelId {
        self.channel.channel_id()
    }
}
