use rsnano_core::PublicKey;
use rsnano_network::{Channel, ChannelId};
use rsnano_nullable_clock::Timestamp;
use std::sync::Arc;

/// A representative to which we have a direct connection
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeeredRep {
    pub account: PublicKey,
    pub channel_id: ChannelId,
    pub last_request: Timestamp,
}

impl PeeredRep {
    pub fn new(account: PublicKey, channel_id: ChannelId, last_request: Timestamp) -> Self {
        Self {
            account,
            channel_id,
            last_request,
        }
    }

    pub fn new2(account: PublicKey, channel: Arc<Channel>, last_request: Timestamp) -> Self {
        Self {
            account,
            channel_id: channel.channel_id(),
            last_request,
        }
    }
}
