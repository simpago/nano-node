use crate::stats::{Direction, StatType, Stats};
use rsnano_messages::{Message, MessageSerializer, ProtocolInfo};
use rsnano_network::{Channel, ChannelId, TrafficType};
use std::sync::Arc;
use tracing::trace;

pub type MessageCallback = Arc<dyn Fn(ChannelId, &Message) + Send + Sync>;

/// Sends messages via a given channel to a peered node
#[derive(Clone)]
pub struct MessageSender {
    stats: Arc<Stats>,
    message_serializer: MessageSerializer,
    published_callback: Option<MessageCallback>,
}

impl MessageSender {
    pub fn new(stats: Arc<Stats>, protocol_info: ProtocolInfo) -> Self {
        Self {
            stats,
            message_serializer: MessageSerializer::new(protocol_info),
            published_callback: None,
        }
    }

    pub fn new_with_buffer_size(
        stats: Arc<Stats>,
        protocol_info: ProtocolInfo,
        buffer_size: usize,
    ) -> Self {
        Self {
            stats,
            message_serializer: MessageSerializer::new_with_buffer_size(protocol_info, buffer_size),
            published_callback: None,
        }
    }

    pub fn set_published_callback(&mut self, callback: MessageCallback) {
        self.published_callback = Some(callback);
    }

    pub(crate) fn new_null() -> Self {
        Self::new(Arc::new(Stats::default()), Default::default())
    }

    pub fn try_send(
        &mut self,
        channel: &Channel,
        message: &Message,
        traffic_type: TrafficType,
    ) -> bool {
        let buffer = self.message_serializer.serialize(message);
        let sent =
            { try_send_serialized_message(&channel, &self.stats, buffer, message, traffic_type) };

        if let Some(callback) = &self.published_callback {
            callback(channel.channel_id(), message);
        }

        sent
    }

    pub fn get_serializer(&self) -> MessageSerializer {
        self.message_serializer.clone()
    }
}

pub(crate) fn try_send_serialized_message(
    channel: &Channel,
    stats: &Stats,
    buffer: &[u8],
    message: &Message,
    traffic_type: TrafficType,
) -> bool {
    let sent = channel.send(buffer, traffic_type);

    if sent {
        stats.inc_dir_aggregate(StatType::Message, message.into(), Direction::Out);
        trace!(peer=%channel.peer_addr(), message = ?message, "Message sent");
    } else {
        let detail_type = message.into();
        stats.inc_dir_aggregate(StatType::Drop, detail_type, Direction::Out);
        trace!(peer=%channel.peer_addr(), message = ?message, "Message dropped");
    }

    sent
}
