use crate::stats::{Direction, StatType, Stats};
use rsnano_messages::{Message, MessageSerializer, ProtocolInfo};
use rsnano_network::{Channel, ChannelId, Network, TrafficType};
use std::sync::{Arc, RwLock};
use tracing::trace;

pub type MessageCallback = Arc<dyn Fn(ChannelId, &Message) + Send + Sync>;

/// Sends messages via a given channel to a peered node
#[derive(Clone)]
pub struct MessageSender {
    network: Arc<RwLock<Network>>,
    stats: Arc<Stats>,
    message_serializer: MessageSerializer,
    published_callback: Option<MessageCallback>,
}

impl MessageSender {
    pub fn new(
        network: Arc<RwLock<Network>>,
        stats: Arc<Stats>,
        protocol_info: ProtocolInfo,
    ) -> Self {
        Self {
            network,
            stats,
            message_serializer: MessageSerializer::new(protocol_info),
            published_callback: None,
        }
    }

    pub fn new_with_buffer_size(
        network: Arc<RwLock<Network>>,
        stats: Arc<Stats>,
        protocol_info: ProtocolInfo,
        buffer_size: usize,
    ) -> Self {
        Self {
            network,
            stats,
            message_serializer: MessageSerializer::new_with_buffer_size(protocol_info, buffer_size),
            published_callback: None,
        }
    }

    pub fn set_published_callback(&mut self, callback: MessageCallback) {
        self.published_callback = Some(callback);
    }

    pub(crate) fn new_null() -> Self {
        Self::new(
            Arc::new(RwLock::new(Network::new_test_instance())),
            Arc::new(Stats::default()),
            Default::default(),
        )
    }

    pub fn try_send(
        &mut self,
        channel_id: ChannelId,
        message: &Message,
        traffic_type: TrafficType,
    ) -> bool {
        let buffer = self.message_serializer.serialize(message);
        let sent = {
            let network = self.network.read().unwrap();
            let Some(channel) = network.get(channel_id) else {
                return false;
            };
            try_send_serialized_message(&channel, &self.stats, buffer, message, traffic_type)
        };

        if let Some(callback) = &self.published_callback {
            callback(channel_id, message);
        }

        sent
    }

    pub fn try_send_channel(
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

    pub fn try_send_serialized_message(
        &self,
        channel_id: ChannelId,
        buffer: &[u8],
        message: &Message,
        traffic_type: TrafficType,
    ) -> bool {
        let sent = self
            .network
            .read()
            .unwrap()
            .try_send_buffer(channel_id, buffer, traffic_type);

        if sent {
            self.stats
                .inc_dir_aggregate(StatType::Message, message.into(), Direction::Out);
            trace!(%channel_id, message = ?message, "Message sent");
        } else {
            let detail_type = message.into();
            self.stats
                .inc_dir_aggregate(StatType::Drop, detail_type, Direction::Out);
            trace!(%channel_id, message = ?message, "Message dropped");
        }

        sent
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
