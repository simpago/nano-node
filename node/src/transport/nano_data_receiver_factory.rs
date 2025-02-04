use super::{
    nano_data_receiver::NanoDataReceiver, HandshakeProcess, InboundMessageQueue, LatestKeepalives,
    SynCookies,
};
use crate::{config::NetworkParams, stats::Stats};
use rsnano_core::PrivateKey;
use rsnano_messages::*;
use rsnano_network::{Channel, DataReceiver, DataReceiverFactory, Network};
use std::sync::{Arc, Mutex, RwLock, Weak};

pub(crate) struct NanoDataReceiverFactory {
    network_params: Arc<NetworkParams>,
    stats: Arc<Stats>,
    network: Weak<RwLock<Network>>,
    inbound_queue: Arc<InboundMessageQueue>,
    network_filter: Arc<NetworkFilter>,
    syn_cookies: Arc<SynCookies>,
    node_id: PrivateKey,
    latest_keepalives: Arc<Mutex<LatestKeepalives>>,
}

impl NanoDataReceiverFactory {
    pub fn new(
        network: &Arc<RwLock<Network>>,
        inbound_queue: Arc<InboundMessageQueue>,
        network_filter: Arc<NetworkFilter>,
        network_params: Arc<NetworkParams>,
        stats: Arc<Stats>,
        syn_cookies: Arc<SynCookies>,
        node_id: PrivateKey,
        latest_keepalives: Arc<Mutex<LatestKeepalives>>,
    ) -> Self {
        Self {
            network: Arc::downgrade(network),
            inbound_queue,
            syn_cookies: syn_cookies.clone(),
            node_id: node_id.clone(),
            network_params,
            stats: stats.clone(),
            network_filter,
            latest_keepalives,
        }
    }
}

impl DataReceiverFactory for NanoDataReceiverFactory {
    fn create_receiver_for(&self, channel: Arc<Channel>) -> Box<dyn DataReceiver + Send> {
        let handshake_process = HandshakeProcess::new(
            self.network_params.ledger.genesis_block.hash(),
            self.node_id.clone(),
            self.syn_cookies.clone(),
            self.stats.clone(),
            self.network_params.network.protocol_info(),
        );

        let message_deserializer = MessageDeserializer::new(
            self.network_params.network.protocol_info(),
            self.network_filter.clone(),
            self.network_params.network.work.clone(),
        );
        let mut receiver = NanoDataReceiver::new(
            channel,
            self.network_params.clone(),
            handshake_process,
            message_deserializer,
            self.inbound_queue.clone(),
            self.latest_keepalives.clone(),
            self.stats.clone(),
            self.network.clone(),
        );

        receiver.ensure_handshake();

        Box::new(receiver)
    }
}
