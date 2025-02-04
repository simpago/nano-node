use super::KeepaliveMessageFactory;
use crate::transport::MessageSender;
use rsnano_core::utils::Peer;
use rsnano_network::{
    utils::into_ipv6_socket_address, Channel, Network, PeerConnector, TrafficType,
};
use rsnano_output_tracker::{OutputListenerMt, OutputTrackerMt};
use std::{
    net::SocketAddr,
    sync::{Arc, Mutex, RwLock},
};
use tracing::error;

/// Connects to a peer if we don't have a connection
/// or it sends a keepalive message if we are already connected
pub struct KeepalivePublisher {
    keepalive_listener: OutputListenerMt<Peer>,
    network: Arc<RwLock<Network>>,
    peer_connector: Arc<PeerConnector>,
    message_sender: Mutex<MessageSender>,
    message_factory: Arc<KeepaliveMessageFactory>,
}

impl KeepalivePublisher {
    pub fn new(
        network: Arc<RwLock<Network>>,
        peer_connector: Arc<PeerConnector>,
        message_sender: MessageSender,
        message_factory: Arc<KeepaliveMessageFactory>,
    ) -> Self {
        Self {
            keepalive_listener: OutputListenerMt::new(),
            network,
            peer_connector,
            message_sender: Mutex::new(message_sender),
            message_factory,
        }
    }

    pub fn track_keepalives(&self) -> Arc<OutputTrackerMt<Peer>> {
        self.keepalive_listener.track()
    }

    pub async fn keepalive_or_connect(&self, address: String, port: u16) {
        self.keepalive_listener
            .emit(Peer::new(address.clone(), port));
        match tokio::net::lookup_host((address.as_str(), port)).await {
            Ok(addresses) => {
                for addr in addresses {
                    self.keepalive_or_connect_socket(addr);
                }
            }
            Err(e) => {
                error!(
                    "Error resolving address for keepalive: {}:{} ({})",
                    address, port, e
                )
            }
        }
    }

    fn keepalive_or_connect_socket(&self, peer: SocketAddr) {
        let peer_v6 = into_ipv6_socket_address(peer);

        let channel = self
            .network
            .read()
            .unwrap()
            .find_realtime_channel_by_peering_addr(&peer_v6)
            .cloned();

        match channel {
            Some(channel) => {
                self.try_send_keepalive(&channel);
            }
            None => {
                let _ = self.peer_connector.connect_to(peer_v6);
            }
        }
    }

    fn try_send_keepalive(&self, channel: &Channel) {
        let keepalive = self.message_factory.create_keepalive();

        self.message_sender
            .lock()
            .unwrap()
            .try_send(channel, &keepalive, TrafficType::Keepalive);
    }
}
