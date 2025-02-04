use super::{HandshakeProcess, HandshakeStatus, InboundMessageQueue, LatestKeepalives};
use crate::{
    config::NetworkParams,
    stats::{DetailType, Direction, StatType, Stats},
};
use rsnano_core::NodeId;
use rsnano_messages::*;
use rsnano_network::{
    Channel, ChannelDirection, ChannelMode, DataReceiver, Network, ReceiveResult,
};
use std::{
    sync::{Arc, Mutex, RwLock, Weak},
    time::Instant,
};
use tracing::debug;

pub(crate) struct NanoDataReceiver {
    channel: Arc<Channel>,
    handshake_process: HandshakeProcess,
    message_deserializer: MessageDeserializer,
    inbound_queue: Arc<InboundMessageQueue>,
    last_telemetry_req: Mutex<Option<Instant>>,
    network_params: Arc<NetworkParams>,
    latest_keepalives: Arc<Mutex<LatestKeepalives>>,
    stats: Arc<Stats>,
    network: Weak<RwLock<Network>>,
    first_message: bool,
    node_id: NodeId,
}

impl NanoDataReceiver {
    pub fn new(
        channel: Arc<Channel>,
        network_params: Arc<NetworkParams>,
        handshake_process: HandshakeProcess,
        message_deserializer: MessageDeserializer,
        inbound_queue: Arc<InboundMessageQueue>,
        latest_keepalives: Arc<Mutex<LatestKeepalives>>,
        stats: Arc<Stats>,
        network: Weak<RwLock<Network>>,
    ) -> Self {
        Self {
            channel,
            handshake_process,
            message_deserializer,
            inbound_queue,
            last_telemetry_req: Mutex::new(None),
            network_params,
            latest_keepalives,
            stats,
            network,
            first_message: true,
            node_id: NodeId::ZERO,
        }
    }

    pub fn ensure_handshake(&mut self) {
        if self.channel.direction() == ChannelDirection::Outbound {
            self.initiate_handshake();
        }
    }

    fn initiate_handshake(&self) {
        if self
            .handshake_process
            .initiate_handshake(&self.channel)
            .is_err()
        {
            self.channel.close();
        }
    }

    fn queue_realtime(&self, message: Message) {
        self.inbound_queue.put(message, self.channel.clone());
        // TODO: Throttle if not added
    }

    fn is_outside_cooldown_period(&self) -> bool {
        let lock = self.last_telemetry_req.lock().unwrap();
        match *lock {
            Some(last_req) => {
                last_req.elapsed() >= self.network_params.network.telemetry_request_cooldown
            }
            None => true,
        }
    }

    fn set_last_telemetry_req(&self) {
        let mut lk = self.last_telemetry_req.lock().unwrap();
        *lk = Some(Instant::now());
    }

    fn set_last_keepalive(&self, keepalive: Keepalive) {
        self.latest_keepalives
            .lock()
            .unwrap()
            .insert(self.channel.channel_id(), keepalive);
    }

    fn process_realtime(&self, message: Message) -> ReceiveResult {
        let process = match &message {
            Message::Keepalive(keepalive) => {
                self.set_last_keepalive(keepalive.clone());
                true
            }
            Message::Publish(_)
            | Message::AscPullAck(_)
            | Message::AscPullReq(_)
            | Message::ConfirmAck(_)
            | Message::ConfirmReq(_)
            | Message::FrontierReq(_)
            | Message::TelemetryAck(_) => true,
            Message::TelemetryReq => {
                // Only handle telemetry requests if they are outside of the cooldown period
                if self.is_outside_cooldown_period() {
                    self.set_last_telemetry_req();
                    true
                } else {
                    self.stats.inc_dir(
                        StatType::Telemetry,
                        DetailType::RequestWithinProtectionCacheZone,
                        Direction::In,
                    );
                    false
                }
            }
            _ => false,
        };

        if process {
            self.queue_realtime(message);
        }

        ReceiveResult::Continue
    }

    fn to_realtime_connection(&self, node_id: &NodeId) -> bool {
        if self.channel.mode() != ChannelMode::Undefined {
            return false;
        }

        let Some(network) = self.network.upgrade() else {
            return false;
        };

        let result = network
            .read()
            .unwrap()
            .upgrade_to_realtime_connection(self.channel.channel_id(), *node_id);

        if let Some((channel, observers)) = result {
            for observer in observers {
                observer(channel.clone());
            }

            self.stats
                .inc(StatType::TcpChannels, DetailType::ChannelAccepted);

            debug!(
                "Switched to realtime mode (addr: {}, node_id: {})",
                self.channel.peer_addr(),
                node_id
            );
            true
        } else {
            debug!(
                channel_id = ?self.channel.channel_id(),
                peer = %self.channel.peer_addr(),
                %node_id,
                "Could not upgrade channel to realtime connection, because another channel for the same node ID was found",
            );
            false
        }
    }

    fn process_message(&mut self, message: Message) -> ReceiveResult {
        self.stats.inc_dir(
            StatType::TcpServer,
            DetailType::from(message.message_type()),
            Direction::In,
        );

        /*
         * Server initially starts in undefined state, where it waits for either a handshake or booststrap request message
         * If the server receives a handshake (and it is successfully validated) it will switch to a realtime mode.
         * In realtime mode messages are deserialized and queued to `tcp_message_manager` for further processing.
         * In realtime mode any bootstrap requests are ignored.
         *
         * If the server receives a bootstrap request before receiving a handshake, it will switch to a bootstrap mode.
         * In bootstrap mode once a valid bootstrap request message is received, the server will start a corresponding bootstrap server and pass control to that server.
         * Once that server finishes its task, control is passed back to this server to read and process any subsequent messages.
         * In bootstrap mode any realtime messages are ignored
         */
        if self.channel.mode() == ChannelMode::Undefined {
            let result = match &message {
                Message::BulkPull(_)
                | Message::BulkPullAccount(_)
                | Message::BulkPush
                | Message::FrontierReq(_) => HandshakeStatus::Bootstrap,
                Message::NodeIdHandshake(payload) => self
                    .handshake_process
                    .process_handshake(payload, &self.channel),

                _ => HandshakeStatus::Abort,
            };

            match result {
                HandshakeStatus::Abort | HandshakeStatus::AbortOwnNodeId => {
                    self.stats.inc_dir(
                        StatType::TcpServer,
                        DetailType::HandshakeAbort,
                        Direction::In,
                    );
                    debug!(
                        "Aborting handshake: {:?} ({})",
                        message.message_type(),
                        self.channel.peer_addr()
                    );
                    if matches!(result, HandshakeStatus::AbortOwnNodeId) {
                        if let Some(peering_addr) = self.channel.peering_addr() {
                            if let Some(network) = self.network.upgrade() {
                                network.write().unwrap().perma_ban(peering_addr);
                            }
                        }
                    }
                    return ReceiveResult::Abort;
                }
                HandshakeStatus::Handshake => {
                    return ReceiveResult::Continue; // Continue handshake
                }
                HandshakeStatus::Realtime(node_id) => {
                    self.node_id = node_id;
                    // Wait until send queue is empty for the handshake to complete
                    return ReceiveResult::Pause;
                }
                HandshakeStatus::Bootstrap => {
                    debug!(peer = ?self.channel.peer_addr(), "Legacy bootstrap isn't supported. Closing connection");
                    // Legacy bootstrap is not supported anymore
                    return ReceiveResult::Abort;
                }
            }
        } else if self.channel.mode() == ChannelMode::Realtime {
            return self.process_realtime(message);
        }

        debug_assert!(false);
        ReceiveResult::Abort
    }
}

impl DataReceiver for NanoDataReceiver {
    fn receive(&mut self, data: &[u8]) -> ReceiveResult {
        self.message_deserializer.push(data);
        while let Some(result) = self.message_deserializer.try_deserialize() {
            let result = match result {
                Ok(msg) => {
                    if self.first_message {
                        // TODO: if version using changes => peer misbehaved!
                        self.channel
                            .set_protocol_version(msg.protocol.version_using);
                        self.first_message = false;
                    }
                    self.process_message(msg.message)
                }
                Err(ParseMessageError::DuplicatePublishMessage) => {
                    // Avoid too much noise about `duplicate_publish_message` errors
                    self.stats.inc_dir(
                        StatType::Filter,
                        DetailType::DuplicatePublishMessage,
                        Direction::In,
                    );
                    ReceiveResult::Continue
                }
                Err(ParseMessageError::DuplicateConfirmAckMessage) => {
                    self.stats.inc_dir(
                        StatType::Filter,
                        DetailType::DuplicateConfirmAckMessage,
                        Direction::In,
                    );
                    ReceiveResult::Continue
                }
                Err(ParseMessageError::InsufficientWork) => {
                    // IO error or critical error when deserializing message
                    self.stats.inc_dir(
                        StatType::Error,
                        DetailType::InsufficientWork,
                        Direction::In,
                    );
                    ReceiveResult::Continue
                }
                Err(e) => {
                    // IO error or critical error when deserializing message
                    self.stats
                        .inc_dir(StatType::Error, DetailType::from(&e), Direction::In);
                    debug!(
                        "Error reading message: {:?} ({})",
                        e,
                        self.channel.peer_addr()
                    );
                    ReceiveResult::Abort
                }
            };

            if !matches!(result, ReceiveResult::Continue) {
                return result;
            }
        }

        ReceiveResult::Continue
    }

    fn try_unpause(&self) -> ReceiveResult {
        if self.channel.mode() != ChannelMode::Undefined {
            // Pausing is currently only needed during handshake
            return ReceiveResult::Continue;
        }

        // Wait until all outbound messages are processed.
        // This is needed for the handshake because the channel can't be upgraded to
        // a realtime channel unless the handshake response is actually sent out
        if self.channel.queue_len() > 0 {
            return ReceiveResult::Pause;
        }

        if !self.to_realtime_connection(&self.node_id) {
            self.stats.inc_dir(
                StatType::TcpServer,
                DetailType::HandshakeError,
                Direction::In,
            );
            debug!(
                "Error switching to realtime mode ({})",
                self.channel.peer_addr()
            );
            return ReceiveResult::Abort;
        }

        ReceiveResult::Continue
    }
}

impl Drop for NanoDataReceiver {
    fn drop(&mut self) {
        self.channel.close();
    }
}
