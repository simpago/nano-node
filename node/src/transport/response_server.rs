use super::{
    HandshakeProcess, HandshakeStatus, InboundMessageQueue, LatestKeepalives, MessageDeserializer,
    NetworkFilter, SynCookies,
};
use crate::{
    block_processing::BlockProcessor,
    bootstrap::{
        BootstrapInitiator, BulkPullAccountServer, BulkPullServer, BulkPushServer,
        FrontierReqServer,
    },
    config::NodeFlags,
    stats::{DetailType, Direction, StatType, Stats},
    utils::ThreadPool,
    NetworkParams,
};
use async_trait::async_trait;
use rsnano_core::{KeyPair, PublicKey};
use rsnano_ledger::Ledger;
use rsnano_messages::*;
use rsnano_network::{Channel, ChannelMode, ChannelReader, NetworkInfo};
use rsnano_output_tracker::{OutputListenerMt, OutputTrackerMt};
use std::{
    net::SocketAddrV6,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, RwLock, Weak,
    },
    time::{Duration, Instant},
};
use tracing::debug;

#[derive(Clone, Debug, PartialEq)]
pub struct TcpConfig {
    pub max_inbound_connections: usize,
    pub max_outbound_connections: usize,
    pub max_attempts: usize,
    pub max_attempts_per_ip: usize,
    pub connect_timeout: Duration,
}

impl TcpConfig {
    pub fn for_dev_network() -> Self {
        Self {
            max_inbound_connections: 128,
            max_outbound_connections: 128,
            max_attempts: 128,
            max_attempts_per_ip: 128,
            connect_timeout: Duration::from_secs(5),
        }
    }
}

impl Default for TcpConfig {
    fn default() -> Self {
        Self {
            max_inbound_connections: 2048,
            max_outbound_connections: 2048,
            max_attempts: 60,
            max_attempts_per_ip: 1,
            connect_timeout: Duration::from_secs(60),
        }
    }
}

pub struct ResponseServer {
    channel: Arc<Channel>,
    pub disable_bootstrap_listener: bool,
    pub connections_max: usize,

    // Remote endpoint used to remove response channel even after socket closing
    remote_endpoint: Mutex<SocketAddrV6>,

    network_params: Arc<NetworkParams>,
    last_telemetry_req: Mutex<Option<Instant>>,
    unique_id: usize,
    stats: Arc<Stats>,
    pub disable_bootstrap_bulk_pull_server: bool,
    allow_bootstrap: bool,
    network_info: Arc<RwLock<NetworkInfo>>,
    inbound_queue: Arc<InboundMessageQueue>,
    handshake_process: HandshakeProcess,
    initiate_handshake_listener: OutputListenerMt<()>,
    network_filter: Arc<NetworkFilter>,
    tokio: tokio::runtime::Handle,
    ledger: Arc<Ledger>,
    workers: Arc<dyn ThreadPool>,
    block_processor: Arc<BlockProcessor>,
    bootstrap_initiator: Weak<BootstrapInitiator>,
    latest_keepalives: Arc<Mutex<LatestKeepalives>>,
    flags: NodeFlags,
}

static NEXT_UNIQUE_ID: AtomicUsize = AtomicUsize::new(0);

impl ResponseServer {
    pub fn new(
        network_info: Arc<RwLock<NetworkInfo>>,
        inbound_queue: Arc<InboundMessageQueue>,
        channel: Arc<Channel>,
        network_filter: Arc<NetworkFilter>,
        network_params: Arc<NetworkParams>,
        stats: Arc<Stats>,
        allow_bootstrap: bool,
        syn_cookies: Arc<SynCookies>,
        node_id: KeyPair,
        tokio: tokio::runtime::Handle,
        ledger: Arc<Ledger>,
        workers: Arc<dyn ThreadPool>,
        block_processor: Arc<BlockProcessor>,
        bootstrap_initiator: Arc<BootstrapInitiator>,
        flags: NodeFlags,
        latest_keepalives: Arc<Mutex<LatestKeepalives>>,
    ) -> Self {
        let network_constants = network_params.network.clone();
        let remote_endpoint = channel.info.peer_addr();
        Self {
            network_info,
            inbound_queue,
            channel,
            disable_bootstrap_listener: false,
            connections_max: 64,
            remote_endpoint: Mutex::new(remote_endpoint),
            last_telemetry_req: Mutex::new(None),
            handshake_process: HandshakeProcess::new(
                network_params.ledger.genesis.hash(),
                node_id.clone(),
                syn_cookies,
                stats.clone(),
                remote_endpoint,
                network_constants.protocol_info(),
            ),
            network_params,
            unique_id: NEXT_UNIQUE_ID.fetch_add(1, Ordering::Relaxed),
            stats: stats.clone(),
            disable_bootstrap_bulk_pull_server: false,
            allow_bootstrap,
            initiate_handshake_listener: OutputListenerMt::new(),
            network_filter,
            tokio,
            ledger,
            workers,
            block_processor,
            bootstrap_initiator: Arc::downgrade(&bootstrap_initiator),
            flags,
            latest_keepalives,
        }
    }

    pub fn channel(&self) -> &Arc<Channel> {
        &self.channel
    }

    pub fn track_handshake_initiation(&self) -> Arc<OutputTrackerMt<()>> {
        self.initiate_handshake_listener.track()
    }

    pub fn is_stopped(&self) -> bool {
        !self.channel.info.is_alive()
    }

    pub fn remote_endpoint(&self) -> SocketAddrV6 {
        *self.remote_endpoint.lock().unwrap()
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

    fn to_bootstrap_connection(&self) -> bool {
        if !self.allow_bootstrap {
            return false;
        }

        if self.channel.info.mode() != ChannelMode::Undefined {
            return false;
        }

        if self.disable_bootstrap_listener {
            return false;
        }

        let bootstrap_count = self
            .network_info
            .read()
            .unwrap()
            .count_by_mode(ChannelMode::Bootstrap);

        if bootstrap_count >= self.connections_max {
            return false;
        }

        self.channel.info.set_mode(ChannelMode::Bootstrap);
        debug!("Switched to bootstrap mode ({})", self.remote_endpoint());
        true
    }

    fn set_last_telemetry_req(&self) {
        let mut lk = self.last_telemetry_req.lock().unwrap();
        *lk = Some(Instant::now());
    }

    pub fn unique_id(&self) -> usize {
        self.unique_id
    }

    fn is_undefined_connection(&self) -> bool {
        self.channel.info.mode() == ChannelMode::Undefined
    }

    fn is_bootstrap_connection(&self) -> bool {
        self.channel.info.mode() == ChannelMode::Bootstrap
    }

    fn is_realtime_connection(&self) -> bool {
        self.channel.info.mode() == ChannelMode::Realtime
    }

    fn queue_realtime(&self, message: Message) {
        self.inbound_queue.put(message, self.channel.info.clone());
        // TODO: Throttle if not added
    }

    fn set_last_keepalive(&self, keepalive: Keepalive) {
        self.latest_keepalives
            .lock()
            .unwrap()
            .insert(self.channel.channel_id(), keepalive);
    }

    pub async fn initiate_handshake(&self) {
        self.initiate_handshake_listener.emit(());
        if self
            .handshake_process
            .initiate_handshake(&self.channel)
            .await
            .is_err()
        {
            self.channel.info.close();
        }
    }
}

impl Drop for ResponseServer {
    fn drop(&mut self) {
        let remote_ep = { *self.remote_endpoint.lock().unwrap() };
        debug!("Exiting server: {}", remote_ep);
        self.channel.info.close();
    }
}

pub trait RealtimeMessageVisitor: MessageVisitor {
    fn process(&self) -> bool;
    fn as_message_visitor(&mut self) -> &mut dyn MessageVisitor;
}

pub trait BootstrapMessageVisitor: MessageVisitor {
    fn processed(&self) -> bool;
    fn as_message_visitor(&mut self) -> &mut dyn MessageVisitor;
}

#[async_trait]
pub trait ResponseServerExt {
    fn to_realtime_connection(&self, node_id: &PublicKey) -> bool;
    async fn run(&self);
    async fn process_message(&self, message: Message) -> ProcessResult;
    fn process_realtime(&self, message: Message) -> ProcessResult;
    fn process_bootstrap(&self, message: Message) -> ProcessResult;
}

pub enum ProcessResult {
    Abort,
    Progress,
    Pause,
}

#[async_trait]
impl ResponseServerExt for Arc<ResponseServer> {
    fn to_realtime_connection(&self, node_id: &PublicKey) -> bool {
        if self.channel.info.mode() != ChannelMode::Undefined {
            return false;
        }

        let result = self
            .network_info
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
                self.channel.info.peer_addr(),
                node_id.to_node_id()
            );
            true
        } else {
            debug!(
                channel_id = ?self.channel.channel_id(),
                peer = %self.channel.info.peer_addr(),
                node_id = node_id.to_node_id(),
                "Could not upgrade channel to realtime connection, because another channel for the same node ID was found",
            );
            false
        }
    }

    async fn run(&self) {
        // Set remote_endpoint
        {
            let mut guard = self.remote_endpoint.lock().unwrap();
            if guard.port() == 0 {
                *guard = self.channel.info.peer_addr();
            }
            debug!("Starting response server for peer: {}", *guard);
        }

        let mut message_deserializer = MessageDeserializer::new(
            self.network_params.network.protocol_info(),
            self.network_params.network.work.clone(),
            self.network_filter.clone(),
            ChannelReader::new(self.channel.clone()),
        );

        let mut first_message = true;

        loop {
            if self.is_stopped() {
                break;
            }

            let result = match message_deserializer.read().await {
                Ok(msg) => {
                    if first_message {
                        // TODO: if version using changes => peer misbehaved!
                        self.network_info.read().unwrap().set_protocol_version(
                            self.channel.channel_id(),
                            msg.protocol.version_using,
                        );
                        first_message = false;
                    }
                    self.process_message(msg.message).await
                }
                Err(ParseMessageError::DuplicatePublishMessage) => {
                    // Avoid too much noise about `duplicate_publish_message` errors
                    self.stats.inc_dir(
                        StatType::Filter,
                        DetailType::DuplicatePublishMessage,
                        Direction::In,
                    );
                    ProcessResult::Progress
                }
                Err(ParseMessageError::DuplicateConfirmAckMessage) => {
                    self.stats.inc_dir(
                        StatType::Filter,
                        DetailType::DuplicateConfirmAckMessage,
                        Direction::In,
                    );
                    ProcessResult::Progress
                }
                Err(ParseMessageError::InsufficientWork) => {
                    // IO error or critical error when deserializing message
                    self.stats.inc_dir(
                        StatType::Error,
                        DetailType::InsufficientWork,
                        Direction::In,
                    );
                    ProcessResult::Progress
                }
                Err(e) => {
                    // IO error or critical error when deserializing message
                    self.stats
                        .inc_dir(StatType::Error, DetailType::from(&e), Direction::In);
                    debug!(
                        "Error reading message: {:?} ({})",
                        e,
                        self.remote_endpoint()
                    );
                    ProcessResult::Abort
                }
            };

            match result {
                ProcessResult::Abort => {
                    self.channel.info.close();
                    break;
                }
                ProcessResult::Progress => {}
                ProcessResult::Pause => {
                    break;
                }
            }
        }
    }

    async fn process_message(&self, message: Message) -> ProcessResult {
        self.stats.inc_dir(
            StatType::TcpServer,
            DetailType::from(message.message_type()),
            Direction::In,
        );

        debug_assert!(
            self.is_undefined_connection()
                || self.is_realtime_connection()
                || self.is_bootstrap_connection()
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
        if self.is_undefined_connection() {
            let result = match &message {
                Message::BulkPull(_)
                | Message::BulkPullAccount(_)
                | Message::BulkPush
                | Message::FrontierReq(_) => HandshakeStatus::Bootstrap,
                Message::NodeIdHandshake(payload) => {
                    self.handshake_process
                        .process_handshake(payload, &self.channel)
                        .await
                }

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
                        self.remote_endpoint()
                    );
                    if matches!(result, HandshakeStatus::AbortOwnNodeId) {
                        if let Some(peering_addr) = self.channel.info.peering_addr() {
                            self.network_info.write().unwrap().perma_ban(peering_addr);
                        }
                    }
                    return ProcessResult::Abort;
                }
                HandshakeStatus::Handshake => {
                    return ProcessResult::Progress; // Continue handshake
                }
                HandshakeStatus::Realtime(node_id) => {
                    if !self.to_realtime_connection(&node_id) {
                        self.stats.inc_dir(
                            StatType::TcpServer,
                            DetailType::HandshakeError,
                            Direction::In,
                        );
                        debug!(
                            "Error switching to realtime mode ({})",
                            self.remote_endpoint()
                        );
                        return ProcessResult::Abort;
                    }
                    self.queue_realtime(message);
                    return ProcessResult::Progress; // Continue receiving new messages
                }
                HandshakeStatus::Bootstrap => {
                    if !self.to_bootstrap_connection() {
                        self.stats.inc_dir(
                            StatType::TcpServer,
                            DetailType::HandshakeError,
                            Direction::In,
                        );
                        debug!(
                            "Error switching to bootstrap mode: {:?} ({})",
                            message.message_type(),
                            self.remote_endpoint()
                        );
                        return ProcessResult::Abort;
                    } else {
                        // Fall through to process the bootstrap message
                    }
                }
            }
        } else if self.is_realtime_connection() {
            return self.process_realtime(message);
        }

        // The server will switch to bootstrap mode immediately after processing the first bootstrap message, thus no `else if`
        if self.is_bootstrap_connection() {
            return self.process_bootstrap(message);
        }
        debug_assert!(false);
        ProcessResult::Abort
    }

    fn process_realtime(&self, message: Message) -> ProcessResult {
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

        ProcessResult::Progress
    }

    fn process_bootstrap(&self, message: Message) -> ProcessResult {
        let Some(bootstrap_initiator) = self.bootstrap_initiator.upgrade() else {
            return ProcessResult::Abort;
        };
        match &message {
            Message::BulkPull(payload) => {
                if self.flags.disable_bootstrap_bulk_pull_server {
                    return ProcessResult::Progress;
                }

                // TODO from original code: Add completion callback to bulk pull server
                // TODO from original code: There should be no need to re-copy message as unique pointer, refactor those bulk/frontier pull/push servers
                let mut bulk_pull_server = BulkPullServer::new(
                    payload.clone(),
                    Arc::clone(self),
                    self.ledger.clone(),
                    self.workers.clone(),
                    self.tokio.clone(),
                );
                self.workers.push_task(Box::new(move || {
                    bulk_pull_server.send_next();
                }));

                ProcessResult::Pause
            }
            Message::BulkPullAccount(payload) => {
                if self.flags.disable_bootstrap_bulk_pull_server {
                    return ProcessResult::Progress;
                }
                // original code TODO: Add completion callback to bulk pull server
                // original code TODO: There should be no need to re-copy message as unique pointer, refactor those bulk/frontier pull/push servers
                let bulk_pull_account_server = BulkPullAccountServer::new(
                    Arc::clone(self),
                    payload.clone(),
                    self.workers.clone(),
                    self.ledger.clone(),
                    self.tokio.clone(),
                );
                self.workers.push_task(Box::new(move || {
                    bulk_pull_account_server.send_frontier();
                }));

                ProcessResult::Pause
            }
            Message::BulkPush => {
                // original code TODO: Add completion callback to bulk pull server
                let bulk_push_server = BulkPushServer::new(
                    self.tokio.clone(),
                    Arc::clone(self),
                    self.workers.clone(),
                    self.block_processor.clone(),
                    bootstrap_initiator.clone(),
                    self.stats.clone(),
                    self.network_params.network.work.clone(),
                );

                self.workers.push_task(Box::new(move || {
                    bulk_push_server.throttled_receive();
                }));

                ProcessResult::Pause
            }
            Message::FrontierReq(payload) => {
                // original code TODO: There should be no need to re-copy message as unique pointer, refactor those bulk/frontier pull/push servers
                let response = FrontierReqServer::new(
                    Arc::clone(self),
                    payload.clone(),
                    self.workers.clone(),
                    self.ledger.clone(),
                    self.tokio.clone(),
                );
                self.workers.push_task(Box::new(move || {
                    response.send_next();
                }));

                ProcessResult::Pause
            }
            _ => ProcessResult::Progress,
        }
    }
}
