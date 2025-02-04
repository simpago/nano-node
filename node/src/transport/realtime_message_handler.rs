use crate::{
    block_processing::{BlockProcessor, BlockSource},
    bootstrap::{BootstrapResponder, Bootstrapper},
    config::NodeConfig,
    consensus::{AggregatorRequest, RequestAggregator, VoteProcessorQueue},
    stats::{DetailType, Direction, StatType, Stats},
    wallets::Wallets,
    Telemetry,
};
use rsnano_core::VoteSource;
use rsnano_messages::{Message, NetworkFilter};
use rsnano_network::{Channel, Network};
use std::{
    net::SocketAddrV6,
    sync::{Arc, RwLock},
};
use tracing::trace;

/// Handle realtime messages (as opposed to bootstrap messages)
pub struct RealtimeMessageHandler {
    stats: Arc<Stats>,
    network_filter: Arc<NetworkFilter>,
    network: Arc<RwLock<Network>>,
    block_processor: Arc<BlockProcessor>,
    config: NodeConfig,
    wallets: Arc<Wallets>,
    request_aggregator: Arc<RequestAggregator>,
    vote_processor_queue: Arc<VoteProcessorQueue>,
    telemetry: Arc<Telemetry>,
    bootstrap_responder: Arc<BootstrapResponder>,
    bootstrapper: Arc<Bootstrapper>,
}

impl RealtimeMessageHandler {
    pub(crate) fn new(
        stats: Arc<Stats>,
        network: Arc<RwLock<Network>>,
        network_filter: Arc<NetworkFilter>,
        block_processor: Arc<BlockProcessor>,
        config: NodeConfig,
        wallets: Arc<Wallets>,
        request_aggregator: Arc<RequestAggregator>,
        vote_processor_queue: Arc<VoteProcessorQueue>,
        telemetry: Arc<Telemetry>,
        bootstrap_responder: Arc<BootstrapResponder>,
        bootstrapper: Arc<Bootstrapper>,
    ) -> Self {
        Self {
            stats,
            network,
            network_filter,
            block_processor,
            config,
            wallets,
            request_aggregator,
            vote_processor_queue,
            telemetry,
            bootstrap_responder,
            bootstrapper,
        }
    }

    pub fn process(&self, message: Message, channel: &Arc<Channel>) {
        self.stats.inc_dir(
            StatType::Message,
            message.message_type().into(),
            Direction::In,
        );
        trace!(?message, "network processed");

        match message {
            Message::Keepalive(keepalive) => {
                // Check for special node port data
                let peer0 = keepalive.peers[0];
                // The first entry is used to inform us of the peering address of the sending node
                if peer0.ip().is_unspecified() && peer0.port() != 0 {
                    let peering_addr =
                        SocketAddrV6::new(*channel.peer_addr().ip(), peer0.port(), 0, 0);

                    // Remember this for future forwarding to other peers
                    self.network
                        .read()
                        .unwrap()
                        .set_peering_addr(channel.channel_id(), peering_addr);
                }
            }
            Message::Publish(publish) => {
                // Put blocks that are being initially broadcasted in a separate queue, so that they won't have to compete with rebroadcasted blocks
                // Both queues have the same priority and size, so the potential for exploiting this is limited
                let source = if publish.is_originator {
                    BlockSource::LiveOriginator
                } else {
                    BlockSource::Live
                };
                let added = self
                    .block_processor
                    .add(publish.block, source, channel.channel_id());
                if !added {
                    // The message couldn't be handled. We have to remove it from the duplicate
                    // filter, so that it can be retransmitted and handled later
                    self.network_filter.clear(publish.digest);
                    self.stats
                        .inc_dir(StatType::Drop, DetailType::Publish, Direction::In);
                }
            }
            Message::ConfirmReq(req) => {
                // Don't load nodes with disabled voting
                // TODO: This check should be cached somewhere
                if self.config.enable_voting && self.wallets.voting_reps_count() > 0 {
                    let aggregator_req = AggregatorRequest {
                        roots_hashes: req.roots_hashes,
                    };
                    self.request_aggregator
                        .request(aggregator_req, channel.channel_id());
                }
            }
            Message::ConfirmAck(ack) => {
                // Ignore zero account votes
                if ack.vote().voting_account.is_zero() {
                    self.stats.inc_dir(
                        StatType::Drop,
                        DetailType::ConfirmAckZeroAccount,
                        Direction::In,
                    );
                }

                let source = match ack.is_rebroadcasted() {
                    true => VoteSource::Rebroadcast,
                    false => VoteSource::Live,
                };

                let added = self.vote_processor_queue.vote(
                    Arc::new(ack.vote().clone()),
                    Some(channel.clone()),
                    source,
                );

                if !added {
                    // The message couldn't be handled. We have to remove it from the duplicate
                    // filter, so that it can be retransmitted and handled later
                    self.network_filter.clear(ack.digest);
                    self.stats
                        .inc_dir(StatType::Drop, DetailType::ConfirmAck, Direction::In);
                }
            }
            Message::NodeIdHandshake(_) => {
                self.stats.inc_dir(
                    StatType::Message,
                    DetailType::NodeIdHandshake,
                    Direction::In,
                );
            }
            Message::TelemetryReq => {
                // Ignore telemetry requests as telemetry is being periodically broadcasted since V25+
            }
            Message::TelemetryAck(ack) => self.telemetry.process(&ack, channel),
            Message::AscPullReq(req) => {
                self.bootstrap_responder.enqueue(req, channel.clone());
            }
            Message::AscPullAck(ack) => self.bootstrapper.process(ack, channel.channel_id()),
            Message::FrontierReq(_)
            | Message::BulkPush
            | Message::BulkPull(_)
            | Message::BulkPullAccount(_) => unreachable!(),
        }
    }
}
