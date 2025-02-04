use super::{vote_generator::VoteGenerator, LocalVoteHistory};
use crate::{
    config::NodeConfig, consensus::VoteBroadcaster, stats::Stats, transport::MessageSender,
    wallets::Wallets, NetworkParams,
};
use rsnano_core::{utils::ContainerInfo, BlockHash, Networks, Root, SavedBlock};
use rsnano_ledger::Ledger;
use rsnano_network::ChannelId;
use rsnano_nullable_clock::SteadyClock;
use rsnano_output_tracker::{OutputListenerMt, OutputTrackerMt};
use std::{sync::Arc, time::Duration};

#[derive(Clone)]
pub struct VoteGenerationEvent {
    pub channel_id: ChannelId,
    pub blocks: Vec<SavedBlock>,
    pub final_vote: bool,
}

pub struct VoteGenerators {
    non_final_vote_generator: VoteGenerator,
    final_vote_generator: VoteGenerator,
    vote_listener: OutputListenerMt<VoteGenerationEvent>,
    voting_delay: Duration,
}

impl VoteGenerators {
    fn voting_delay_for(network: Networks) -> Duration {
        match network {
            Networks::NanoDevNetwork => Duration::from_secs(1),
            _ => Duration::from_secs(15),
        }
    }

    pub(crate) fn new(
        ledger: Arc<Ledger>,
        wallets: Arc<Wallets>,
        history: Arc<LocalVoteHistory>,
        stats: Arc<Stats>,
        config: &NodeConfig,
        network_params: &NetworkParams,
        vote_broadcaster: Arc<VoteBroadcaster>,
        message_sender: MessageSender,
        clock: Arc<SteadyClock>,
    ) -> Self {
        let voting_delay = Self::voting_delay_for(network_params.network.current_network);

        let non_final_vote_generator = VoteGenerator::new(
            ledger.clone(),
            wallets.clone(),
            history.clone(),
            false, //none-final
            stats.clone(),
            message_sender.clone(),
            voting_delay,
            config.vote_generator_delay,
            vote_broadcaster.clone(),
            clock.clone(),
        );

        let final_vote_generator = VoteGenerator::new(
            ledger,
            wallets,
            history,
            true, //final
            stats,
            message_sender.clone(),
            voting_delay,
            config.vote_generator_delay,
            vote_broadcaster,
            clock,
        );

        Self {
            non_final_vote_generator,
            final_vote_generator,
            vote_listener: OutputListenerMt::new(),
            voting_delay,
        }
    }

    pub fn voting_delay(&self) -> Duration {
        self.voting_delay
    }

    pub fn start(&self) {
        self.non_final_vote_generator.start();
        self.final_vote_generator.start();
    }

    pub fn stop(&self) {
        self.non_final_vote_generator.stop();
        self.final_vote_generator.stop();
    }

    pub fn track(&self) -> Arc<OutputTrackerMt<VoteGenerationEvent>> {
        self.vote_listener.track()
    }

    pub(crate) fn generate_final_vote(&self, root: &Root, hash: &BlockHash) {
        self.final_vote_generator.add(root, hash);
    }

    pub(crate) fn generate_final_votes(
        &self,
        blocks: &[SavedBlock],
        channel_id: ChannelId,
    ) -> usize {
        if self.vote_listener.is_tracked() {
            self.vote_listener.emit(VoteGenerationEvent {
                channel_id,
                blocks: blocks.to_vec(),
                final_vote: true,
            });
        }
        self.final_vote_generator.generate(blocks, channel_id)
    }

    pub fn generate_non_final_vote(&self, root: &Root, hash: &BlockHash) {
        self.non_final_vote_generator.add(root, hash);
    }

    pub fn generate_non_final_votes(&self, blocks: &[SavedBlock], channel_id: ChannelId) -> usize {
        if self.vote_listener.is_tracked() {
            self.vote_listener.emit(VoteGenerationEvent {
                channel_id,
                blocks: blocks.to_vec(),
                final_vote: false,
            });
        }
        self.non_final_vote_generator.generate(blocks, channel_id)
    }

    pub(crate) fn container_info(&self) -> ContainerInfo {
        ContainerInfo::builder()
            .node("non_final", self.non_final_vote_generator.container_info())
            .node("final", self.final_vote_generator.container_info())
            .finish()
    }
}
