use super::wallet_reps_cache::WalletRepsCache;
use crate::{
    stats::{DetailType, StatType, Stats},
    transport::MessageFlooder,
    wallets::WalletRepresentatives,
};
use rsnano_core::Vote;
use rsnano_messages::{ConfirmAck, Message};
use rsnano_network::TrafficType;
use std::sync::{Arc, Mutex};

/// Rebroadcasts a given vote if necessary
pub(super) struct RebroadcastProcessor {
    wallet_reps: WalletRepsCache,
    message_flooder: MessageFlooder,
    stats: Arc<Stats>,
}

impl RebroadcastProcessor {
    pub(super) fn new(
        wallet_reps: Arc<Mutex<WalletRepresentatives>>,
        message_flooder: MessageFlooder,
        stats: Arc<Stats>,
    ) -> Self {
        Self {
            wallet_reps: WalletRepsCache::new(wallet_reps),
            message_flooder,
            stats,
        }
    }

    pub fn process_vote(&mut self, vote: &Vote) {
        self.wallet_reps.refresh_if_needed();

        if self.should_republish(vote) {
            self.republish(vote);
        }
    }

    fn should_republish(&self, vote: &Vote) -> bool {
        // Disable vote rebroadcasting if the node has a principal representative (or close to)
        if self.wallet_reps.have_half_rep() {
            return false;
        }

        // Don't republish votes created by this node
        if self.wallet_reps.exists(vote.voting_account) {
            return false;
        }

        true
    }

    fn republish(&mut self, vote: &Vote) {
        self.update_stats(vote);
        let message = self.create_ack_message(vote);

        self.message_flooder
            .flood(&message, TrafficType::VoteRebroadcast, 0.5);
    }

    fn create_ack_message(&self, vote: &Vote) -> Message {
        Message::ConfirmAck(ConfirmAck::new_with_rebroadcasted_vote(vote.clone()))
    }

    fn update_stats(&self, vote: &Vote) {
        self.stats
            .inc(StatType::VoteRebroadcaster, DetailType::Rebroadcast);

        self.stats.add(
            StatType::VoteRebroadcaster,
            DetailType::RebroadcastHashes,
            vote.hashes.len() as u64,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{transport::FloodEvent, wallets::WalletRepresentatives};
    use rsnano_core::Vote;
    use std::sync::Mutex;

    #[test]
    fn rebroadcast_vote() {
        let wallet_reps = Arc::new(Mutex::new(WalletRepresentatives::default()));
        let message_flooder = MessageFlooder::new_null();
        let flood_tracker = message_flooder.track_floods();
        let stats = Arc::new(Stats::default());

        let mut processor = RebroadcastProcessor::new(wallet_reps, message_flooder, stats);

        let vote = Vote::new_test_instance();
        processor.process_vote(&vote);

        assert_eq!(
            flood_tracker.output(),
            vec![FloodEvent {
                message: Message::ConfirmAck(ConfirmAck::new_with_rebroadcasted_vote(vote)),
                traffic_type: TrafficType::VoteRebroadcast,
                scale: 0.5
            }]
        )
    }

    #[test]
    fn dont_rebroadcast_when_node_has_half_rep() {
        let wallet_reps = Arc::new(Mutex::new(WalletRepresentatives::default()));
        let message_flooder = MessageFlooder::new_null();
        let flood_tracker = message_flooder.track_floods();
        let stats = Arc::new(Stats::default());

        let mut processor = RebroadcastProcessor::new(wallet_reps.clone(), message_flooder, stats);

        wallet_reps.lock().unwrap().set_have_half_rep(true);

        let vote = Vote::new_test_instance();
        processor.process_vote(&vote);

        assert_eq!(flood_tracker.output(), vec![]);
    }
}
