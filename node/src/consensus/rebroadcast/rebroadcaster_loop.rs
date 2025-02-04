use super::{wallet_reps_cache::WalletRepsCache, VoteRebroadcastQueue};
use crate::{
    stats::{DetailType, StatType, Stats},
    transport::MessageFlooder,
};
use rsnano_core::Vote;
use rsnano_messages::{ConfirmAck, Message};
use rsnano_network::TrafficType;
use std::sync::Arc;

pub(super) struct RebroadcastLoop {
    queue: Arc<VoteRebroadcastQueue>,
    message_flooder: MessageFlooder,
    stats: Arc<Stats>,
    wallet_reps: WalletRepsCache,
}

impl RebroadcastLoop {
    pub(super) fn new(
        queue: Arc<VoteRebroadcastQueue>,
        message_flooder: MessageFlooder,
        wallet_reps: WalletRepsCache,
        stats: Arc<Stats>,
    ) -> Self {
        Self {
            queue,
            message_flooder,
            stats,
            wallet_reps,
        }
    }

    pub fn run(&mut self) {
        while let Some(vote) = self.queue.dequeue() {
            self.process_vote(&vote);
        }
    }

    fn process_vote(&mut self, vote: &Vote) {
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
    fn empty_queue() {
        let queue = Arc::new(
            VoteRebroadcastQueue::build()
                .block_when_empty(false)
                .finish(),
        );
        let wallet_reps = Arc::new(Mutex::new(WalletRepresentatives::default()));
        let wallet_reps_cache = WalletRepsCache::new(wallet_reps);
        let message_flooder = MessageFlooder::new_null();
        let stats = Arc::new(Stats::default());
        let mut rebroadcast_loop =
            RebroadcastLoop::new(queue, message_flooder, wallet_reps_cache, stats);

        rebroadcast_loop.run();
    }

    #[test]
    fn rebroadcast_vote() {
        let queue = Arc::new(
            VoteRebroadcastQueue::build()
                .block_when_empty(false)
                .finish(),
        );
        let wallet_reps = Arc::new(Mutex::new(WalletRepresentatives::default()));
        let wallet_reps_cache = WalletRepsCache::new(wallet_reps);
        let message_flooder = MessageFlooder::new_null();
        let stats = Arc::new(Stats::default());
        let flood_tracker = message_flooder.track_floods();
        let mut rebroadcast_loop =
            RebroadcastLoop::new(queue.clone(), message_flooder, wallet_reps_cache, stats);

        let vote = Vote::new_test_instance();
        queue.enqueue(Arc::new(vote.clone()));

        rebroadcast_loop.run();

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
        let queue = Arc::new(
            VoteRebroadcastQueue::build()
                .block_when_empty(false)
                .finish(),
        );
        let wallet_reps = Arc::new(Mutex::new(WalletRepresentatives::default()));
        let wallet_reps_cache = WalletRepsCache::new(wallet_reps.clone());
        let message_flooder = MessageFlooder::new_null();
        let stats = Arc::new(Stats::default());
        let flood_tracker = message_flooder.track_floods();
        wallet_reps.lock().unwrap().set_have_half_rep(true);
        let mut rebroadcast_loop =
            RebroadcastLoop::new(queue.clone(), message_flooder, wallet_reps_cache, stats);

        let vote = Vote::new_test_instance();
        queue.enqueue(Arc::new(vote.clone()));

        rebroadcast_loop.run();

        assert_eq!(flood_tracker.output(), vec![]);
    }

    #[test]
    #[ignore]
    fn update_rep_status_periodically() {
        let queue = Arc::new(
            VoteRebroadcastQueue::build()
                .block_when_empty(false)
                .finish(),
        );
        let wallet_reps = Arc::new(Mutex::new(WalletRepresentatives::default()));
        let wallet_reps_cache = WalletRepsCache::new(wallet_reps.clone());
        let message_flooder = MessageFlooder::new_null();
        let stats = Arc::new(Stats::default());
        let flood_tracker = message_flooder.track_floods();
        let mut rebroadcast_loop =
            RebroadcastLoop::new(queue.clone(), message_flooder, wallet_reps_cache, stats);

        let vote = Vote::new_test_instance();
        queue.enqueue(Arc::new(vote.clone()));
        let vote = Vote::new_test_instance();
        queue.enqueue(Arc::new(vote.clone()));

        rebroadcast_loop.run();
        assert_eq!(flood_tracker.output().len(), 1);

        wallet_reps.lock().unwrap().set_have_half_rep(true);
        rebroadcast_loop.run();
        assert_eq!(flood_tracker.output().len(), 2);

        //        clock.advance(RebroadcastLoop::REFRESH_INTERVAL);
        queue.enqueue(Arc::new(vote.clone()));
        rebroadcast_loop.run();
        assert_eq!(flood_tracker.output().len(), 2);
    }
}
