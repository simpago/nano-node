use super::VoteRebroadcastQueue;
use crate::{
    stats::{DetailType, StatType, Stats},
    transport::MessageFlooder,
    wallets::WalletRepresentatives,
};
use rsnano_messages::{ConfirmAck, Message};
use rsnano_network::TrafficType;
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

pub(super) struct RebroadcastLoop {
    queue: Arc<VoteRebroadcastQueue>,
    wallet_reps: Arc<Mutex<WalletRepresentatives>>,
    message_flooder: MessageFlooder,
    stats: Arc<Stats>,
    wallet_reps_copy: WalletRepresentatives,
    last_refresh: Instant,
    paused: bool,
}

impl RebroadcastLoop {
    pub(super) fn new(
        queue: Arc<VoteRebroadcastQueue>,
        wallet_reps: Arc<Mutex<WalletRepresentatives>>,
        message_flooder: MessageFlooder,
        stats: Arc<Stats>,
    ) -> Self {
        Self {
            queue,
            wallet_reps,
            message_flooder,
            stats,
            wallet_reps_copy: Default::default(),
            last_refresh: Instant::now(),
            paused: false,
        }
    }

    pub fn run(&mut self) {
        self.refresh();

        while let Some(vote) = self.queue.dequeue() {
            self.refresh_if_needed();

            if self.paused {
                continue;
            }

            if self.wallet_reps_copy.exists(&vote.voting_account.into()) {
                // Don't republish votes created by this node
                continue;
            }

            self.stats
                .inc(StatType::VoteRebroadcaster, DetailType::Rebroadcast);

            self.stats.add(
                StatType::VoteRebroadcaster,
                DetailType::RebroadcastHashes,
                vote.hashes.len() as u64,
            );

            let ack = Message::ConfirmAck(ConfirmAck::new_with_rebroadcasted_vote(
                vote.as_ref().clone(),
            ));

            self.message_flooder
                .flood(&ack, TrafficType::VoteRebroadcast, 0.5);
        }
    }

    fn refresh_if_needed(&mut self) {
        if self.last_refresh.elapsed() >= Duration::from_secs(15) {
            self.refresh();
        }
    }

    fn refresh(&mut self) {
        self.wallet_reps_copy = self.wallet_reps.lock().unwrap().clone();
        // Disable vote rebroadcasting if the node has a principal representative (or close to)
        self.paused = self.wallet_reps_copy.have_half_rep();
        self.last_refresh = Instant::now();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_queue() {
        let queue = Arc::new(
            VoteRebroadcastQueue::build()
                .block_when_empty(false)
                .finish(),
        );
        let wallet_reps = Arc::new(Mutex::new(WalletRepresentatives::default()));
        let message_flooder = MessageFlooder::new_null();
        let stats = Arc::new(Stats::default());
        let mut rebroadcast_loop = RebroadcastLoop::new(queue, wallet_reps, message_flooder, stats);

        rebroadcast_loop.run();
    }
}
