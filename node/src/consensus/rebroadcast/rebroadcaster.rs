use super::{rebroadcast_processor::RebroadcastProcessor, VoteRebroadcastQueue};
use crate::{stats::Stats, transport::MessageFlooder, wallets::WalletRepresentatives};
use rsnano_core::utils::ContainerInfo;
use std::{
    sync::{Arc, Mutex},
    thread::JoinHandle,
};

/// Rebroadcasts votes that were created by other nodes
pub(crate) struct VoteRebroadcaster {
    queue: Arc<VoteRebroadcastQueue>,
    join_handle: Option<JoinHandle<()>>,
    wallet_reps: Arc<Mutex<WalletRepresentatives>>,
    message_flooder: MessageFlooder,
    stats: Arc<Stats>,
}

impl VoteRebroadcaster {
    pub(crate) fn new(
        queue: Arc<VoteRebroadcastQueue>,
        wallet_reps: Arc<Mutex<WalletRepresentatives>>,
        message_flooder: MessageFlooder,
        stats: Arc<Stats>,
    ) -> Self {
        Self {
            queue,
            wallet_reps,
            join_handle: None,
            message_flooder,
            stats,
        }
    }

    pub fn start(&mut self) {
        let queue = self.queue.clone();
        let mut rebroadcast_processor = RebroadcastProcessor::new(
            self.wallet_reps.clone(),
            self.message_flooder.clone(),
            self.stats.clone(),
        );

        let handle = std::thread::Builder::new()
            .name("Vote rebroad".to_owned())
            .spawn(move || {
                while let Some(vote) = queue.dequeue() {
                    rebroadcast_processor.process_vote(&vote);
                }
            })
            .unwrap();
        self.join_handle = Some(handle);
    }

    pub fn stop(&mut self) {
        self.queue.stop();
        if let Some(handle) = self.join_handle.take() {
            handle.join().unwrap();
        }
    }

    pub fn container_info(&self) -> ContainerInfo {
        self.queue.container_info()
    }
}

impl Drop for VoteRebroadcaster {
    fn drop(&mut self) {
        self.stop();
    }
}
