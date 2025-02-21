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
    message_flooder: Option<MessageFlooder>,
    stats: Arc<Stats>,
    vote_processed_callback: Option<Box<dyn Fn() + Send + Sync>>,
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
            message_flooder: Some(message_flooder),
            stats,
            vote_processed_callback: None,
        }
    }

    pub fn start(&mut self) {
        let queue = self.queue.clone();
        let mut rebroadcast_processor = RebroadcastProcessor::new(
            self.wallet_reps.clone(),
            self.message_flooder.take().unwrap(),
            self.stats.clone(),
        );
        let callback = self.vote_processed_callback.take();

        let handle = std::thread::Builder::new()
            .name("Vote rebroad".to_owned())
            .spawn(move || {
                while let Some(vote) = queue.dequeue() {
                    rebroadcast_processor.process_vote(&vote);
                    if let Some(cb) = &callback {
                        cb();
                    }
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

    #[allow(dead_code)]
    pub fn on_vote_processed(&mut self, callback: impl Fn() + Send + Sync + 'static) {
        self.vote_processed_callback = Some(Box::new(callback));
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::FloodEvent;
    use rsnano_core::{utils::OneShotNotification, Vote};
    use rsnano_output_tracker::OutputTrackerMt;

    #[test]
    fn rebroadcast() {
        let (mut rebroadcaster, queue, flood_tracker) = create_fixture();

        let done = OneShotNotification::new();
        let done2 = done.clone();
        rebroadcaster.on_vote_processed(move || done2.notify(()));
        rebroadcaster.start();

        queue.enqueue(Arc::new(Vote::new_test_instance()));

        done.wait();

        assert_eq!(flood_tracker.output().len(), 1);
    }

    #[test]
    fn container_info() {
        let (rebroadcaster, queue, _) = create_fixture();
        assert_eq!(rebroadcaster.container_info(), queue.container_info());
    }

    fn create_fixture() -> (
        VoteRebroadcaster,
        Arc<VoteRebroadcastQueue>,
        Arc<OutputTrackerMt<FloodEvent>>,
    ) {
        let queue = Arc::new(VoteRebroadcastQueue::default());
        let wallet_reps = Arc::new(Mutex::new(WalletRepresentatives::default()));
        let message_flooder = MessageFlooder::new_null();
        let flood_tracker = message_flooder.track_floods();
        let stats = Arc::new(Stats::default());
        let rebroadcaster =
            VoteRebroadcaster::new(queue.clone(), wallet_reps, message_flooder, stats);

        (rebroadcaster, queue, flood_tracker)
    }
}
