use super::{rebroadcaster_loop::RebroadcastLoop, VoteRebroadcastQueue};
use crate::{stats::Stats, transport::MessageFlooder, wallets::WalletRepresentatives};
use rsnano_core::utils::ContainerInfo;
use rsnano_nullable_clock::SteadyClock;
use std::{
    sync::{Arc, Mutex},
    thread::JoinHandle,
};

pub(crate) struct VoteRebroadcaster {
    queue: Arc<VoteRebroadcastQueue>,
    join_handle: Option<JoinHandle<()>>,
    wallet_reps: Arc<Mutex<WalletRepresentatives>>,
    message_flooder: MessageFlooder,
    stats: Arc<Stats>,
    clock: Arc<SteadyClock>,
}

impl VoteRebroadcaster {
    pub(crate) fn new(
        queue: Arc<VoteRebroadcastQueue>,
        wallet_reps: Arc<Mutex<WalletRepresentatives>>,
        message_flooder: MessageFlooder,
        stats: Arc<Stats>,
        clock: Arc<SteadyClock>,
    ) -> Self {
        Self {
            queue,
            wallet_reps,
            join_handle: None,
            message_flooder,
            stats,
            clock,
        }
    }

    pub fn start(&mut self) {
        let mut rebroadcast_loop = RebroadcastLoop::new(
            self.queue.clone(),
            self.wallet_reps.clone(),
            self.message_flooder.clone(),
            self.stats.clone(),
            self.clock.clone(),
        );

        let handle = std::thread::Builder::new()
            .name("Vote rebroad".to_owned())
            .spawn(move || rebroadcast_loop.run())
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
