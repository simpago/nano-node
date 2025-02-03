use super::{rebroadcaster_loop::RebroadcastLoop, VoteRebroadcastQueue};
use crate::{stats::Stats, transport::MessageFlooder, wallets::Wallets};
use rsnano_core::utils::ContainerInfo;
use std::{sync::Arc, thread::JoinHandle};

pub(crate) struct VoteRebroadcaster {
    queue: Arc<VoteRebroadcastQueue>,
    join_handle: Option<JoinHandle<()>>,
    wallets: Arc<Wallets>,
    message_flooder: MessageFlooder,
    stats: Arc<Stats>,
}

impl VoteRebroadcaster {
    pub(crate) fn new(
        queue: Arc<VoteRebroadcastQueue>,
        wallets: Arc<Wallets>,
        message_flooder: MessageFlooder,
        stats: Arc<Stats>,
    ) -> Self {
        Self {
            queue,
            wallets,
            join_handle: None,
            message_flooder,
            stats,
        }
    }

    pub fn start(&mut self) {
        let mut rebroadcast_loop = RebroadcastLoop::new(
            self.queue.clone(),
            self.wallets.clone(),
            self.message_flooder.clone(),
            self.stats.clone(),
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
