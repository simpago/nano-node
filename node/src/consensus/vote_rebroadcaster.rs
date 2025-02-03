use crate::{
    stats::{DetailType, StatType, Stats},
    transport::MessageFlooder,
    wallets::{WalletRepresentatives, Wallets},
};
use rsnano_core::{utils::ContainerInfo, BlockHash, Vote, VoteCode};
use rsnano_messages::{ConfirmAck, Message};
use rsnano_network::TrafficType;
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

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
        let mut rebroadcast_loop = RebroadcastLoop {
            queue: self.queue.clone(),
            wallets: self.wallets.clone(),
            message_flooder: self.message_flooder.clone(),
            last_refresh: Instant::now(),
            stats: self.stats.clone(),
            wallet_reps: Default::default(),
            paused: false,
        };

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

struct RebroadcastLoop {
    queue: Arc<VoteRebroadcastQueue>,
    wallets: Arc<Wallets>,
    message_flooder: MessageFlooder,
    stats: Arc<Stats>,
    last_refresh: Instant,
    wallet_reps: WalletRepresentatives,
    paused: bool,
}

impl RebroadcastLoop {
    fn run(&mut self) {
        self.refresh();

        while let Some(vote) = self.queue.dequeue() {
            self.refresh_if_needed();

            if self.paused {
                continue;
            }

            if self.wallet_reps.exists(&vote.voting_account.into()) {
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
        self.wallet_reps = self.wallets.representatives();
        // Disable vote rebroadcasting if the node has a principal representative (or close to)
        self.paused = self.wallet_reps.have_half_rep();
        self.last_refresh = Instant::now();
    }
}

pub(crate) struct VoteRebroadcastQueue {
    queue: Mutex<VecDeque<Arc<Vote>>>,
    enqueued: Condvar,
    stopped: AtomicBool,
    stats: Arc<Stats>,
}

struct QueueData {}

impl VoteRebroadcastQueue {
    const MAX_QUEUE: usize = 1024 * 16;

    pub(crate) fn new(stats: Arc<Stats>) -> Self {
        Self {
            queue: Mutex::new(Default::default()),
            enqueued: Condvar::new(),
            stopped: AtomicBool::new(false),
            stats,
        }
    }

    pub fn handle_processed_vote(&self, vote: &Arc<Vote>, results: &HashMap<BlockHash, VoteCode>) {
        let processed = results.iter().any(|(_, code)| *code == VoteCode::Vote);
        if processed {
            self.enqueue(vote.clone());
        }
    }

    pub fn enqueue(&self, vote: Arc<Vote>) {
        let added = {
            let mut queue = self.queue.lock().unwrap();
            if queue.len() < Self::MAX_QUEUE && !self.stopped() {
                queue.push_back(vote);
                true
            } else {
                false
            }
        };

        if added {
            self.enqueued.notify_all();
        } else {
            self.stats
                .inc(StatType::VoteRebroadcaster, DetailType::Overfill);
        }
    }

    /// This will wait for a vote to be enqueued or for the
    /// queue to be stopped.
    pub fn dequeue(&self) -> Option<Arc<Vote>> {
        let mut queue = self.queue.lock().unwrap();

        queue = self
            .enqueued
            .wait_while(queue, |q| q.len() == 0 && !self.stopped())
            .unwrap();

        return queue.pop_front();
    }

    pub fn stopped(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
    }

    pub fn stop(&self) {
        {
            let _guard = self.queue.lock().unwrap();
            self.stopped.store(true, Ordering::SeqCst);
        }
        self.enqueued.notify_all();
    }

    pub fn container_info(&self) -> ContainerInfo {
        let queue = self.queue.lock().unwrap();
        [("queue", queue.len(), 0)].into()
    }
}
