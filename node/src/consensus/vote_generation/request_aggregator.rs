use super::{
    request_aggregator_impl::{AggregateResult, RequestAggregatorImpl},
    VoteGenerators,
};
use crate::stats::{DetailType, Direction, StatType, Stats};
use rsnano_core::{
    utils::{ContainerInfo, FairQueue},
    BlockHash, Root,
};
use rsnano_ledger::Ledger;
use rsnano_network::{Channel, ChannelId, DeadChannelCleanupStep, TrafficType};
use rsnano_store_lmdb::{LmdbReadTransaction, Transaction};
use std::{
    cmp::{max, min},
    sync::{Arc, Condvar, Mutex, MutexGuard},
    thread::JoinHandle,
};

#[derive(Clone, Debug, PartialEq)]
pub struct RequestAggregatorConfig {
    pub threads: usize,
    pub max_queue: usize,
    pub batch_size: usize,
}

impl RequestAggregatorConfig {
    pub fn new(parallelism: usize) -> Self {
        Self {
            threads: max(1, min(parallelism / 2, 4)),
            max_queue: 128,
            batch_size: 16,
        }
    }
}

/**
 * Pools together confirmation requests, separately for each endpoint.
 * Requests are added from network messages, and aggregated to minimize bandwidth and vote generation. Example:
 * * Two votes are cached, one for hashes {1,2,3} and another for hashes {4,5,6}
 * * A request arrives for hashes {1,4,5}. Another request arrives soon afterwards for hashes {2,3,6}
 * * The aggregator will reply with the two cached votes
 * Votes are generated for uncached hashes.
 */
pub struct RequestAggregator {
    config: RequestAggregatorConfig,
    stats: Arc<Stats>,
    vote_generators: Arc<VoteGenerators>,
    ledger: Arc<Ledger>,
    pub(crate) state: Arc<Mutex<RequestAggregatorState>>,
    condition: Arc<Condvar>,
    threads: Mutex<Vec<JoinHandle<()>>>,
}

impl RequestAggregator {
    pub fn new(
        config: RequestAggregatorConfig,
        stats: Arc<Stats>,
        vote_generators: Arc<VoteGenerators>,
        ledger: Arc<Ledger>,
    ) -> Self {
        let max_queue = config.max_queue;
        Self {
            stats,
            vote_generators,
            ledger,
            config,
            condition: Arc::new(Condvar::new()),
            state: Arc::new(Mutex::new(RequestAggregatorState {
                queue: FairQueue::new(move |_| max_queue, |_| 1),
                stopped: false,
            })),
            threads: Mutex::new(Vec::new()),
        }
    }

    pub fn start(&self) {
        let mut guard = self.threads.lock().unwrap();
        for _ in 0..self.config.threads {
            let aggregator_loop = RequestAggregatorLoop {
                mutex: self.state.clone(),
                condition: self.condition.clone(),
                stats: self.stats.clone(),
                config: self.config.clone(),
                ledger: self.ledger.clone(),
                vote_generators: self.vote_generators.clone(),
            };

            guard.push(
                std::thread::Builder::new()
                    .name("Req aggregator".to_string())
                    .spawn(move || aggregator_loop.run())
                    .unwrap(),
            );
        }
    }

    pub fn request(&self, request: AggregatorRequest) -> bool {
        if request.roots_hashes.is_empty() {
            return false;
        }

        let request_len = request.roots_hashes.len();

        let added = {
            self.state
                .lock()
                .unwrap()
                .queue
                .push(request.channel.channel_id(), request)
        };

        if added {
            self.stats
                .inc(StatType::RequestAggregator, DetailType::Request);
            self.stats.add(
                StatType::RequestAggregator,
                DetailType::RequestHashes,
                request_len as u64,
            );
            self.condition.notify_one();
        } else {
            self.stats
                .inc(StatType::RequestAggregator, DetailType::Overfill);
            self.stats.add(
                StatType::RequestAggregator,
                DetailType::OverfillHashes,
                request_len as u64,
            );
        }

        // TODO: This stat is for compatibility with existing tests and is in principle unnecessary
        self.stats.inc(
            StatType::Aggregator,
            if added {
                DetailType::AggregatorAccepted
            } else {
                DetailType::AggregatorDropped
            },
        );

        added
    }

    pub fn stop(&self) {
        self.state.lock().unwrap().stopped = true;
        self.condition.notify_all();
        let mut threads = Vec::new();
        {
            let mut guard = self.threads.lock().unwrap();
            std::mem::swap(&mut threads, &mut *guard);
        }
        for thread in threads {
            thread.join().unwrap();
        }
    }

    /// Returns the number of currently queued request pools
    pub fn len(&self) -> usize {
        self.state.lock().unwrap().queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn container_info(&self) -> ContainerInfo {
        let guard = self.state.lock().unwrap();
        ContainerInfo::builder()
            .node("queue", guard.queue.container_info())
            .finish()
    }
}

impl Drop for RequestAggregator {
    fn drop(&mut self) {
        debug_assert!(self.threads.lock().unwrap().is_empty())
    }
}

#[derive(Clone)]
pub struct AggregatorRequest {
    pub channel: Arc<Channel>,
    pub roots_hashes: Vec<(BlockHash, Root)>,
}

pub(crate) struct RequestAggregatorState {
    queue: FairQueue<ChannelId, AggregatorRequest>,
    stopped: bool,
}

struct RequestAggregatorLoop {
    mutex: Arc<Mutex<RequestAggregatorState>>,
    condition: Arc<Condvar>,
    stats: Arc<Stats>,
    config: RequestAggregatorConfig,
    ledger: Arc<Ledger>,
    vote_generators: Arc<VoteGenerators>,
}

impl RequestAggregatorLoop {
    fn run(&self) {
        let mut guard = self.mutex.lock().unwrap();
        while !guard.stopped {
            if !guard.queue.is_empty() {
                guard = self.run_batch(guard);
            } else {
                guard = self
                    .condition
                    .wait_while(guard, |g| !g.stopped && g.queue.is_empty())
                    .unwrap();
            }
        }
    }

    fn run_batch<'a>(
        &'a self,
        mut state: MutexGuard<'a, RequestAggregatorState>,
    ) -> MutexGuard<'a, RequestAggregatorState> {
        let batch = state.queue.next_batch(self.config.batch_size);
        drop(state);

        let mut tx = self.ledger.read_txn();

        for (_, request) in &batch {
            tx.refresh_if_needed();

            let should_drop = request.channel.should_drop(TrafficType::VoteReply);

            if !should_drop {
                self.process(&tx, request);
            } else {
                self.stats.inc_dir(
                    StatType::RequestAggregator,
                    DetailType::ChannelFull,
                    Direction::Out,
                );
            }
        }

        self.mutex.lock().unwrap()
    }

    fn process(&self, tx: &LmdbReadTransaction, request: &AggregatorRequest) {
        let remaining = self.aggregate(tx, request);

        if !remaining.remaining_normal.is_empty() {
            self.stats
                .inc(StatType::RequestAggregatorReplies, DetailType::NormalVote);

            // Generate votes for the remaining hashes
            let generated = self
                .vote_generators
                .generate_non_final_votes(&remaining.remaining_normal, &request.channel);
            self.stats.add_dir(
                StatType::Requests,
                DetailType::RequestsCannotVote,
                Direction::In,
                (remaining.remaining_normal.len() - generated) as u64,
            );
        }

        if !remaining.remaining_final.is_empty() {
            self.stats
                .inc(StatType::RequestAggregatorReplies, DetailType::FinalVote);

            // Generate final votes for the remaining hashes
            let generated = self
                .vote_generators
                .generate_final_votes(&remaining.remaining_final, &request.channel);
            self.stats.add_dir(
                StatType::Requests,
                DetailType::RequestsCannotVote,
                Direction::In,
                (remaining.remaining_final.len() - generated) as u64,
            );
        }
    }

    /// Aggregate requests and send cached votes to channel.
    /// Return the remaining hashes that need vote generation for each block for regular & final vote generators
    fn aggregate(&self, tx: &LmdbReadTransaction, requests: &AggregatorRequest) -> AggregateResult {
        let mut aggregator = RequestAggregatorImpl::new(&self.ledger, &self.stats, tx);
        aggregator.add_votes(&requests.roots_hashes);
        aggregator.get_result()
    }
}

pub(crate) struct RequestAggregatorCleanup {
    state: Arc<Mutex<RequestAggregatorState>>,
}

impl RequestAggregatorCleanup {
    pub(crate) fn new(state: Arc<Mutex<RequestAggregatorState>>) -> Self {
        Self { state }
    }
}

impl DeadChannelCleanupStep for RequestAggregatorCleanup {
    fn clean_up_dead_channels(&self, dead_channel_ids: &[ChannelId]) {
        let mut guard = self.state.lock().unwrap();
        for channel_id in dead_channel_ids {
            guard.queue.remove(channel_id);
        }
    }
}
