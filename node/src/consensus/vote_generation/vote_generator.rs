use super::{LocalVoteHistory, VoteSpacing};
use crate::{
    consensus::VoteBroadcaster,
    stats::{DetailType, Direction, Sample, StatType, Stats},
    transport::MessageSender,
    utils::ProcessingQueue,
    wallets::Wallets,
};
use rsnano_core::{
    utils::{milliseconds_since_epoch, ContainerInfo},
    BlockHash, Root, SavedBlock, Vote,
};
use rsnano_ledger::{Ledger, Writer};
use rsnano_messages::{ConfirmAck, Message};
use rsnano_network::{Channel, ChannelId, TrafficType};
use rsnano_nullable_clock::SteadyClock;
use rsnano_store_lmdb::{LmdbReadTransaction, LmdbWriteTransaction, Transaction};
use std::{
    collections::VecDeque,
    mem::size_of,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex, MutexGuard,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

pub struct VoteGeneratorRequest {
    pub candidates: Vec<(Root, BlockHash)>,
    pub channel: Arc<Channel>,
}

pub(crate) struct VoteGenerator {
    ledger: Arc<Ledger>,
    vote_generation_queue: ProcessingQueue<(Root, BlockHash)>,
    shared_state: Arc<SharedState>,
    thread: Mutex<Option<JoinHandle<()>>>,
    stats: Arc<Stats>,
}

impl VoteGenerator {
    const MAX_REQUESTS: usize = 2048;
    const MAX_HASHES: usize = 255;

    pub(crate) fn new(
        ledger: Arc<Ledger>,
        wallets: Arc<Wallets>,
        history: Arc<LocalVoteHistory>,
        is_final: bool,
        stats: Arc<Stats>,
        message_sender: MessageSender,
        voting_delay: Duration,
        vote_generator_delay: Duration,
        vote_broadcaster: Arc<VoteBroadcaster>,
        clock: Arc<SteadyClock>,
    ) -> Self {
        let shared_state = Arc::new(SharedState {
            ledger: Arc::clone(&ledger),
            message_sender: Mutex::new(message_sender),
            history,
            wallets,
            condition: Condvar::new(),
            queues: Mutex::new(Queues {
                requests: Default::default(),
                candidates: Default::default(),
                next_broadcast: Instant::now(),
            }),
            is_final,
            stopped: AtomicBool::new(false),
            stats: Arc::clone(&stats),
            vote_broadcaster,
            spacing: Mutex::new(VoteSpacing::new(voting_delay)),
            vote_generator_delay,
            clock,
        });

        let shared_state_clone = Arc::clone(&shared_state);
        Self {
            ledger,
            shared_state,
            thread: Mutex::new(None),
            vote_generation_queue: ProcessingQueue::new(
                Arc::clone(&stats),
                StatType::VoteGenerator,
                Self::thread_name(is_final),
                1,         // single threaded
                1024 * 32, // max queue size
                256,       // max batch size,
                Box::new(move |batch| {
                    shared_state_clone.process_batch(batch);
                }),
            ),
            stats,
        }
    }

    fn thread_name(is_final: bool) -> String {
        if is_final {
            "Voting final".to_owned()
        } else {
            "Voting".to_owned()
        }
    }

    pub(crate) fn start(&self) {
        let shared_state_clone = Arc::clone(&self.shared_state);
        *self.thread.lock().unwrap() = Some(
            thread::Builder::new()
                .name(Self::thread_name(self.shared_state.is_final))
                .spawn(move || shared_state_clone.run())
                .unwrap(),
        );
        self.vote_generation_queue.start();
    }

    pub(crate) fn stop(&self) {
        self.vote_generation_queue.stop();
        {
            let _guard = self.shared_state.queues.lock().unwrap();
            self.shared_state.stopped.store(true, Ordering::SeqCst);
        }
        self.shared_state.condition.notify_all();
        let thread = self.thread.lock().unwrap().take();
        if let Some(thread) = thread {
            thread.join().unwrap();
        }
    }

    /// Queue items for vote generation, or broadcast votes already in cache
    pub(crate) fn add(&self, root: &Root, hash: &BlockHash) {
        self.vote_generation_queue.add((*root, *hash));
    }

    /// Queue blocks for vote generation, returning the number of successful candidates.
    pub(crate) fn generate(&self, blocks: &[SavedBlock], channel: &Arc<Channel>) -> usize {
        let req_candidates = {
            let txn = self.ledger.read_txn();
            blocks
                .iter()
                .filter_map(|i| {
                    if self.ledger.dependents_confirmed(&txn, i) {
                        Some((i.root(), i.hash()))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        };

        let result = req_candidates.len();
        let mut guard = self.shared_state.queues.lock().unwrap();
        let vote_req = VoteGeneratorRequest {
            candidates: req_candidates,
            channel: channel.clone(),
        };
        guard.requests.push_back(vote_req);
        while guard.requests.len() > Self::MAX_REQUESTS {
            // On a large queue of requests, erase the oldest one
            guard.requests.pop_front();
            self.stats.inc(
                StatType::VoteGenerator,
                DetailType::GeneratorRepliesDiscarded,
            );
        }

        result
    }

    pub(crate) fn container_info(&self) -> ContainerInfo {
        let candidates_count;
        let requests_count;
        {
            let guard = self.shared_state.queues.lock().unwrap();
            candidates_count = guard.candidates.len();
            requests_count = guard.requests.len();
        }

        [
            (
                "candidates",
                candidates_count,
                size_of::<Root>() + size_of::<BlockHash>(),
            ),
            (
                "requests",
                requests_count,
                size_of::<ChannelId>() + size_of::<Vec<(Root, BlockHash)>>(),
            ),
        ]
        .into()
    }
}

impl Drop for VoteGenerator {
    fn drop(&mut self) {
        debug_assert!(self.thread.lock().unwrap().is_none())
    }
}

struct SharedState {
    ledger: Arc<Ledger>,
    wallets: Arc<Wallets>,
    history: Arc<LocalVoteHistory>,
    message_sender: Mutex<MessageSender>,
    is_final: bool,
    condition: Condvar,
    stopped: AtomicBool,
    queues: Mutex<Queues>,
    stats: Arc<Stats>,
    vote_broadcaster: Arc<VoteBroadcaster>,
    spacing: Mutex<VoteSpacing>,
    vote_generator_delay: Duration,
    clock: Arc<SteadyClock>,
}

impl SharedState {
    fn run(&self) {
        let mut queues = self.queues.lock().unwrap();
        while !self.stopped.load(Ordering::SeqCst) {
            queues = self
                .condition
                .wait_timeout_while(queues, self.vote_generator_delay, |i| {
                    !self.stopped.load(Ordering::SeqCst)
                        && i.requests.is_empty()
                        && !i.should_broadcast()
                })
                .unwrap()
                .0;

            if self.stopped.load(Ordering::SeqCst) {
                return;
            }

            if queues.should_broadcast() {
                queues = self.broadcast(queues);
                queues.next_broadcast = Instant::now() + self.vote_generator_delay;
            }

            if let Some(request) = queues.requests.pop_front() {
                drop(queues);
                self.reply(request);
                queues = self.queues.lock().unwrap();
            }
        }
    }

    fn broadcast<'a>(&'a self, mut queues: MutexGuard<'a, Queues>) -> MutexGuard<'a, Queues> {
        let mut hashes = Vec::with_capacity(VoteGenerator::MAX_HASHES);
        let mut roots = Vec::with_capacity(VoteGenerator::MAX_HASHES);
        {
            let spacing = self.spacing.lock().unwrap();
            while let Some((root, hash)) = queues.candidates.pop_front() {
                if !roots.contains(&root) {
                    if spacing.votable(&root, &hash, self.clock.now()) {
                        roots.push(root);
                        hashes.push(hash);
                    } else {
                        self.stats
                            .inc(StatType::VoteGenerator, DetailType::GeneratorSpacing);
                    }
                }
                if hashes.len() == VoteGenerator::MAX_HASHES {
                    break;
                }
            }
        }

        if !hashes.is_empty() {
            drop(queues);
            self.vote(&hashes, &roots, |generated_vote| {
                self.stats
                    .inc(StatType::VoteGenerator, DetailType::GeneratorBroadcasts);
                let sample = if self.is_final {
                    Sample::VoteGeneratorFinalHashes
                } else {
                    Sample::VoteGeneratorHashes
                };
                self.stats.sample(
                    sample,
                    generated_vote.hashes.len() as i64,
                    (0, ConfirmAck::HASHES_MAX as i64),
                );
                self.vote_broadcaster.broadcast(generated_vote);
            });
            queues = self.queues.lock().unwrap();
        }

        queues
    }

    fn vote<F>(&self, hashes: &Vec<BlockHash>, roots: &Vec<Root>, action: F)
    where
        F: Fn(Arc<Vote>),
    {
        debug_assert_eq!(hashes.len(), roots.len());
        let mut votes = Vec::new();
        self.wallets.foreach_representative(|keys| {
            let timestamp = if self.is_final {
                Vote::TIMESTAMP_MAX
            } else {
                milliseconds_since_epoch()
            };
            let duration = if self.is_final {
                Vote::DURATION_MAX
            } else {
                0x9 /*8192ms*/
            };
            votes.push(Arc::new(Vote::new(
                keys,
                timestamp,
                duration,
                hashes.clone(),
            )));
        });

        for vote in votes {
            {
                let mut spacing = self.spacing.lock().unwrap();
                let now = self.clock.now();
                for i in 0..hashes.len() {
                    self.history.add(&roots[i], &hashes[i], &vote);
                    spacing.flag(&roots[i], &hashes[i], now);
                }
            }
            action(vote);
        }
    }

    fn reply(&self, request: VoteGeneratorRequest) {
        let mut i = request.candidates.iter().peekable();
        while i.peek().is_some() && !self.stopped.load(Ordering::SeqCst) {
            let mut hashes = Vec::with_capacity(VoteGenerator::MAX_HASHES);
            let mut roots = Vec::with_capacity(VoteGenerator::MAX_HASHES);
            {
                let spacing = self.spacing.lock().unwrap();
                while hashes.len() < VoteGenerator::MAX_HASHES {
                    let Some((root, hash)) = i.next() else {
                        break;
                    };
                    if !roots.contains(root) {
                        if spacing.votable(root, hash, self.clock.now()) {
                            roots.push(*root);
                            hashes.push(*hash);
                        } else {
                            self.stats
                                .inc(StatType::VoteGenerator, DetailType::GeneratorSpacing);
                        }
                    }
                }
            }
            if !hashes.is_empty() {
                self.stats.add_dir(
                    StatType::Requests,
                    DetailType::RequestsGeneratedHashes,
                    Direction::In,
                    hashes.len() as u64,
                );
                self.vote(&hashes, &roots, |vote| {
                    let confirm =
                        Message::ConfirmAck(ConfirmAck::new_with_own_vote((*vote).clone()));
                    self.message_sender.lock().unwrap().try_send(
                        &request.channel,
                        &confirm,
                        TrafficType::Vote,
                    );
                    self.stats.inc_dir(
                        StatType::Requests,
                        DetailType::RequestsGeneratedVotes,
                        Direction::In,
                    );
                });
            }
        }
        self.stats
            .inc(StatType::VoteGenerator, DetailType::GeneratorReplies);
    }

    fn process_batch(&self, batch: VecDeque<(Root, BlockHash)>) {
        let mut verified = VecDeque::new();

        if self.is_final {
            let mut write_guard = self.ledger.write_queue.wait(Writer::VotingFinal);
            let mut tx = self.ledger.rw_txn();
            for (root, hash) in &batch {
                (write_guard, tx) = self.ledger.refresh_if_needed(write_guard, tx);
                if self.should_vote_final(&mut tx, root, hash) {
                    verified.push_back((*root, *hash));
                }
            }
        } else {
            let mut tx = self.ledger.read_txn();
            for (root, hash) in &batch {
                tx.refresh_if_needed();
                if self.should_vote_non_final(&tx, root, hash) {
                    verified.push_back((*root, *hash));
                }
            }
        };

        // Submit verified candidates to the main processing thread
        if !verified.is_empty() {
            let should_notify = {
                let mut queues = self.queues.lock().unwrap();
                queues.candidates.extend(verified);
                queues.candidates.len() >= VoteGenerator::MAX_HASHES
            };

            if should_notify {
                self.condition.notify_all();
            }
        }
    }

    fn should_vote_non_final(
        &self,
        txn: &LmdbReadTransaction,
        root: &Root,
        hash: &BlockHash,
    ) -> bool {
        let Some(block) = self.ledger.any().get_block(txn, hash) else {
            return false;
        };
        debug_assert!(block.root() == *root);
        self.ledger.dependents_confirmed(txn, &block)
    }

    fn should_vote_final(
        &self,
        txn: &mut LmdbWriteTransaction,
        root: &Root,
        hash: &BlockHash,
    ) -> bool {
        let Some(block) = self.ledger.any().get_block(txn, hash) else {
            return false;
        };
        debug_assert!(block.root() == *root);
        self.ledger.dependents_confirmed(txn, &block)
            && self
                .ledger
                .store
                .final_vote
                .put(txn, &block.qualified_root(), hash)
    }
}

struct Queues {
    candidates: VecDeque<(Root, BlockHash)>,
    requests: VecDeque<VoteGeneratorRequest>,
    next_broadcast: Instant,
}

impl Queues {
    fn should_broadcast(&self) -> bool {
        if self.candidates.len() >= ConfirmAck::HASHES_MAX {
            return true;
        }

        !self.candidates.is_empty() && Instant::now() >= self.next_broadcast
    }
}
