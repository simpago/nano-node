use super::{VoteProcessorQueue, VoteRouter};
use crate::stats::{DetailType, StatType, Stats};
use rsnano_core::{Vote, VoteCode, VoteSource};
use rsnano_network::Channel;
use std::{
    cmp::{max, min},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread::JoinHandle,
    time::Instant,
};
use tracing::{debug, trace};

#[derive(Clone, Debug, PartialEq)]
pub struct VoteProcessorConfig {
    pub max_pr_queue: usize,
    pub max_non_pr_queue: usize,
    pub pr_priority: usize,
    pub threads: usize,
    pub batch_size: usize,
    pub max_triggered: usize,
}

impl VoteProcessorConfig {
    pub fn new(parallelism: usize) -> Self {
        Self {
            max_pr_queue: 256,
            max_non_pr_queue: 32,
            pr_priority: 3,
            threads: max(1, min(4, parallelism / 2)),
            batch_size: 1024,
            max_triggered: 16384,
        }
    }
}

pub type VoteProcessedCallback2 =
    Box<dyn Fn(&Arc<Vote>, Option<&Arc<Channel>>, VoteSource, VoteCode) + Send + Sync>;

pub struct VoteProcessor {
    threads: Mutex<Vec<JoinHandle<()>>>,
    queue: Arc<VoteProcessorQueue>,
    vote_router: Arc<VoteRouter>,
    stats: Arc<Stats>,
    vote_processed: Mutex<Vec<VoteProcessedCallback2>>,
    pub total_processed: AtomicU64,
}

impl VoteProcessor {
    pub fn new(
        queue: Arc<VoteProcessorQueue>,
        vote_router: Arc<VoteRouter>,
        stats: Arc<Stats>,
        on_vote: VoteProcessedCallback2,
    ) -> Self {
        Self {
            queue,
            vote_router,
            stats,
            vote_processed: Mutex::new(vec![on_vote]),
            threads: Mutex::new(Vec::new()),
            total_processed: AtomicU64::new(0),
        }
    }

    pub fn stop(&self) {
        self.queue.stop();

        let mut handles = Vec::new();
        {
            let mut guard = self.threads.lock().unwrap();
            std::mem::swap(&mut handles, &mut guard);
        }
        for handle in handles {
            handle.join().unwrap()
        }
    }

    pub fn run(&self) {
        loop {
            self.stats.inc(StatType::VoteProcessor, DetailType::Loop);

            let batch = self.queue.wait_for_votes(self.queue.config.batch_size);
            if batch.is_empty() {
                break; //stopped
            }

            let start = Instant::now();

            for (_, (vote, source, channel)) in &batch {
                self.vote_blocking(vote, channel.clone(), *source);
            }

            self.total_processed
                .fetch_add(batch.len() as u64, Ordering::SeqCst);

            let elapsed_millis = start.elapsed().as_millis();
            if batch.len() == self.queue.config.batch_size && elapsed_millis > 100 {
                debug!(
                    "Processed {} votes in {} milliseconds (rate of {} votes per second)",
                    batch.len(),
                    elapsed_millis,
                    (batch.len() * 1000) / elapsed_millis as usize
                );
            }
        }
    }

    pub fn vote_blocking(
        &self,
        vote: &Arc<Vote>,
        channel: Option<Arc<Channel>>,
        source: VoteSource,
    ) -> VoteCode {
        let mut result = VoteCode::Invalid;
        if vote.validate().is_ok() {
            let vote_results = self.vote_router.vote(vote, source);

            // Aggregate results for individual hashes
            let mut replay = false;
            let mut processed = false;
            for (_, hash_result) in vote_results {
                replay |= hash_result == VoteCode::Replay;
                processed |= hash_result == VoteCode::Vote;
            }
            result = if replay {
                VoteCode::Replay
            } else if processed {
                VoteCode::Vote
            } else {
                VoteCode::Indeterminate
            };

            let callbacks = self.vote_processed.lock().unwrap();
            for callback in callbacks.iter() {
                (callback)(vote, channel.as_ref(), source, result);
            }
        }

        self.stats.inc(StatType::Vote, DetailType::VoteProcessed);
        trace!(?vote, ?result, ?source, "vote processed");

        result
    }

    pub fn on_vote_processed(
        &self,
        callback: Box<
            dyn Fn(&Arc<Vote>, Option<&Arc<Channel>>, VoteSource, VoteCode) + Send + Sync,
        >,
    ) {
        self.vote_processed.lock().unwrap().push(callback);
    }
}

impl Drop for VoteProcessor {
    fn drop(&mut self) {
        // Thread must be stopped before destruction
        debug_assert!(self.threads.lock().unwrap().is_empty());
    }
}

pub trait VoteProcessorExt {
    fn start(&self);
}

impl VoteProcessorExt for Arc<VoteProcessor> {
    fn start(&self) {
        let mut threads = self.threads.lock().unwrap();
        debug_assert!(threads.is_empty());
        for _ in 0..self.queue.config.threads {
            let self_l = Arc::clone(self);
            threads.push(
                std::thread::Builder::new()
                    .name("Vote processing".to_string())
                    .spawn(Box::new(move || {
                        self_l.run();
                    }))
                    .unwrap(),
            )
        }
    }
}
