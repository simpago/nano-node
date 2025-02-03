use crate::stats::{DetailType, StatType, Stats};
use rsnano_core::{utils::ContainerInfo, BlockHash, Vote, VoteCode};
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex,
    },
};

pub(crate) struct VoteRebroadcastQueue {
    queue: Mutex<VecDeque<Arc<Vote>>>,
    enqueued: Condvar,
    stopped: AtomicBool,
    stats: Arc<Stats>,
}

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
