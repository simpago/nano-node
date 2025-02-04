use crate::stats::{DetailType, StatType, Stats};
use rsnano_core::{utils::ContainerInfo, BlockHash, Vote, VoteCode};
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex,
    },
};

pub(crate) struct VoteRebroadcastQueueBuilder {
    stats: Option<Arc<Stats>>,
    block_when_empty: bool,
    max_len: usize,
}

impl VoteRebroadcastQueueBuilder {
    pub fn stats(mut self, stats: Arc<Stats>) -> Self {
        self.stats = Some(stats);
        self
    }

    #[allow(dead_code)]
    pub fn block_when_empty(mut self, block: bool) -> Self {
        self.block_when_empty = block;
        self
    }

    #[allow(dead_code)]
    pub fn max_len(mut self, max: usize) -> Self {
        self.max_len = max;
        self
    }

    pub fn finish(self) -> VoteRebroadcastQueue {
        let stats = self.stats.unwrap_or_default();
        VoteRebroadcastQueue::new(stats, self.block_when_empty, self.max_len)
    }
}

impl Default for VoteRebroadcastQueueBuilder {
    fn default() -> Self {
        Self {
            stats: Default::default(),
            block_when_empty: true,
            max_len: VoteRebroadcastQueue::DEFAULT_MAX_QUEUE,
        }
    }
}

pub(crate) struct VoteRebroadcastQueue {
    queue: Mutex<VecDeque<Arc<Vote>>>,
    enqueued: Condvar,
    stopped: AtomicBool,
    stats: Arc<Stats>,
    block_when_empty: bool,
    max_len: usize,
}

impl VoteRebroadcastQueue {
    const DEFAULT_MAX_QUEUE: usize = 1024 * 16;

    pub fn build() -> VoteRebroadcastQueueBuilder {
        Default::default()
    }

    fn new(stats: Arc<Stats>, block_when_empty: bool, max_queue: usize) -> Self {
        Self {
            queue: Mutex::new(Default::default()),
            enqueued: Condvar::new(),
            stopped: AtomicBool::new(false),
            stats,
            block_when_empty,
            max_len: max_queue,
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

            if self.stopped() {
                return;
            }

            if queue.len() < self.max_len && !self.stopped() {
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
        if queue.len() == 0 && !self.block_when_empty {
            return None;
        }

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
            let mut guard = self.queue.lock().unwrap();
            guard.clear();
            self.stopped.store(true, Ordering::SeqCst);
        }
        self.enqueued.notify_all();
    }

    pub fn container_info(&self) -> ContainerInfo {
        let queue = self.queue.lock().unwrap();
        [("queue", queue.len(), 0)].into()
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.queue.lock().unwrap().len()
    }
}

impl Default for VoteRebroadcastQueue {
    fn default() -> Self {
        Self::build().finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty() {
        let queue = VoteRebroadcastQueue::build().finish();
        assert_eq!(queue.len(), 0);
        assert_eq!(queue.max_len, VoteRebroadcastQueue::DEFAULT_MAX_QUEUE);
    }

    #[test]
    fn enqueue_and_dequeue_a_vote() {
        let queue = VoteRebroadcastQueue::build().finish();
        queue.enqueue(test_vote());
        assert_eq!(queue.len(), 1);

        let dequeued = queue.dequeue();
        assert!(dequeued.is_some());
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn dequeue_waits_when_queue_empty() {
        let queue = VoteRebroadcastQueue::build().finish();
        let notify = Condvar::new();
        let waiting = Mutex::new(false);
        let mut dequeued = None;

        std::thread::scope(|s| {
            // spawn blocking dequeue
            s.spawn(|| {
                *waiting.lock().unwrap() = true;
                notify.notify_one();
                dequeued = queue.dequeue();
            });

            // enqueue when waiting
            {
                let guard = waiting.lock().unwrap();
                drop(notify.wait_while(guard, |i| !*i).unwrap());
            }
            queue.enqueue(test_vote());
        });

        assert!(dequeued.is_some());
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn disable_blocking_dequeue() {
        let queue = VoteRebroadcastQueue::build()
            .block_when_empty(false)
            .finish();

        let result = queue.dequeue();
        assert!(result.is_none());
    }

    #[test]
    fn stop() {
        let queue = VoteRebroadcastQueue::build().finish();
        queue.enqueue(test_vote());

        queue.stop();

        assert!(queue.stopped());
        assert_eq!(queue.len(), 0);
        assert!(queue.dequeue().is_none());

        queue.enqueue(test_vote());
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn max_len() {
        let queue = VoteRebroadcastQueue::build().max_len(2).finish();
        queue.enqueue(test_vote());
        queue.enqueue(test_vote());
        assert_eq!(queue.len(), 2);

        queue.enqueue(test_vote());
        assert_eq!(queue.len(), 2);
    }

    #[test]
    fn container_info() {
        let queue = VoteRebroadcastQueue::build().max_len(2).finish();
        queue.enqueue(test_vote());
        let info = queue.container_info();
        let expected: ContainerInfo = [("queue", 1, 0)].into();
        assert_eq!(info, expected);
    }

    #[test]
    fn ignore_unprocessed_vote() {
        let queue = VoteRebroadcastQueue::build().finish();
        let mut results = HashMap::new();
        results.insert(BlockHash::from(1), VoteCode::Invalid);
        results.insert(BlockHash::from(2), VoteCode::Replay);
        results.insert(BlockHash::from(3), VoteCode::Indeterminate);
        results.insert(BlockHash::from(4), VoteCode::Ignored);

        queue.handle_processed_vote(&test_vote(), &results);

        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn enqueue_processed_vote() {
        let queue = VoteRebroadcastQueue::build().finish();
        let mut results = HashMap::new();
        results.insert(BlockHash::from(1), VoteCode::Invalid);
        results.insert(BlockHash::from(2), VoteCode::Replay);
        results.insert(BlockHash::from(3), VoteCode::Indeterminate);
        results.insert(BlockHash::from(4), VoteCode::Ignored);

        //This means a processed vote:
        results.insert(BlockHash::from(5), VoteCode::Vote);

        queue.handle_processed_vote(&test_vote(), &results);

        assert_eq!(queue.len(), 1);
    }

    fn test_vote() -> Arc<Vote> {
        Arc::new(Vote::new_test_instance())
    }
}
