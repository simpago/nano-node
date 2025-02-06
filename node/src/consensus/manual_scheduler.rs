use super::{ActiveElections, ActiveElectionsExt, ElectionBehavior};
use crate::stats::{DetailType, StatType, Stats};
use rsnano_core::{utils::ContainerInfo, Amount, Block, BlockHash, SavedBlock};
use std::{
    collections::VecDeque,
    mem::size_of,
    sync::{Arc, Condvar, Mutex},
    thread::JoinHandle,
};

pub struct ManualScheduler {
    thread: Mutex<Option<JoinHandle<()>>>,
    condition: Condvar,
    mutex: Mutex<ManualSchedulerImpl>,
    stats: Arc<Stats>,
    active: Arc<ActiveElections>,
}

impl ManualScheduler {
    pub fn new(stats: Arc<Stats>, active: Arc<ActiveElections>) -> Self {
        Self {
            thread: Mutex::new(None),
            condition: Condvar::new(),
            stats,
            active,
            mutex: Mutex::new(ManualSchedulerImpl {
                queue: Default::default(),
                stopped: false,
            }),
        }
    }

    pub fn stop(&self) {
        self.mutex.lock().unwrap().stopped = true;
        self.notify();
        let handle = self.thread.lock().unwrap().take();
        if let Some(handle) = handle {
            handle.join().unwrap();
        }
    }

    pub fn contains(&self, hash: &BlockHash) -> bool {
        self.mutex
            .lock()
            .unwrap()
            .queue
            .iter()
            .any(|(block, _, _)| block.hash() == *hash)
    }

    pub fn notify(&self) {
        self.condition.notify_all();
    }

    pub fn push(&self, block: SavedBlock, previous_balance: Option<Amount>) {
        {
            let mut guard = self.mutex.lock().unwrap();
            guard
                .queue
                .push_back((block, previous_balance, ElectionBehavior::Manual));
        }
        self.notify();
    }

    fn run(&self) {
        let mut guard = self.mutex.lock().unwrap();
        while !guard.stopped {
            guard = self
                .condition
                .wait_while(guard, |g| !g.stopped && !g.predicate())
                .unwrap();

            if !guard.stopped {
                self.stats
                    .inc(StatType::ElectionScheduler, DetailType::Loop);

                if guard.predicate() {
                    let (block, _previous_balance, election_behavior) =
                        guard.queue.pop_front().unwrap();

                    drop(guard);

                    self.stats
                        .inc(StatType::ElectionScheduler, DetailType::InsertManual);

                    let (_inserted, election) = self.active.insert(block, election_behavior, None);
                    if let Some(election) = election {
                        election.transition_active();
                    }
                } else {
                    drop(guard);
                }
                self.notify();
                guard = self.mutex.lock().unwrap();
            }
        }
    }

    pub fn container_info(&self) -> ContainerInfo {
        let guard = self.mutex.lock().unwrap();
        [(
            "queue",
            guard.queue.len(),
            size_of::<Arc<Block>>() + size_of::<Option<Amount>>() + size_of::<ElectionBehavior>(),
        )]
        .into()
    }
}

impl Drop for ManualScheduler {
    fn drop(&mut self) {
        // Thread must be stopped before destruction
        debug_assert!(self.thread.lock().unwrap().is_none());
    }
}

pub trait ManualSchedulerExt {
    fn start(&self);
}

impl ManualSchedulerExt for Arc<ManualScheduler> {
    fn start(&self) {
        debug_assert!(self.thread.lock().unwrap().is_none());
        let self_l = Arc::clone(self);
        *self.thread.lock().unwrap() = Some(
            std::thread::Builder::new()
                .name("Sched Manual".to_string())
                .spawn(Box::new(move || {
                    self_l.run();
                }))
                .unwrap(),
        )
    }
}

struct ManualSchedulerImpl {
    queue: VecDeque<(SavedBlock, Option<Amount>, ElectionBehavior)>,
    stopped: bool,
}

impl ManualSchedulerImpl {
    fn predicate(&self) -> bool {
        !self.queue.is_empty()
    }
}
