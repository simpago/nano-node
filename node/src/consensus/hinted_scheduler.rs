use super::{ActiveElections, ElectionBehavior, VoteCache};
use crate::{
    cementation::ConfirmingSet,
    consensus::ActiveElectionsExt,
    representatives::OnlineReps,
    stats::{DetailType, StatType, Stats},
};
use rsnano_core::{utils::ContainerInfo, Amount, BlockHash};
use rsnano_ledger::Ledger;
use rsnano_store_lmdb::{LmdbReadTransaction, Transaction};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    mem::size_of,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex,
    },
    thread::JoinHandle,
    time::{Duration, Instant},
};

#[derive(Clone, Debug, PartialEq)]
pub struct HintedSchedulerConfig {
    pub check_interval: Duration,
    pub block_cooldown: Duration,
    pub hinting_threshold_percent: u32,
    pub vacancy_threshold_percent: u32,
}

impl HintedSchedulerConfig {
    pub fn default_for_dev_network() -> Self {
        Self {
            check_interval: Duration::from_millis(100),
            block_cooldown: Duration::from_millis(100),
            ..Default::default()
        }
    }
}

impl Default for HintedSchedulerConfig {
    fn default() -> Self {
        Self {
            check_interval: Duration::from_millis(1000),
            block_cooldown: Duration::from_millis(5000),
            hinting_threshold_percent: 10,
            vacancy_threshold_percent: 20,
        }
    }
}

/// Monitors inactive vote cache and schedules elections with the highest observed vote tally.
pub struct HintedScheduler {
    thread: Mutex<Option<JoinHandle<()>>>,
    config: HintedSchedulerConfig,
    active: Arc<ActiveElections>,
    condition: Condvar,
    ledger: Arc<Ledger>,
    confirming_set: Arc<ConfirmingSet>,
    stats: Arc<Stats>,
    vote_cache: Arc<Mutex<VoteCache>>,
    online_reps: Arc<Mutex<OnlineReps>>,
    stopped: AtomicBool,
    stopped_mutex: Mutex<()>,
    cooldowns: Mutex<OrderedCooldowns>,
}

impl HintedScheduler {
    pub fn new(
        config: HintedSchedulerConfig,
        active: Arc<ActiveElections>,
        ledger: Arc<Ledger>,
        stats: Arc<Stats>,
        vote_cache: Arc<Mutex<VoteCache>>,
        confirming_set: Arc<ConfirmingSet>,
        online_reps: Arc<Mutex<OnlineReps>>,
    ) -> Self {
        Self {
            thread: Mutex::new(None),
            config,
            condition: Condvar::new(),
            active,
            ledger,
            stats,
            vote_cache,
            confirming_set,
            online_reps,
            stopped: AtomicBool::new(false),
            stopped_mutex: Mutex::new(()),
            cooldowns: Mutex::new(OrderedCooldowns::new()),
        }
    }

    pub fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
        self.notify();
        let handle = self.thread.lock().unwrap().take();
        if let Some(handle) = handle {
            handle.join().unwrap();
        }
    }

    /// Notify about changes in AEC vacancy
    pub fn notify(&self) {
        // Avoid notifying when there is very little space inside AEC
        let limit = self.active.limit(ElectionBehavior::Hinted);
        if self.active.vacancy(ElectionBehavior::Hinted)
            >= (limit * self.config.vacancy_threshold_percent as usize / 100) as i64
        {
            self.condition.notify_all();
        }
    }

    pub fn container_info(&self) -> ContainerInfo {
        let guard = self.cooldowns.lock().unwrap();
        [(
            "cooldowns",
            guard.len(),
            (size_of::<BlockHash>() + size_of::<Instant>()) * 2,
        )]
        .into()
    }

    fn predicate(&self) -> bool {
        // Check if there is space inside AEC for a new hinted election
        self.active.vacancy(ElectionBehavior::Hinted) > 0
    }

    fn activate(&self, tx: &mut LmdbReadTransaction, hash: BlockHash, check_dependents: bool) {
        const MAX_ITERATIONS: usize = 64;
        let mut visited = HashSet::new();
        let mut stack = Vec::new();
        stack.push(hash);
        let mut iterations = 0;
        while let Some(current_hash) = stack.pop() {
            if iterations >= MAX_ITERATIONS {
                break;
            }
            iterations += 1;
            tx.refresh_if_needed();

            // Check if block exists
            if let Some(block) = self.ledger.any().get_block(tx, &current_hash) {
                // Ensure block is not already confirmed
                if self.confirming_set.contains(&current_hash)
                    || self
                        .ledger
                        .confirmed()
                        .block_exists_or_pruned(tx, &current_hash)
                {
                    self.stats
                        .inc(StatType::Hinting, DetailType::AlreadyConfirmed);
                    self.vote_cache.lock().unwrap().erase(&current_hash); // Remove from vote cache
                    continue; // Move on to the next item in the stack
                }

                if check_dependents {
                    // Perform a depth-first search of the dependency graph
                    if !self.ledger.dependents_confirmed(tx, &block) {
                        self.stats
                            .inc(StatType::Hinting, DetailType::DependentUnconfirmed);
                        let dependents = self.ledger.dependent_blocks(tx, &block);
                        for dependent_hash in dependents.iter() {
                            // Avoid visiting the same block twice
                            if !dependent_hash.is_zero() && visited.insert(*dependent_hash) {
                                stack.push(*dependent_hash); // Add dependent block to the stack
                            }
                        }
                        continue; // Move on to the next item in the stack
                    }
                }

                // Try to insert it into AEC as hinted election
                let (inserted, _) = self.active.insert(block, ElectionBehavior::Hinted, None);
                self.stats.inc(
                    StatType::Hinting,
                    if inserted {
                        DetailType::Insert
                    } else {
                        DetailType::InsertFailed
                    },
                );
            } else {
                self.stats.inc(StatType::Hinting, DetailType::MissingBlock);

                // TODO: Block is missing, bootstrap it
            }
        }
    }

    fn run_interactive(&self) {
        let minimum_tally = self.tally_threshold();
        let minimum_final_tally = self.final_tally_threshold();

        // Get the list before db transaction starts to avoid unnecessary slowdowns
        let tops = self.vote_cache.lock().unwrap().top(minimum_tally);

        let mut tx = self.ledger.read_txn();

        for entry in tops {
            if self.stopped.load(Ordering::SeqCst) {
                return;
            }

            if !self.predicate() {
                return;
            }

            if self.cooldown(entry.hash) {
                continue;
            }

            // Check dependents only if cached tally is lower than quorum
            if entry.final_tally < minimum_final_tally {
                // Ensure all dependent blocks are already confirmed before activating
                self.stats.inc(StatType::Hinting, DetailType::Activate);
                self.activate(&mut tx, entry.hash, /* activate dependents */ true);
            } else {
                // Blocks with a vote tally higher than quorum, can be activated and confirmed immediately
                self.stats
                    .inc(StatType::Hinting, DetailType::ActivateImmediate);
                self.activate(&mut tx, entry.hash, false);
            }
        }
    }

    fn run(&self) {
        let mut guard = self.stopped_mutex.lock().unwrap();
        while !self.stopped.load(Ordering::SeqCst) {
            self.stats.inc(StatType::Hinting, DetailType::Loop);
            guard = self
                .condition
                .wait_timeout_while(guard, self.config.check_interval, |_| {
                    !self.stopped.load(Ordering::SeqCst)
                })
                .unwrap()
                .0;
            if !self.stopped.load(Ordering::SeqCst) {
                drop(guard);
                if self.predicate() {
                    self.run_interactive()
                }
                guard = self.stopped_mutex.lock().unwrap();
            }
        }
    }

    fn tally_threshold(&self) -> Amount {
        (self.online_reps.lock().unwrap().trended_or_minimum_weight() / 100)
            * self.config.hinting_threshold_percent as u128
    }

    fn final_tally_threshold(&self) -> Amount {
        self.online_reps.lock().unwrap().quorum_delta()
    }

    fn cooldown(&self, hash: BlockHash) -> bool {
        let mut guard = self.cooldowns.lock().unwrap();
        let now = Instant::now();
        // Check if the hash is still in the cooldown period using the hashed index
        if let Some(timeout) = guard.get(&hash) {
            if *timeout > now {
                return true; // Needs cooldown
            }
            guard.remove(&hash); // Entry is outdated, so remove it
        }

        // Insert the new entry
        guard.insert(hash, now + self.config.block_cooldown);

        // Trim old entries
        guard.trim(now);
        false // No need to cooldown
    }
}

impl Drop for HintedScheduler {
    fn drop(&mut self) {
        // Thread must be stopped before destruction
        debug_assert!(self.thread.lock().unwrap().is_none());
    }
}

pub trait HintedSchedulerExt {
    fn start(&self);
}

impl HintedSchedulerExt for Arc<HintedScheduler> {
    fn start(&self) {
        debug_assert!(self.thread.lock().unwrap().is_none());
        let self_l = Arc::clone(self);
        *self.thread.lock().unwrap() = Some(
            std::thread::Builder::new()
                .name("Sched Hinted".to_string())
                .spawn(Box::new(move || {
                    self_l.run();
                }))
                .unwrap(),
        );
    }
}

struct OrderedCooldowns {
    by_hash: HashMap<BlockHash, Instant>,
    by_time: BTreeMap<Instant, Vec<BlockHash>>,
}

impl OrderedCooldowns {
    fn new() -> Self {
        Self {
            by_hash: HashMap::new(),
            by_time: BTreeMap::new(),
        }
    }
    fn insert(&mut self, hash: BlockHash, timeout: Instant) {
        if let Some(old_timeout) = self.by_hash.insert(hash, timeout) {
            self.remove_timeout_entry(&hash, old_timeout);
        }
        self.by_time.entry(timeout).or_default().push(hash);
    }

    fn get(&self, hash: &BlockHash) -> Option<&Instant> {
        self.by_hash.get(hash)
    }

    fn remove(&mut self, hash: &BlockHash) {
        if let Some(timeout) = self.by_hash.remove(hash) {
            self.remove_timeout_entry(hash, timeout);
        }
    }

    fn remove_timeout_entry(&mut self, hash: &BlockHash, timeout: Instant) {
        if let Some(hashes) = self.by_time.get_mut(&timeout) {
            if hashes.len() == 1 {
                self.by_time.remove(&timeout);
            } else {
                hashes.retain(|h| h != hash)
            }
        }
    }

    fn trim(&mut self, now: Instant) {
        while let Some(entry) = self.by_time.first_entry() {
            if *entry.key() <= now {
                let hashes = entry.remove();
                for hash in hashes {
                    self.by_hash.remove(&hash);
                }
            } else {
                break;
            }
        }
    }

    fn len(&self) -> usize {
        self.by_hash.len()
    }
}
