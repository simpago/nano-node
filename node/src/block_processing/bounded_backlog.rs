use super::{
    backlog_index::{BacklogEntry, BacklogIndex},
    backlog_scan::ActivatedInfo,
    BlockContext, LedgerNotifications,
};
use crate::{
    consensus::Bucketing,
    stats::{DetailType, StatType, Stats},
    utils::{ThreadPool, ThreadPoolImpl},
};
use rsnano_core::{
    utils::ContainerInfo, Account, AccountInfo, BlockHash, ConfirmationHeightInfo, SavedBlock,
};
use rsnano_ledger::{BlockStatus, Ledger, Writer};
use rsnano_network::bandwidth_limiter::RateLimiter;
use rsnano_store_lmdb::{LmdbReadTransaction, Transaction};
use std::{
    cmp::min,
    sync::{Arc, Condvar, Mutex, RwLock},
    thread::JoinHandle,
    time::Duration,
};
use tracing::debug;

#[derive(Clone, Debug, PartialEq)]
pub struct BoundedBacklogConfig {
    pub max_backlog: usize,
    pub batch_size: usize,
    pub max_queued_notifications: usize,
    pub scan_rate: usize,
}

impl Default for BoundedBacklogConfig {
    fn default() -> Self {
        Self {
            max_backlog: 100_000,
            batch_size: 32,
            max_queued_notifications: 128,
            scan_rate: 64,
        }
    }
}

pub struct BoundedBacklog {
    thread: Mutex<Option<JoinHandle<()>>>,
    scan_thread: Mutex<Option<JoinHandle<()>>>,
    backlog_impl: Arc<BoundedBacklogImpl>,
    bucketing: Bucketing,
}

impl BoundedBacklog {
    pub fn new(
        bucketing: Bucketing,
        config: BoundedBacklogConfig,
        ledger: Arc<Ledger>,
        notifications: Arc<LedgerNotifications>,
        stats: Arc<Stats>,
    ) -> Self {
        let backlog_impl = Arc::new(BoundedBacklogImpl {
            condition: Condvar::new(),
            mutex: Mutex::new(BacklogData {
                stopped: false,
                index: BacklogIndex::new(bucketing.bucket_count()),
                ledger: ledger.clone(),
                config: config.clone(),
                bucket_count: bucketing.bucket_count(),
                scan_limiter: RateLimiter::new(config.scan_rate),
            }),
            workers: ThreadPoolImpl::create(1, "Bounded b notif".to_string()),
            config,
            stats,
            ledger,
            notifications,
            can_rollback: RwLock::new(Box::new(|_| true)),
        });

        Self {
            backlog_impl,
            thread: Mutex::new(None),
            scan_thread: Mutex::new(None),
            bucketing,
        }
    }

    pub fn start(&self) {
        debug_assert!(self.thread.lock().unwrap().is_none());

        let backlog_impl = self.backlog_impl.clone();
        let handle = std::thread::Builder::new()
            .name("Bounded backlog".to_owned())
            .spawn(move || backlog_impl.run())
            .unwrap();
        *self.thread.lock().unwrap() = Some(handle);

        let backlog_impl = self.backlog_impl.clone();
        let handle = std::thread::Builder::new()
            .name("Bounded b scan".to_owned())
            .spawn(move || backlog_impl.run_scan())
            .unwrap();
        *self.scan_thread.lock().unwrap() = Some(handle);
    }

    pub fn stop(&self) {
        self.backlog_impl.mutex.lock().unwrap().stopped = true;
        self.backlog_impl.condition.notify_all();

        let handle = self.thread.lock().unwrap().take();
        if let Some(handle) = handle {
            handle.join().unwrap();
        }

        let handle = self.scan_thread.lock().unwrap().take();
        if let Some(handle) = handle {
            handle.join().unwrap();
        }
        self.backlog_impl.workers.stop();
    }

    // Give other components a chance to veto a rollback
    pub fn on_rolling_back(&self, f: impl Fn(&BlockHash) -> bool + Send + Sync + 'static) {
        *self.backlog_impl.can_rollback.write().unwrap() = Box::new(f);
    }

    pub fn activate_batch(&self, batch: &[ActivatedInfo]) {
        let mut tx = self.backlog_impl.ledger.read_txn();
        for info in batch {
            self.activate(&mut tx, &info.account, &info.account_info, &info.conf_info);
        }
    }

    pub fn insert_batch(&self, batch: &[(BlockStatus, Arc<BlockContext>)]) {
        let tx = self.backlog_impl.ledger.read_txn();
        for (result, context) in batch {
            if *result == BlockStatus::Progress {
                if let Some(block) = context.saved_block.lock().unwrap().clone() {
                    self.insert(&tx, &block);
                }
            }
        }
    }

    pub fn erase_accounts(&self, accounts: impl IntoIterator<Item = Account>) {
        let mut guard = self.backlog_impl.mutex.lock().unwrap();
        for account in accounts.into_iter() {
            guard.index.erase_account(&account);
        }
    }

    pub fn erase_hashes(&self, accounts: impl IntoIterator<Item = BlockHash>) {
        let mut guard = self.backlog_impl.mutex.lock().unwrap();
        for account in accounts.into_iter() {
            guard.index.erase_hash(&account);
        }
    }

    fn contains(&self, hash: &BlockHash) -> bool {
        let guard = self.backlog_impl.mutex.lock().unwrap();
        guard.index.contains(hash)
    }

    fn activate(
        &self,
        tx: &mut LmdbReadTransaction,
        _account: &Account,
        account_info: &AccountInfo,
        conf_info: &ConfirmationHeightInfo,
    ) {
        debug_assert!(conf_info.frontier != account_info.head);

        // Insert blocks into the index starting from the account head block
        let mut block = self
            .backlog_impl
            .ledger
            .any()
            .get_block(tx, &account_info.head);

        while let Some(blk) = block {
            // We reached the confirmed frontier, no need to track more blocks
            if blk.hash() == conf_info.frontier {
                break;
            }

            // Check if the block is already in the backlog, avoids unnecessary ledger lookups
            if self.contains(&blk.hash()) {
                break;
            }

            let inserted = self.insert(tx, &blk);

            // If the block was not inserted, we already have it in the backlog
            if !inserted {
                break;
            }

            tx.refresh_if_needed();

            block = self
                .backlog_impl
                .ledger
                .any()
                .get_block(tx, &blk.previous());
        }
    }

    pub fn insert(&self, tx: &LmdbReadTransaction, block: &SavedBlock) -> bool {
        let (priority_balance, priority_timestamp) =
            self.backlog_impl.ledger.block_priority(tx, block);
        let bucket_index = self.bucketing.bucket_index(priority_balance);

        self.backlog_impl
            .mutex
            .lock()
            .unwrap()
            .index
            .insert(BacklogEntry {
                hash: block.hash(),
                account: block.account(),
                bucket_index,
                priority: priority_timestamp,
            })
    }

    pub fn container_info(&self) -> ContainerInfo {
        let guard = self.backlog_impl.mutex.lock().unwrap();
        ContainerInfo::builder()
            .leaf("backlog", guard.index.len(), 0)
            .node("index", guard.index.container_info())
            .finish()
    }
}

impl Drop for BoundedBacklog {
    fn drop(&mut self) {
        // Thread must be stopped before destruction
        debug_assert!(self.thread.lock().unwrap().is_none());
        debug_assert!(self.scan_thread.lock().unwrap().is_none());
    }
}

struct BoundedBacklogImpl {
    mutex: Mutex<BacklogData>,
    condition: Condvar,
    workers: ThreadPoolImpl,
    config: BoundedBacklogConfig,
    stats: Arc<Stats>,
    ledger: Arc<Ledger>,
    notifications: Arc<LedgerNotifications>,
    can_rollback: RwLock<Box<dyn Fn(&BlockHash) -> bool + Send + Sync>>,
}

impl BoundedBacklogImpl {
    fn run(&self) {
        let mut guard = self.mutex.lock().unwrap();
        while !guard.stopped {
            guard = self
                .condition
                .wait_timeout_while(guard, Duration::from_secs(1), |i| {
                    !i.stopped && !i.predicate()
                })
                .unwrap()
                .0;

            if guard.stopped {
                return;
            }

            // Wait until all notification about the previous rollbacks are processed
            while self.workers.num_queued_tasks() >= self.config.max_queued_notifications {
                self.stats
                    .inc(StatType::BoundedBacklog, DetailType::Cooldown);
                guard = self
                    .condition
                    .wait_timeout_while(guard, Duration::from_millis(100), |i| !i.stopped)
                    .unwrap()
                    .0;
                if guard.stopped {
                    return;
                }
            }

            self.stats.inc(StatType::BoundedBacklog, DetailType::Loop);

            // Calculate the number of targets to rollback
            let backlog = self.ledger.backlog_count() as usize;

            let target_count = if backlog > self.config.max_backlog {
                backlog - self.config.max_backlog
            } else {
                0
            };

            let targets = guard.gather_targets(
                min(target_count, self.config.batch_size),
                &*self.can_rollback.read().unwrap(),
            );

            if !targets.is_empty() {
                drop(guard);
                self.stats.add(
                    StatType::BoundedBacklog,
                    DetailType::GatheredTargets,
                    targets.len() as u64,
                );
                let processed = self.perform_rollbacks(&targets, target_count);
                guard = self.mutex.lock().unwrap();

                // Erase rolled back blocks from the index
                for hash in &processed {
                    guard.index.erase_hash(hash);
                }
            } else {
                // Cooldown, this should not happen in normal operation
                self.stats
                    .inc(StatType::BoundedBacklog, DetailType::NoTargets);
                guard = self
                    .condition
                    .wait_timeout_while(guard, Duration::from_millis(100), |i| !i.stopped)
                    .unwrap()
                    .0;
            }
        }
    }

    fn perform_rollbacks(&self, targets: &[BlockHash], max_rollbacks: usize) -> Vec<BlockHash> {
        self.stats
            .inc(StatType::BoundedBacklog, DetailType::PerformingRollbacks);

        let _guard = self.ledger.write_queue.wait(Writer::BoundedBacklog);
        let mut tx = self.ledger.rw_txn();

        let mut processed = Vec::new();
        for hash in targets {
            // Skip the rollback if the block is being used by the node, this should be race free as it's checked while holding the ledger write lock
            if !(self.can_rollback.read().unwrap())(hash) {
                self.stats
                    .inc(StatType::BoundedBacklog, DetailType::RollbackSkipped);
                continue;
            }

            // Here we check that the block is still OK to rollback, there could be a delay between gathering the targets and performing the rollbacks
            if let Some(block) = self.ledger.any().get_block(&tx, hash) {
                debug!(
                    "Rolling back: {}, account: {}",
                    hash,
                    block.account().encode_account()
                );

                let rollback_list = match self.ledger.rollback(&mut tx, &block.hash()) {
                    Ok(rollback_list) => {
                        self.stats
                            .inc(StatType::BoundedBacklog, DetailType::Rollback);
                        rollback_list
                    }
                    Err((_, rollback_list)) => {
                        self.stats
                            .inc(StatType::BoundedBacklog, DetailType::RollbackFailed);
                        rollback_list
                    }
                };

                for rollback in &rollback_list {
                    processed.push(rollback.hash());
                }

                // Notify observers of the rolled back blocks on a background thread, avoid dispatching notifications when holding ledger write transaction
                let notifications = self.notifications.clone();
                self.workers.post(Box::new(move || {
                    // TODO: Calling block_processor's event here is not ideal, but duplicating these events is even worse
                    notifications.notify_rollback(&rollback_list, block.qualified_root());
                }));

                // Return early if we reached the maximum number of rollbacks
                if processed.len() >= max_rollbacks {
                    break;
                }
            } else {
                self.stats
                    .inc(StatType::BoundedBacklog, DetailType::RollbackMissingBlock);
                processed.push(*hash);
            }
        }

        processed
    }

    fn run_scan(&self) {
        let mut guard = self.mutex.lock().unwrap();
        while !guard.stopped {
            let mut last = BlockHash::zero();
            while !guard.stopped {
                //	wait
                while !guard.scan_limiter.should_pass(self.config.batch_size) {
                    guard = self
                        .condition
                        .wait_timeout(guard, Duration::from_millis(100))
                        .unwrap()
                        .0;
                    if guard.stopped {
                        return;
                    }
                }

                self.stats
                    .inc(StatType::BoundedBacklog, DetailType::LoopScan);

                let batch = guard.index.next(&last, self.config.batch_size);
                // If batch is empty, we iterated over all accounts in the index
                if batch.is_empty() {
                    break;
                }

                drop(guard);
                {
                    let tx = self.ledger.read_txn();
                    for hash in batch {
                        self.stats
                            .inc(StatType::BoundedBacklog, DetailType::Scanned);
                        self.update(&tx, &hash);
                        last = hash;
                    }
                }
                guard = self.mutex.lock().unwrap();
            }
        }
    }

    fn update(&self, tx: &LmdbReadTransaction, hash: &BlockHash) {
        // Erase if the block is either confirmed or missing
        if !self.ledger.unconfirmed_exists(tx, hash) {
            self.mutex.lock().unwrap().index.erase_hash(hash);
        }
    }
}

struct BacklogData {
    stopped: bool,
    index: BacklogIndex,
    ledger: Arc<Ledger>,
    config: BoundedBacklogConfig,
    bucket_count: usize,
    scan_limiter: RateLimiter,
}

impl BacklogData {
    fn predicate(&self) -> bool {
        // Both ledger and tracked backlog must be over the threshold
        self.ledger.backlog_count() as usize > self.config.max_backlog
            && self.index.len() > self.config.max_backlog
    }

    fn gather_targets(
        &self,
        max_count: usize,
        can_rollback: impl Fn(&BlockHash) -> bool,
    ) -> Vec<BlockHash> {
        let mut targets = Vec::new();

        // Start rolling back from lowest index buckets first
        for bucket in 0..self.bucket_count {
            // Only start rolling back if the bucket is over the threshold of unconfirmed blocks
            if self.index.len_of_bucket(bucket) > self.bucket_threshold() {
                let count = min(max_count, self.config.batch_size);
                let top = self.index.top(bucket, count, |hash| {
                    // Only rollback if the block is not being used by the node
                    can_rollback(hash)
                });
                targets.extend(top);
            }
        }
        targets
    }

    fn bucket_threshold(&self) -> usize {
        self.config.max_backlog / self.bucket_count
    }
}
