use crate::stats::{DetailType, StatType, Stats};
use rsnano_core::{Account, AccountInfo, ConfirmationHeightInfo};
use rsnano_ledger::Ledger;
use rsnano_network::bandwidth_limiter::RateLimiter;
use std::{
    cmp::max,
    sync::{Arc, Condvar, Mutex, MutexGuard, RwLock},
    thread::{self, JoinHandle},
    time::Duration,
};

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct BacklogScanConfig {
    /// Control if ongoing backlog population is enabled. If not, backlog population can still be triggered by RPC
    pub enabled: bool,

    /// Number of accounts per second to process.
    pub batch_size: usize,

    /// Number of accounts to scan per second
    pub rate_limit: usize,
}

impl Default for BacklogScanConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            batch_size: 1000,
            rate_limit: 10_000,
        }
    }
}

struct BacklogScanFlags {
    stopped: bool,
    /** This is a manual trigger, the ongoing backlog population does not use this.
     *  It can be triggered even when backlog population (frontiers confirmation) is disabled. */
    triggered: bool,
}

pub struct BacklogScan {
    ledger: Arc<Ledger>,
    stats: Arc<Stats>,
    /// Callback called for each backlogged account
    activated_observers: Arc<RwLock<Vec<Box<dyn Fn(&[ActivatedInfo]) + Send + Sync>>>>,
    scanned_observers: Arc<RwLock<Vec<Box<dyn Fn(&[ActivatedInfo]) + Send + Sync>>>>,

    config: BacklogScanConfig,
    mutex: Arc<Mutex<BacklogScanFlags>>,
    condition: Arc<Condvar>,
    /** Thread that runs the backlog implementation logic. The thread always runs, even if
     *  backlog population is disabled, so that it can service a manual trigger (e.g. via RPC). */
    thread: Mutex<Option<JoinHandle<()>>>,
}

impl BacklogScan {
    pub(crate) fn new(config: BacklogScanConfig, ledger: Arc<Ledger>, stats: Arc<Stats>) -> Self {
        Self {
            config,
            ledger,
            stats,
            activated_observers: Arc::new(RwLock::new(Vec::new())),
            scanned_observers: Arc::new(RwLock::new(Vec::new())),
            mutex: Arc::new(Mutex::new(BacklogScanFlags {
                stopped: false,
                triggered: false,
            })),
            condition: Arc::new(Condvar::new()),
            thread: Mutex::new(None),
        }
    }

    /// Accounts activated
    pub fn on_batch_activated(&self, callback: impl Fn(&[ActivatedInfo]) + Send + Sync + 'static) {
        self.activated_observers
            .write()
            .unwrap()
            .push(Box::new(callback));
    }

    /// Accounts scanned but not activated
    pub fn on_batch_scanned(&self, callback: impl Fn(&[ActivatedInfo]) + Send + Sync + 'static) {
        self.scanned_observers
            .write()
            .unwrap()
            .push(Box::new(callback));
    }

    pub fn start(&self) {
        debug_assert!(self.thread.lock().unwrap().is_none());

        let thread = BacklogScanThread {
            ledger: self.ledger.clone(),
            stats: self.stats.clone(),
            activated_observers: self.activated_observers.clone(),
            scanned_observers: self.scanned_observers.clone(),
            config: self.config.clone(),
            mutex: self.mutex.clone(),
            condition: self.condition.clone(),
            limiter: RateLimiter::new(self.config.rate_limit),
        };

        *self.thread.lock().unwrap() = Some(
            thread::Builder::new()
                .name("Backlog".to_owned())
                .spawn(move || {
                    thread.run();
                })
                .unwrap(),
        );
    }

    pub fn stop(&self) {
        let mut lock = self.mutex.lock().unwrap();
        lock.stopped = true;
        drop(lock);
        self.notify();
        let handle = self.thread.lock().unwrap().take();
        if let Some(handle) = handle {
            handle.join().unwrap()
        }
    }

    /** Manually trigger backlog population */
    pub fn trigger(&self) {
        {
            let mut lock = self.mutex.lock().unwrap();
            lock.triggered = true;
        }
        self.notify();
    }

    /** Notify about AEC vacancy */
    pub fn notify(&self) {
        self.condition.notify_all();
    }
}

impl Drop for BacklogScan {
    fn drop(&mut self) {
        self.stop();
    }
}

struct BacklogScanThread {
    ledger: Arc<Ledger>,
    stats: Arc<Stats>,
    activated_observers: Arc<RwLock<Vec<Box<dyn Fn(&[ActivatedInfo]) + Send + Sync>>>>,
    scanned_observers: Arc<RwLock<Vec<Box<dyn Fn(&[ActivatedInfo]) + Send + Sync>>>>,
    config: BacklogScanConfig,
    mutex: Arc<Mutex<BacklogScanFlags>>,
    condition: Arc<Condvar>,
    limiter: RateLimiter,
}

impl BacklogScanThread {
    fn run(&self) {
        let mut lock = self.mutex.lock().unwrap();
        while !lock.stopped {
            if self.predicate(&lock) {
                self.stats.inc(StatType::BacklogScan, DetailType::Loop);

                lock.triggered = false;
                // Does a single iteration over all accounts
                lock = self.populate_backlog(lock);
            } else {
                lock = self
                    .condition
                    .wait_while(lock, |l| !l.stopped && !self.predicate(l))
                    .unwrap();
            }
        }
    }

    fn predicate(&self, lock: &BacklogScanFlags) -> bool {
        lock.triggered || self.config.enabled
    }

    fn populate_backlog<'a>(
        &'a self,
        mut lock: MutexGuard<'a, BacklogScanFlags>,
    ) -> MutexGuard<'a, BacklogScanFlags> {
        let mut next = Account::zero();
        let mut done = false;
        while !lock.stopped && !done {
            // Wait for the rate limiter
            while !self.limiter.should_pass(self.config.batch_size) {
                let wait_time = Duration::from_millis(
                    1000 / max(self.config.rate_limit / self.config.batch_size, 1) as u64 / 2,
                );

                lock = self
                    .condition
                    .wait_timeout_while(lock, max(wait_time, Duration::from_millis(10)), |i| {
                        !i.stopped
                    })
                    .unwrap()
                    .0;
                if lock.stopped {
                    return lock;
                }
            }

            drop(lock);

            let mut scanned = Vec::new();
            let mut activated = Vec::new();
            {
                let tx = self.ledger.store.tx_begin_read();
                let mut count = 0;
                let mut it = self.ledger.any().accounts_range(&tx, next..);
                while let Some((account, account_info)) = it.next() {
                    if count >= self.config.batch_size {
                        break;
                    }

                    self.stats.inc(StatType::BacklogScan, DetailType::Total);

                    let conf_info = self
                        .ledger
                        .store
                        .confirmation_height
                        .get(&tx, &account)
                        .unwrap_or_default();

                    let info = ActivatedInfo {
                        account,
                        account_info,
                        conf_info: conf_info.clone(),
                    };

                    scanned.push(info.clone());

                    if conf_info.height < info.account_info.block_count {
                        activated.push(info);
                    }

                    next = account.inc_or_max();
                    count += 1;
                }
                done = self
                    .ledger
                    .any()
                    .accounts_range(&tx, next..)
                    .next()
                    .is_none();
            }

            self.stats.add(
                StatType::BacklogScan,
                DetailType::Scanned,
                scanned.len() as u64,
            );
            self.stats.add(
                StatType::BacklogScan,
                DetailType::Activated,
                activated.len() as u64,
            );

            // Notify about scanned and activated accounts without holding database transaction
            {
                let observers = self.scanned_observers.read().unwrap();
                for observer in &*observers {
                    observer(&scanned);
                }
            }
            {
                let observers = self.activated_observers.read().unwrap();
                for observer in &*observers {
                    observer(&activated);
                }
            }

            lock = self.mutex.lock().unwrap();
        }
        lock
    }
}

#[derive(Clone)]
pub struct ActivatedInfo {
    pub account: Account,
    pub account_info: AccountInfo,
    pub conf_info: ConfirmationHeightInfo,
}
