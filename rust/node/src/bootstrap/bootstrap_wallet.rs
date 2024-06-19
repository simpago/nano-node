use super::{
    BootstrapAttempt, BootstrapConnections, BootstrapConnectionsExt, BootstrapInitiator,
    BootstrapMode, BulkPullAccountClient, BulkPullAccountClientExt,
};
use crate::{
    block_processing::BlockProcessor, stats::Stats, utils::ThreadPool, websocket::WebsocketListener,
};
use rsnano_core::{utils::PropertyTree, Account, Amount};
use rsnano_ledger::Ledger;
use std::{
    collections::VecDeque,
    sync::{atomic::Ordering, Arc, Mutex, MutexGuard, Weak},
    time::{Duration, Instant},
};
use tracing::{debug, info};

pub struct BootstrapAttemptWallet {
    pub attempt: BootstrapAttempt,
    mutex: Mutex<WalletData>,
    connections: Arc<BootstrapConnections>,
    workers: Arc<dyn ThreadPool>,
    receive_minimum: Amount,
    stats: Arc<Stats>,
    ledger: Arc<Ledger>,
    bootstrap_initiator: Weak<BootstrapInitiator>,
}

impl BootstrapAttemptWallet {
    pub fn new(
        websocket_server: Option<Arc<WebsocketListener>>,
        block_processor: Arc<BlockProcessor>,
        bootstrap_initiator: Arc<BootstrapInitiator>,
        ledger: Arc<Ledger>,
        id: String,
        incremental_id: u64,
        connections: Arc<BootstrapConnections>,
        workers: Arc<dyn ThreadPool>,
        receive_minimum: Amount,
        stats: Arc<Stats>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            attempt: BootstrapAttempt::new(
                websocket_server,
                Arc::downgrade(&block_processor),
                Arc::downgrade(&bootstrap_initiator),
                Arc::clone(&ledger),
                id,
                BootstrapMode::WalletLazy,
                incremental_id,
            )?,
            mutex: Mutex::new(WalletData {
                wallet_accounts: VecDeque::new(),
            }),
            connections,
            workers,
            receive_minimum,
            stats,
            ledger,
            bootstrap_initiator: Arc::downgrade(&bootstrap_initiator),
        })
    }

    pub fn requeue_pending(&self, account: Account) {
        {
            let mut guard = self.mutex.lock().unwrap();
            guard.wallet_accounts.push_front(account);
        }
        self.attempt.condition.notify_all();
    }

    pub fn wallet_start(&self, accounts: &mut VecDeque<Account>) {
        {
            let mut guard = self.mutex.lock().unwrap();
            std::mem::swap(&mut guard.wallet_accounts, accounts);
        }
        self.attempt.condition.notify_all();
    }

    fn wallet_finished(&self, data: &WalletData) -> bool {
        let running = !self.attempt.stopped.load(Ordering::SeqCst);
        let more_accounts = !data.wallet_accounts.is_empty();
        let still_pulling = self.attempt.pulling.load(Ordering::SeqCst) > 0;
        return running && (more_accounts || still_pulling);
    }

    pub fn wallet_size(&self) -> usize {
        let guard = self.mutex.lock().unwrap();
        guard.wallet_accounts.len()
    }

    pub fn get_information(&self, result: &mut dyn PropertyTree) {
        result
            .put_u64("wallet_accounts", self.wallet_size() as u64)
            .unwrap();
    }
}

pub struct WalletData {
    wallet_accounts: VecDeque<Account>,
}

pub trait BootstrapAttemptWalletExt {
    fn run(&self);
    fn request_pending<'a>(
        &'a self,
        guard: MutexGuard<'a, WalletData>,
    ) -> MutexGuard<'a, WalletData>;
}

impl BootstrapAttemptWalletExt for Arc<BootstrapAttemptWallet> {
    fn run(&self) {
        debug_assert!(self.attempt.started.load(Ordering::SeqCst));
        self.connections.populate_connections(false);
        let start_time = Instant::now();
        let max_time = Duration::from_secs(60 * 10);
        let mut guard = self.mutex.lock().unwrap();
        while self.wallet_finished(&guard) && start_time.elapsed() < max_time {
            if !guard.wallet_accounts.is_empty() {
                guard = self.request_pending(guard);
            } else {
                guard = self
                    .attempt
                    .condition
                    .wait_timeout(guard, Duration::from_millis(1000))
                    .unwrap()
                    .0;
            }
        }
        if !self.attempt.stopped() {
            info!("Completed wallet lazy pulls");
        }
        drop(guard);
        self.attempt.stop();
        self.attempt.condition.notify_all();
    }

    fn request_pending<'a>(
        &'a self,
        guard: MutexGuard<'a, WalletData>,
    ) -> MutexGuard<'a, WalletData> {
        drop(guard);
        let (connection_l, should_stop) = self.connections.connection(false);
        if should_stop {
            debug!("Bootstrap attempt stopped because there are no peers");
            self.attempt.stop();
        }

        let mut guard = self.mutex.lock().unwrap();
        if connection_l.is_some() && !self.attempt.stopped() {
            let account = guard.wallet_accounts.pop_front().unwrap();
            self.attempt.pulling.fetch_add(1, Ordering::SeqCst);
            let self_l = Arc::clone(self);
            // The bulk_pull_account_client destructor attempt to requeue_pull which can cause a deadlock if this is the last reference
            // Dispatch request in an external thread in case it needs to be destroyed

            self.workers.push_task(Box::new(move || {
                if let Some(bootstrap_initiator) = self_l.bootstrap_initiator.upgrade() {
                    let client = Arc::new(BulkPullAccountClient::new(
                        connection_l.unwrap(),
                        Arc::clone(&self_l),
                        account,
                        self_l.receive_minimum,
                        Arc::clone(&self_l.stats),
                        Arc::clone(&self_l.connections),
                        Arc::clone(&self_l.ledger),
                        bootstrap_initiator,
                    ));
                    client.request();
                }
            }));
        }
        guard
    }
}
