use super::{
    block_inspector::BlockInspector,
    cleanup::BootstrapCleanup,
    requesters::Requesters,
    response_processor::{ProcessError, ResponseProcessor},
    state::{BootstrapCounters, BootstrapState, CandidateAccountsConfig, FrontierHeadInfo},
    FrontierScanConfig,
};
use crate::{
    block_processing::{BlockContext, BlockProcessor, LedgerNotifications},
    stats::{DetailType, Sample, StatType, Stats},
    transport::MessageSender,
};
use rsnano_core::{utils::ContainerInfo, Account};
use rsnano_ledger::{BlockStatus, Ledger};
use rsnano_messages::{AscPullAck, BlocksAckPayload};
use rsnano_network::{bandwidth_limiter::RateLimiter, ChannelId, DeadChannelCleanupStep, Network};
use rsnano_nullable_clock::SteadyClock;
use std::{
    sync::{Arc, Condvar, Mutex, RwLock},
    thread::JoinHandle,
    time::Duration,
};
use tracing::warn;

#[derive(Clone, Debug, PartialEq)]
pub struct BootstrapConfig {
    pub enable: bool,
    pub enable_scan: bool,
    pub enable_dependency_walker: bool,
    pub enable_frontier_scan: bool,
    /// Maximum number of un-responded requests per channel, should be lower or equal to bootstrap server max queue size
    pub channel_limit: usize,
    pub rate_limit: usize,
    pub database_rate_limit: usize,
    pub frontier_rate_limit: usize,
    pub database_warmup_ratio: usize,
    pub max_pull_count: u8,
    pub request_timeout: Duration,
    pub throttle_coefficient: usize,
    pub throttle_wait: Duration,
    pub block_processor_theshold: usize,
    /** Minimum accepted protocol version used when bootstrapping */
    pub min_protocol_version: u8,
    pub max_requests: usize,
    pub optimistic_request_percentage: u8,
    pub candidate_accounts: CandidateAccountsConfig,
    pub frontier_scan: FrontierScanConfig,
    /// How many frontier acks can get queued in the processor
    pub max_pending_frontier_responses: usize,
}

impl Default for BootstrapConfig {
    fn default() -> Self {
        Self {
            enable: true,
            enable_scan: true,
            enable_dependency_walker: true,
            enable_frontier_scan: true,
            channel_limit: 16,
            rate_limit: 500,
            database_rate_limit: 256,
            frontier_rate_limit: 8,
            database_warmup_ratio: 10,
            max_pull_count: BlocksAckPayload::MAX_BLOCKS,
            request_timeout: Duration::from_secs(15),
            throttle_coefficient: 8 * 1024,
            throttle_wait: Duration::from_millis(100),
            block_processor_theshold: 1000,
            min_protocol_version: 0x14, // TODO don't hard code
            max_requests: 1024,
            optimistic_request_percentage: 75,
            candidate_accounts: Default::default(),
            frontier_scan: Default::default(),
            max_pending_frontier_responses: 16,
        }
    }
}

pub struct Bootstrapper {
    stats: Arc<Stats>,
    threads: Mutex<Option<Threads>>,
    state: Arc<Mutex<BootstrapState>>,
    state_changed: Arc<Condvar>,
    config: BootstrapConfig,
    clock: Arc<SteadyClock>,
    response_handler: ResponseProcessor,
    block_inspector: BlockInspector,
    requesters: Requesters,
}

struct Threads {
    cleanup: JoinHandle<()>,
}

impl Bootstrapper {
    pub(crate) fn new(
        block_processor: Arc<BlockProcessor>,
        ledger: Arc<Ledger>,
        stats: Arc<Stats>,
        network: Arc<RwLock<Network>>,
        message_sender: MessageSender,
        config: BootstrapConfig,
        clock: Arc<SteadyClock>,
    ) -> Self {
        let limiter = Arc::new(RateLimiter::new(config.rate_limit));
        let state = Arc::new(Mutex::new(BootstrapState::new(config.clone())));
        let state_changed = Arc::new(Condvar::new());

        let mut response_handler = ResponseProcessor::new(
            state.clone(),
            stats.clone(),
            block_processor.clone(),
            ledger.clone(),
        );
        response_handler.set_max_pending_frontiers(config.max_pending_frontier_responses);

        let block_inspector = BlockInspector::new(state.clone(), ledger.clone(), stats.clone());
        let requesters = Requesters::new(
            limiter.clone(),
            config.clone(),
            stats.clone(),
            message_sender.clone(),
            state.clone(),
            state_changed.clone(),
            clock.clone(),
            ledger.clone(),
            block_processor.clone(),
            network,
        );

        Self {
            threads: Mutex::new(None),
            state,
            state_changed,
            config,
            stats,
            clock,
            response_handler,
            block_inspector,
            requesters,
        }
    }

    pub fn stop(&self) {
        {
            let mut guard = self.state.lock().unwrap();
            guard.stopped = true;
        }
        self.state_changed.notify_all();

        self.requesters.stop();

        let threads = self.threads.lock().unwrap().take();
        if let Some(threads) = threads {
            threads.cleanup.join().unwrap();
        }
    }

    pub fn frontier_heads(&self) -> Vec<FrontierHeadInfo> {
        self.state.lock().unwrap().frontier_scan.heads()
    }

    pub fn counters(&self) -> BootstrapCounters {
        self.state.lock().unwrap().counters.clone()
    }

    pub fn prioritized(&self, account: &Account) -> bool {
        self.state
            .lock()
            .unwrap()
            .candidate_accounts
            .prioritized(account)
    }

    fn run_timeouts(&self) {
        let mut cleanup = BootstrapCleanup::new(self.clock.clone(), self.stats.clone());
        let mut state = self.state.lock().unwrap();
        while !state.stopped {
            cleanup.cleanup(&mut state);
            self.state_changed.notify_all();

            state = self
                .state_changed
                .wait_timeout_while(state, Duration::from_secs(1), |s| !s.stopped)
                .unwrap()
                .0;
        }
    }

    /// Process `asc_pull_ack` message coming from network
    pub fn process(&self, message: AscPullAck, channel_id: ChannelId) {
        let now = self.clock.now();
        let result = self.response_handler.process(message, channel_id, now);
        match result {
            Ok(info) => {
                self.stats.inc(StatType::Bootstrap, DetailType::Reply);
                self.stats
                    .inc(StatType::BootstrapReply, info.query_type.into());
                self.stats.sample(
                    Sample::BootstrapTagDuration,
                    info.response_time.as_millis() as i64,
                    (0, self.config.request_timeout.as_millis() as i64),
                );
            }
            Err(ProcessError::NoRunningQueryFound) => {
                self.stats.inc(StatType::Bootstrap, DetailType::MissingTag);
            }
            Err(ProcessError::InvalidResponseType) => {
                self.stats
                    .inc(StatType::Bootstrap, DetailType::InvalidResponseType);
            }
            Err(ProcessError::InvalidResponse) => {
                self.stats
                    .inc(StatType::Bootstrap, DetailType::InvalidResponse);
            }
        }
    }

    fn priority_inserted(&self) {
        self.stats
            .inc(StatType::BootstrapAccountSets, DetailType::PriorityInsert);
    }

    fn priority_insertion_failed(&self) {
        self.stats
            .inc(StatType::BootstrapAccountSets, DetailType::PrioritizeFailed);
    }

    fn blocks_processed(&self, batch: &[(BlockStatus, Arc<BlockContext>)]) {
        self.block_inspector.inspect(batch);
        self.state_changed.notify_all();
    }

    pub fn container_info(&self) -> ContainerInfo {
        self.state.lock().unwrap().container_info()
    }
}

impl Drop for Bootstrapper {
    fn drop(&mut self) {
        // All threads must be stopped before destruction
        debug_assert!(self.threads.lock().unwrap().is_none());
    }
}

pub trait BootstrapExt {
    fn initialize(&self, genesis_account: &Account, notifications: &LedgerNotifications);
    fn start(&self);
}

impl BootstrapExt for Arc<Bootstrapper> {
    fn initialize(&self, genesis_account: &Account, notifications: &LedgerNotifications) {
        let self_w = Arc::downgrade(self);
        // Inspect all processed blocks
        notifications.on_blocks_processed(Box::new(move |batch| {
            if let Some(self_l) = self_w.upgrade() {
                self_l.blocks_processed(batch);
            }
        }));

        // Unblock rolled back accounts as the dependency is no longer valid
        let self_w = Arc::downgrade(self);
        notifications.on_blocks_rolled_back(move |blocks, _rollback_root| {
            let Some(self_l) = self_w.upgrade() else {
                return;
            };
            let mut guard = self_l.state.lock().unwrap();
            for block in blocks {
                guard.candidate_accounts.unblock(block.account(), None);
            }
        });

        let inserted = self
            .state
            .lock()
            .unwrap()
            .candidate_accounts
            .priority_set_initial(genesis_account);

        if inserted {
            self.priority_inserted()
        } else {
            self.priority_insertion_failed()
        };
    }

    fn start(&self) {
        debug_assert!(self.threads.lock().unwrap().is_none());

        if !self.config.enable {
            warn!("Ascending bootstrap is disabled");
            return;
        }

        self.requesters.start();

        let self_l = Arc::clone(self);
        let timeout = std::thread::Builder::new()
            .name("Bootstrap clean".to_string())
            .spawn(Box::new(move || self_l.run_timeouts()))
            .unwrap();

        *self.threads.lock().unwrap() = Some(Threads { cleanup: timeout });
    }
}

pub(crate) struct BootstrapperCleanup(pub Arc<Bootstrapper>);

impl DeadChannelCleanupStep for BootstrapperCleanup {
    fn clean_up_dead_channels(&self, dead_channel_ids: &[ChannelId]) {
        self.0
            .state
            .lock()
            .unwrap()
            .scoring
            .clean_up_dead_channels(dead_channel_ids);
    }
}
