use super::{
    crawlers::{AccountDatabaseCrawler, PendingDatabaseCrawler},
    dependency_query::DependencyQuery,
    frontier_scan::{AccountRanges, AccountRangesConfig, FrontierScan},
    peer_scoring::PeerScoring,
    priority_query::PriorityQuery,
    running_query_container::{QuerySource, QueryType, RunningQuery, RunningQueryContainer},
    throttle::Throttle,
    AscPullQuerySpec, BootstrapAction, CandidateAccounts, CandidateAccountsConfig,
    PriorityDownResult, PriorityResult, PriorityUpResult,
};
use crate::{
    block_processing::{BlockContext, BlockProcessor, BlockSource, LedgerNotifications},
    bootstrap::WaitResult,
    stats::{DetailType, Direction, Sample, StatType, Stats},
    transport::MessageSender,
    utils::{ThreadPool, ThreadPoolImpl},
};
use rand::{thread_rng, RngCore};
use rsnano_core::{
    utils::ContainerInfo, Account, Block, BlockHash, BlockType, Frontier, SavedBlock,
};
use rsnano_ledger::{BlockStatus, Ledger};
use rsnano_messages::{
    AccountInfoAckPayload, AscPullAck, AscPullAckType, AscPullReq, AscPullReqType,
    BlocksAckPayload, HashType, Message,
};
use rsnano_network::{bandwidth_limiter::RateLimiter, Channel, ChannelId, Network, TrafficType};
use rsnano_nullable_clock::{SteadyClock, Timestamp};
use std::{
    cmp::{max, min},
    sync::{Arc, Condvar, Mutex, RwLock},
    thread::JoinHandle,
    time::{Duration, Instant},
};
use tracing::{debug, warn};

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
    pub max_pull_count: usize,
    pub request_timeout: Duration,
    pub throttle_coefficient: usize,
    pub throttle_wait: Duration,
    pub block_processor_theshold: usize,
    /** Minimum accepted protocol version used when bootstrapping */
    pub min_protocol_version: u8,
    pub max_requests: usize,
    pub optimistic_request_percentage: u8,
    pub candidate_accounts: CandidateAccountsConfig,
    pub frontier_scan: AccountRangesConfig,
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
        }
    }
}

enum VerifyResult {
    Ok,
    NothingNew,
    Invalid,
}

pub struct Bootstrapper {
    block_processor: Arc<BlockProcessor>,
    ledger: Arc<Ledger>,
    stats: Arc<Stats>,
    threads: Mutex<Option<Threads>>,
    mutex: Arc<Mutex<BootstrapLogic>>,
    condition: Arc<Condvar>,
    config: BootstrapConfig,
    /// Requests for accounts from database have much lower hitrate and could introduce strain on the network
    /// A separate (lower) limiter ensures that we always reserve resources for querying accounts from priority queue
    database_limiter: RateLimiter,
    clock: Arc<SteadyClock>,
    workers: Arc<ThreadPoolImpl>,
}

struct Threads {
    cleanup: JoinHandle<()>,
    priorities: Option<JoinHandle<()>>,
    dependencies: Option<JoinHandle<()>>,
    frontiers: Option<JoinHandle<()>>,
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
        let workers = Arc::new(ThreadPoolImpl::create(1, "Bootstrap work"));

        Self {
            threads: Mutex::new(None),
            mutex: Arc::new(Mutex::new(BootstrapLogic {
                stopped: false,
                candidate_accounts: CandidateAccounts::new(config.candidate_accounts.clone()),
                scoring: PeerScoring::new(config.clone()),
                account_ranges: AccountRanges::new(config.frontier_scan.clone(), stats.clone()),
                running_queries: RunningQueryContainer::default(),
                throttle: Throttle::new(compute_throttle_size(
                    ledger.account_count(),
                    config.throttle_coefficient,
                )),
                sync_dependencies_interval: Instant::now(),
                config: config.clone(),
                network,
                limiter: RateLimiter::new(config.rate_limit),
                frontiers_limiter: RateLimiter::new(config.frontier_rate_limit),
                workers: workers.clone(),
                stats: stats.clone(),
                message_sender,
                block_processor: block_processor.clone(),
                ledger: ledger.clone(),
            })),
            block_processor,
            condition: Arc::new(Condvar::new()),
            database_limiter: RateLimiter::new(config.database_rate_limit),
            config,
            stats,
            ledger,
            clock,
            workers,
        }
    }

    pub fn stop(&self) {
        self.mutex.lock().unwrap().stopped = true;
        self.condition.notify_all();
        let threads = self.threads.lock().unwrap().take();
        if let Some(threads) = threads {
            if let Some(handle) = threads.priorities {
                handle.join().unwrap();
            }
            threads.cleanup.join().unwrap();
            if let Some(dependencies) = threads.dependencies {
                dependencies.join().unwrap();
            }
            if let Some(frontiers) = threads.frontiers {
                frontiers.join().unwrap();
            }
        }
    }

    pub fn prioritized(&self, account: &Account) -> bool {
        self.mutex
            .lock()
            .unwrap()
            .candidate_accounts
            .prioritized(account)
    }

    fn wait_for<A, T>(&self, action: &mut A) -> Option<T>
    where
        A: BootstrapAction<T>,
    {
        const INITIAL_INTERVAL: Duration = Duration::from_millis(5);
        let mut interval = INITIAL_INTERVAL;
        let mut guard = self.mutex.lock().unwrap();
        loop {
            if guard.stopped {
                return None;
            }

            match action.run(&mut *guard, self.clock.now()) {
                WaitResult::BeginWait => {
                    interval = INITIAL_INTERVAL;
                }
                WaitResult::ContinueWait => {
                    interval = min(interval * 2, self.config.throttle_wait);
                }
                WaitResult::Finished(result) => return Some(result),
            }

            guard = self
                .condition
                .wait_timeout_while(guard, interval, |g| !g.stopped)
                .unwrap()
                .0;
        }
    }

    fn run_queries<T: BootstrapAction<AscPullQuerySpec>>(&self, mut query_factory: T) {
        loop {
            let Some(spec) = self.wait_for(&mut query_factory) else {
                return;
            };
            self.send_request(spec);
        }
    }

    fn send_request(&self, spec: AscPullQuerySpec) {
        let id = thread_rng().next_u64();
        let now = self.clock.now();
        let query = RunningQuery::from_request(id, &spec, now, self.config.request_timeout);

        let request = AscPullReq {
            id,
            req_type: spec.req_type,
        };

        let mut guard = self.mutex.lock().unwrap();
        guard.running_queries.insert(query);
        let message = Message::AscPullReq(request);
        let sent = guard.send(&spec.channel, &message, id, now);
        if sent && spec.cooldown_account {
            guard.candidate_accounts.timestamp_set(&spec.account, now);
        }
    }

    fn run_timeouts(&self) {
        let mut guard = self.mutex.lock().unwrap();
        while !guard.stopped {
            self.stats.inc(StatType::Bootstrap, DetailType::LoopCleanup);

            guard.cleanup_and_sync(self.ledger.account_count(), &self.stats, self.clock.now());

            guard = self
                .condition
                .wait_timeout_while(guard, Duration::from_secs(1), |g| !g.stopped)
                .unwrap()
                .0;
        }
    }

    /// Process `asc_pull_ack` message coming from network
    pub fn process(&self, message: AscPullAck, channel_id: ChannelId) {
        let mut guard = self.mutex.lock().unwrap();

        // Only process messages that have a known tag
        let Some(tag) = guard.running_queries.remove(message.id) else {
            self.stats.inc(StatType::Bootstrap, DetailType::MissingTag);
            return;
        };

        self.stats.inc(StatType::Bootstrap, DetailType::Reply);

        let valid = match message.pull_type {
            AscPullAckType::Blocks(_) => matches!(
                tag.query_type,
                QueryType::BlocksByHash | QueryType::BlocksByAccount
            ),
            AscPullAckType::AccountInfo(_) => tag.query_type == QueryType::AccountInfoByHash,
            AscPullAckType::Frontiers(_) => tag.query_type == QueryType::Frontiers,
        };

        if !valid {
            self.stats
                .inc(StatType::Bootstrap, DetailType::InvalidResponseType);
            return;
        }

        // Track bootstrap request response time
        self.stats
            .inc(StatType::BootstrapReply, tag.query_type.into());

        self.stats.sample(
            Sample::BootstrapTagDuration,
            tag.sent.elapsed(self.clock.now()).as_millis() as i64,
            (0, self.config.request_timeout.as_millis() as i64),
        );

        drop(guard);

        // Process the response payload
        let ok = match message.pull_type {
            AscPullAckType::Blocks(blocks) => self.process_blocks(&blocks, &tag),
            AscPullAckType::AccountInfo(info) => self.process_accounts(&info, &tag),
            AscPullAckType::Frontiers(frontiers) => self.process_frontiers(frontiers, &tag),
        };

        if ok {
            self.mutex
                .lock()
                .unwrap()
                .scoring
                .received_message(channel_id);
        } else {
            self.stats
                .inc(StatType::Bootstrap, DetailType::InvalidResponse);
        }

        self.condition.notify_all();
    }

    fn process_frontiers(&self, frontiers: Vec<Frontier>, query: &RunningQuery) -> bool {
        debug_assert_eq!(query.query_type, QueryType::Frontiers);
        debug_assert!(!query.start.is_zero());

        if frontiers.is_empty() {
            self.stats
                .inc(StatType::BootstrapProcess, DetailType::FrontiersEmpty);
            // OK, but nothing to do
            return true;
        }

        self.stats
            .inc(StatType::BootstrapProcess, DetailType::Frontiers);

        let result = self.verify_frontiers(&frontiers, query);
        match result {
            VerifyResult::Ok => {
                self.stats
                    .inc(StatType::BootstrapVerifyFrontiers, DetailType::Ok);
                self.stats.add_dir(
                    StatType::Bootstrap,
                    DetailType::Frontiers,
                    Direction::In,
                    frontiers.len() as u64,
                );

                {
                    let mut guard = self.mutex.lock().unwrap();
                    guard.account_ranges.process(query.start.into(), &frontiers);
                }

                // Allow some overfill to avoid unnecessarily dropping responses
                if self.workers.num_queued_tasks() < self.config.frontier_scan.max_pending * 4 {
                    let stats = self.stats.clone();
                    let ledger = self.ledger.clone();
                    let mutex = self.mutex.clone();
                    self.workers.post(Box::new(move || {
                        process_frontiers(ledger, stats, frontiers, mutex)
                    }));
                } else {
                    self.stats.add(
                        StatType::Bootstrap,
                        DetailType::FrontiersDropped,
                        frontiers.len() as u64,
                    );
                }
                true
            }
            VerifyResult::NothingNew => {
                self.stats
                    .inc(StatType::BootstrapVerifyFrontiers, DetailType::NothingNew);
                true
            }
            VerifyResult::Invalid => {
                self.stats
                    .inc(StatType::BootstrapVerifyFrontiers, DetailType::Invalid);
                false
            }
        }
    }

    fn verify_frontiers(&self, frontiers: &[Frontier], query: &RunningQuery) -> VerifyResult {
        if frontiers.is_empty() {
            return VerifyResult::NothingNew;
        }

        // Ensure frontiers accounts are in ascending order
        let mut previous = Account::zero();
        for f in frontiers {
            if f.account.number() <= previous.number() {
                return VerifyResult::Invalid;
            }
            previous = f.account;
        }

        // Ensure the frontiers are larger or equal to the requested frontier
        if frontiers[0].account.number() < query.start.number() {
            return VerifyResult::Invalid;
        }

        VerifyResult::Ok
    }

    fn process_blocks(&self, response: &BlocksAckPayload, query: &RunningQuery) -> bool {
        self.stats
            .inc(StatType::BootstrapProcess, DetailType::Blocks);

        let result = verify_response(response, query);
        match result {
            VerifyResult::Ok => {
                self.stats
                    .inc(StatType::BootstrapVerifyBlocks, DetailType::Ok);
                self.stats.add_dir(
                    StatType::Bootstrap,
                    DetailType::Blocks,
                    Direction::In,
                    response.blocks().len() as u64,
                );

                let mut blocks = response.blocks().clone();

                // Avoid re-processing the block we already have
                assert!(blocks.len() >= 1);
                if blocks.front().unwrap().hash() == query.start.into() {
                    blocks.pop_front();
                }

                while let Some(block) = blocks.pop_front() {
                    if blocks.is_empty() {
                        // It's the last block submitted for this account chain, reset timestamp to allow more requests
                        let stats = self.stats.clone();
                        let data = self.mutex.clone();
                        let condition = self.condition.clone();
                        let account = query.account;
                        self.block_processor.add_with_callback(
                            block,
                            BlockSource::Bootstrap,
                            ChannelId::LOOPBACK,
                            Box::new(move |_| {
                                stats.inc(StatType::Bootstrap, DetailType::TimestampReset);
                                {
                                    let mut guard = data.lock().unwrap();
                                    guard.candidate_accounts.timestamp_reset(&account);
                                }
                                condition.notify_all();
                            }),
                        );
                    } else {
                        self.block_processor.add(
                            block,
                            BlockSource::Bootstrap,
                            ChannelId::LOOPBACK,
                        );
                    }
                }
                true
            }
            VerifyResult::NothingNew => {
                self.stats
                    .inc(StatType::BootstrapVerifyBlocks, DetailType::NothingNew);

                {
                    let mut guard = self.mutex.lock().unwrap();
                    match guard.candidate_accounts.priority_down(&query.account) {
                        PriorityDownResult::Deprioritized => {
                            self.stats
                                .inc(StatType::BootstrapAccountSets, DetailType::Deprioritize);
                        }
                        PriorityDownResult::Erased => {
                            self.stats
                                .inc(StatType::BootstrapAccountSets, DetailType::Deprioritize);
                            self.stats.inc(
                                StatType::BootstrapAccountSets,
                                DetailType::PriorityEraseThreshold,
                            );
                        }
                        PriorityDownResult::AccountNotFound => {
                            self.stats.inc(
                                StatType::BootstrapAccountSets,
                                DetailType::DeprioritizeFailed,
                            );
                        }
                        PriorityDownResult::InvalidAccount => {}
                    }

                    guard.candidate_accounts.timestamp_reset(&query.account);
                }
                self.condition.notify_all();
                true
            }
            VerifyResult::Invalid => {
                self.stats
                    .inc(StatType::BootstrapVerifyBlocks, DetailType::Invalid);
                false
            }
        }
    }

    fn process_accounts(&self, response: &AccountInfoAckPayload, query: &RunningQuery) -> bool {
        if response.account.is_zero() {
            self.stats
                .inc(StatType::BootstrapProcess, DetailType::AccountInfoEmpty);
            // OK, but nothing to do
            return true;
        }

        self.stats
            .inc(StatType::BootstrapProcess, DetailType::AccountInfo);

        // Prioritize account containing the dependency
        {
            let mut guard = self.mutex.lock().unwrap();
            let updated = guard
                .candidate_accounts
                .dependency_update(&query.hash, response.account);
            if updated > 0 {
                self.stats.add(
                    StatType::BootstrapAccountSets,
                    DetailType::DependencyUpdate,
                    updated as u64,
                );
            } else {
                self.stats.inc(
                    StatType::BootstrapAccountSets,
                    DetailType::DependencyUpdateFailed,
                );
            }

            if guard
                .candidate_accounts
                .priority_set(&response.account, CandidateAccounts::PRIORITY_CUTOFF)
            {
                self.priority_inserted();
            } else {
                self.priority_insertion_failed()
            };
        }
        // OK, no way to verify the response
        true
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
        {
            let mut guard = self.mutex.lock().unwrap();
            let tx = self.ledger.read_txn();
            for (result, context) in batch {
                let block = context.block.lock().unwrap().clone();
                let saved_block = context.saved_block.lock().unwrap().clone();
                let account = block.account_field().unwrap_or_else(|| {
                    self.ledger
                        .any()
                        .block_account(&tx, &block.previous())
                        .unwrap_or_default()
                });

                guard.inspect(
                    &self.stats,
                    *result,
                    &block,
                    saved_block,
                    context.source,
                    &account,
                );
            }
        }

        self.condition.notify_all();
    }

    pub fn container_info(&self) -> ContainerInfo {
        self.mutex
            .lock()
            .unwrap()
            .container_info(&self.database_limiter)
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
            let mut guard = self_l.mutex.lock().unwrap();
            for block in blocks {
                guard.candidate_accounts.unblock(block.account(), None);
            }
        });

        let inserted = self
            .mutex
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

        let priorities = if self.config.enable_scan {
            let self_l = Arc::clone(self);
            Some(
                std::thread::Builder::new()
                    .name("Bootstrap".to_string())
                    .spawn(Box::new(move || self_l.run_queries(PriorityQuery::new())))
                    .unwrap(),
            )
        } else {
            None
        };

        let dependencies = if self.config.enable_dependency_walker {
            let self_l = Arc::clone(self);
            Some(
                std::thread::Builder::new()
                    .name("Bootstrap walkr".to_string())
                    .spawn(Box::new(move || self_l.run_queries(DependencyQuery::new())))
                    .unwrap(),
            )
        } else {
            None
        };

        let frontiers = if self.config.enable_frontier_scan {
            let self_l = Arc::clone(self);
            Some(
                std::thread::Builder::new()
                    .name("Bootstrap front".to_string())
                    .spawn(Box::new(move || self_l.run_queries(FrontierScan::new())))
                    .unwrap(),
            )
        } else {
            None
        };

        let self_l = Arc::clone(self);
        let timeout = std::thread::Builder::new()
            .name("Bootstrap clean".to_string())
            .spawn(Box::new(move || self_l.run_timeouts()))
            .unwrap();

        *self.threads.lock().unwrap() = Some(Threads {
            cleanup: timeout,
            priorities,
            frontiers,
            dependencies,
        });
    }
}

pub(super) struct BootstrapLogic {
    stopped: bool,
    pub candidate_accounts: CandidateAccounts,
    pub scoring: PeerScoring,
    pub running_queries: RunningQueryContainer,
    throttle: Throttle,
    pub account_ranges: AccountRanges,
    sync_dependencies_interval: Instant,
    pub config: BootstrapConfig,
    network: Arc<RwLock<Network>>,
    /// Rate limiter for all types of requests
    pub limiter: RateLimiter,
    /// Rate limiter for frontier requests
    pub frontiers_limiter: RateLimiter,
    pub workers: Arc<ThreadPoolImpl>,
    pub stats: Arc<Stats>,
    pub message_sender: MessageSender,
    pub block_processor: Arc<BlockProcessor>,
    pub ledger: Arc<Ledger>,
}

impl BootstrapLogic {
    /// Inspects a block that has been processed by the block processor
    /// - Marks an account as blocked if the result code is gap source as there is no reason request additional blocks for this account until the dependency is resolved
    /// - Marks an account as forwarded if it has been recently referenced by a block that has been inserted.
    fn inspect(
        &mut self,
        stats: &Stats,
        status: BlockStatus,
        block: &Block,
        saved_block: Option<SavedBlock>,
        source: BlockSource,
        account: &Account,
    ) {
        let hash = block.hash();

        match status {
            BlockStatus::Progress => {
                // Progress blocks from live traffic don't need further bootstrapping
                if source != BlockSource::Live {
                    let saved_block = saved_block.unwrap();
                    let account = saved_block.account();
                    // If we've inserted any block in to an account, unmark it as blocked
                    if self.candidate_accounts.unblock(account, None) {
                        stats.inc(StatType::BootstrapAccountSets, DetailType::Unblock);
                        stats.inc(
                            StatType::BootstrapAccountSets,
                            DetailType::PriorityUnblocked,
                        );
                    } else {
                        stats.inc(StatType::BootstrapAccountSets, DetailType::UnblockFailed);
                    }

                    match self.candidate_accounts.priority_up(&account) {
                        PriorityUpResult::Updated => {
                            stats.inc(StatType::BootstrapAccountSets, DetailType::Prioritize);
                        }
                        PriorityUpResult::Inserted => {
                            stats.inc(StatType::BootstrapAccountSets, DetailType::Prioritize);
                            stats.inc(StatType::BootstrapAccountSets, DetailType::PriorityInsert);
                        }
                        PriorityUpResult::AccountBlocked => {
                            stats.inc(StatType::BootstrapAccountSets, DetailType::PrioritizeFailed);
                        }
                        PriorityUpResult::InvalidAccount => {}
                    }

                    if saved_block.is_send() {
                        let destination = saved_block.destination().unwrap();
                        // Unblocking automatically inserts account into priority set
                        if self.candidate_accounts.unblock(destination, Some(hash)) {
                            stats.inc(StatType::BootstrapAccountSets, DetailType::Unblock);
                            stats.inc(
                                StatType::BootstrapAccountSets,
                                DetailType::PriorityUnblocked,
                            );
                        } else {
                            stats.inc(StatType::BootstrapAccountSets, DetailType::UnblockFailed);
                        }
                        if self.candidate_accounts.priority_set_initial(&destination) {
                            stats.inc(StatType::BootstrapAccountSets, DetailType::PriorityInsert);
                        } else {
                            stats.inc(StatType::BootstrapAccountSets, DetailType::PrioritizeFailed);
                        };
                    }
                }
            }
            BlockStatus::GapSource => {
                // Prevent malicious live traffic from filling up the blocked set
                if source == BlockSource::Bootstrap {
                    let source = block.source_or_link();

                    if !account.is_zero() && !source.is_zero() {
                        // Mark account as blocked because it is missing the source block
                        let blocked = self.candidate_accounts.block(*account, source);
                        if blocked {
                            stats.inc(
                                StatType::BootstrapAccountSets,
                                DetailType::PriorityEraseBlock,
                            );
                            stats.inc(StatType::BootstrapAccountSets, DetailType::Block);
                        } else {
                            stats.inc(StatType::BootstrapAccountSets, DetailType::BlockFailed);
                        }
                    }
                }
            }
            BlockStatus::GapPrevious => {
                // Prevent live traffic from evicting accounts from the priority list
                if source == BlockSource::Live
                    && !self.candidate_accounts.priority_half_full()
                    && !self.candidate_accounts.blocked_half_full()
                {
                    if block.block_type() == BlockType::State {
                        let account = block.account_field().unwrap();
                        if self.candidate_accounts.priority_set_initial(&account) {
                            stats.inc(StatType::BootstrapAccountSets, DetailType::PriorityInsert);
                        } else {
                            stats.inc(StatType::BootstrapAccountSets, DetailType::PrioritizeFailed);
                        }
                    }
                }
            }
            BlockStatus::GapEpochOpenPending => {
                // Epoch open blocks for accounts that don't have any pending blocks yet
                if self.candidate_accounts.priority_erase(account) {
                    stats.inc(StatType::BootstrapAccountSets, DetailType::PriorityErase);
                }
            }
            _ => {
                // No need to handle other cases
                // TODO: If we receive blocks that are invalid (bad signature, fork, etc.),
                // we should penalize the peer that sent them
            }
        }
    }

    fn count_tags_by_hash(&self, hash: &BlockHash, source: QuerySource) -> usize {
        self.running_queries
            .iter_hash(hash)
            .filter(|i| i.source == source)
            .count()
    }

    pub fn next_priority(&mut self, now: Timestamp) -> PriorityResult {
        let next = self.candidate_accounts.next_priority(now, |account| {
            self.running_queries
                .count_by_account(account, QuerySource::Priority)
                < 4
        });

        if next.account.is_zero() {
            return Default::default();
        }

        self.stats
            .inc(StatType::BootstrapNext, DetailType::NextPriority);

        next
    }

    /* Waits for next available blocking block */
    pub fn next_blocking(&self) -> BlockHash {
        let blocking = self
            .candidate_accounts
            .next_blocking(|hash| self.count_tags_by_hash(hash, QuerySource::Dependencies) == 0);

        if blocking.is_zero() {
            return blocking;
        }

        self.stats
            .inc(StatType::BootstrapNext, DetailType::NextBlocking);

        blocking
    }

    fn cleanup_and_sync(&mut self, account_count: u64, stats: &Stats, now: Timestamp) {
        let channels = self.network.read().unwrap().list_realtime_channels(0);
        self.scoring.sync(channels);
        self.scoring.timeout();

        self.throttle.resize(compute_throttle_size(
            account_count,
            self.config.throttle_coefficient,
        ));

        let should_timeout = |query: &RunningQuery| query.response_cutoff < now;

        while let Some(front) = self.running_queries.front() {
            if !should_timeout(front) {
                break;
            }

            stats.inc(StatType::Bootstrap, DetailType::Timeout);
            stats.inc(StatType::BootstrapTimeout, front.query_type.into());
            self.running_queries.pop_front();
        }

        if self.sync_dependencies_interval.elapsed() >= Duration::from_secs(60) {
            self.sync_dependencies_interval = Instant::now();
            stats.inc(StatType::Bootstrap, DetailType::SyncDependencies);
            let inserted = self.candidate_accounts.sync_dependencies();
            if inserted > 0 {
                stats.add(
                    StatType::BootstrapAccountSets,
                    DetailType::PriorityInsert,
                    inserted as u64,
                );
                stats.add(
                    StatType::BootstrapAccountSets,
                    DetailType::DependencySynced,
                    inserted as u64,
                );
            }
        }
    }

    pub fn query_sent(&mut self, id: u64, now: Timestamp) {
        self.running_queries.modify(id, |query| {
            // After the request has been sent, the peer has a limited time to respond
            query.response_cutoff = now + self.config.request_timeout;
        });
    }

    pub fn send_query_failed(&mut self, id: u64) {
        self.running_queries.remove(id);
    }

    pub fn send(&mut self, channel: &Channel, request: &Message, id: u64, now: Timestamp) -> bool {
        let sent =
            self.message_sender
                .try_send_channel(channel, &request, TrafficType::BootstrapRequests);

        if sent {
            self.stats.inc(StatType::Bootstrap, DetailType::Request);
            let query_type = QueryType::from(request);
            self.stats
                .inc(StatType::BootstrapRequest, query_type.into());
        } else {
            self.stats
                .inc(StatType::Bootstrap, DetailType::RequestFailed);
        }

        if sent {
            self.query_sent(id, now);
        } else {
            self.send_query_failed(id);
        }

        sent
    }

    pub fn container_info(&self, database_limiter: &RateLimiter) -> ContainerInfo {
        let limiters: ContainerInfo = [
            ("total", self.limiter.size(), 0),
            ("database", database_limiter.size(), 0),
            ("frontiers", self.frontiers_limiter.size(), 0),
        ]
        .into();

        ContainerInfo::builder()
            .leaf(
                "tags",
                self.running_queries.len(),
                RunningQueryContainer::ELEMENT_SIZE,
            )
            .leaf("throttle", self.throttle.len(), 0)
            .leaf("throttle_success", self.throttle.successes(), 0)
            .node("accounts", self.candidate_accounts.container_info())
            .node("frontiers", self.account_ranges.container_info())
            .node("peers", self.scoring.container_info())
            .node("limiters", limiters)
            .finish()
    }
}

// Calculates a lookback size based on the size of the ledger where larger ledgers have a larger sample count
fn compute_throttle_size(account_count: u64, throttle_coefficient: usize) -> usize {
    let target = if account_count > 0 {
        throttle_coefficient * ((account_count as f64).ln() as usize)
    } else {
        0
    };
    const MIN_SIZE: usize = 16;
    max(target, MIN_SIZE)
}

/// Verifies whether the received response is valid. Returns:
/// - invalid: when received blocks do not correspond to requested hash/account or they do not make a valid chain
/// - nothing_new: when received response indicates that the account chain does not have more blocks
/// - ok: otherwise, if all checks pass
fn verify_response(response: &BlocksAckPayload, query: &RunningQuery) -> VerifyResult {
    let blocks = response.blocks();
    if blocks.is_empty() {
        return VerifyResult::NothingNew;
    }
    if blocks.len() == 1 && blocks.front().unwrap().hash() == query.start.into() {
        return VerifyResult::NothingNew;
    }
    if blocks.len() > query.count {
        return VerifyResult::Invalid;
    }

    let first = blocks.front().unwrap();
    match query.query_type {
        QueryType::BlocksByHash => {
            if first.hash() != query.start.into() {
                // TODO: Stat & log
                return VerifyResult::Invalid;
            }
        }
        QueryType::BlocksByAccount => {
            // Open & state blocks always contain account field
            if first.account_field().unwrap() != query.start.into() {
                // TODO: Stat & log
                return VerifyResult::Invalid;
            }
        }
        QueryType::AccountInfoByHash | QueryType::Frontiers | QueryType::Invalid => {
            return VerifyResult::Invalid;
        }
    }

    // Verify blocks make a valid chain
    let mut previous_hash = first.hash();
    for block in blocks.iter().skip(1) {
        if block.previous() != previous_hash {
            // TODO: Stat & log
            return VerifyResult::Invalid; // Blocks do not make a chain
        }
        previous_hash = block.hash();
    }

    VerifyResult::Ok
}

impl From<&Message> for QueryType {
    fn from(value: &Message) -> Self {
        if let Message::AscPullReq(req) = value {
            match &req.req_type {
                AscPullReqType::Blocks(b) => match b.start_type {
                    HashType::Account => QueryType::BlocksByAccount,
                    HashType::Block => QueryType::BlocksByHash,
                },
                AscPullReqType::AccountInfo(_) => QueryType::AccountInfoByHash,
                AscPullReqType::Frontiers(_) => QueryType::Frontiers,
            }
        } else {
            QueryType::Invalid
        }
    }
}

fn process_frontiers(
    ledger: Arc<Ledger>,
    stats: Arc<Stats>,
    frontiers: Vec<Frontier>,
    mutex: Arc<Mutex<BootstrapLogic>>,
) {
    assert!(!frontiers.is_empty());

    stats.inc(StatType::Bootstrap, DetailType::ProcessingFrontiers);
    let mut outdated = 0;
    let mut pending = 0;

    // Accounts with outdated frontiers to sync
    let mut result = Vec::new();
    {
        let tx = ledger.read_txn();

        let start = frontiers[0].account;
        let mut account_crawler = AccountDatabaseCrawler::new(&ledger, &tx);
        let mut pending_crawler = PendingDatabaseCrawler::new(&ledger, &tx);
        account_crawler.initialize(start);
        pending_crawler.initialize(start);

        let mut should_prioritize = |frontier: &Frontier| {
            account_crawler.advance_to(&frontier.account);
            pending_crawler.advance_to(&frontier.account);

            // Check if account exists in our ledger
            if let Some((cur_acc, info)) = &account_crawler.current {
                if *cur_acc == frontier.account {
                    // Check for frontier mismatch
                    if info.head != frontier.hash {
                        // Check if frontier block exists in our ledger
                        if !ledger.any().block_exists_or_pruned(&tx, &frontier.hash) {
                            outdated += 1;
                            return true; // Frontier is outdated
                        }
                    }
                    return false; // Account exists and frontier is up-to-date
                }
            }

            // Check if account has pending blocks in our ledger
            if let Some((key, _)) = &pending_crawler.current {
                if key.receiving_account == frontier.account {
                    pending += 1;
                    return true; // Account doesn't exist but has pending blocks in the ledger
                }
            }

            false // Account doesn't exist in the ledger and has no pending blocks, can't be prioritized right now
        };

        for frontier in &frontiers {
            if should_prioritize(frontier) {
                result.push(frontier.account);
            }
        }
    }

    stats.add(
        StatType::BootstrapFrontiers,
        DetailType::Processed,
        frontiers.len() as u64,
    );
    stats.add(
        StatType::BootstrapFrontiers,
        DetailType::Prioritized,
        result.len() as u64,
    );
    stats.add(StatType::BootstrapFrontiers, DetailType::Outdated, outdated);
    stats.add(StatType::BootstrapFrontiers, DetailType::Pending, pending);

    debug!(
        "Processed {} frontiers of which outdated: {}, pending: {}",
        frontiers.len(),
        outdated,
        pending
    );

    let mut guard = mutex.lock().unwrap();
    for account in result {
        // Use the lowest possible priority here
        guard
            .candidate_accounts
            .priority_set(&account, CandidateAccounts::PRIORITY_CUTOFF);
    }
}
