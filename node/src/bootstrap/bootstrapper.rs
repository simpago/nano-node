use super::{
    cleanup::BootstrapCleanup,
    dependency_query::DependencyQuery,
    frontier_scan::{AccountRanges, AccountRangesConfig, FrontierQuery},
    peer_scoring::PeerScoring,
    priority_query::PriorityQuery,
    response_handler::ResponseHandler,
    running_query_container::{QuerySource, QueryType, RunningQuery, RunningQueryContainer},
    AscPullQuerySpec, BootstrapAction, CandidateAccounts, CandidateAccountsConfig, PriorityResult,
    PriorityUpResult,
};
use crate::{
    block_processing::{BlockContext, BlockProcessor, BlockSource, LedgerNotifications},
    bootstrap::{channel_waiter::ChannelWaiter, WaitResult},
    stats::{DetailType, StatType, Stats},
    transport::MessageSender,
    utils::ThreadPoolImpl,
};
use rand::{thread_rng, RngCore};
use rsnano_core::{utils::ContainerInfo, Account, Block, BlockHash, BlockType, SavedBlock};
use rsnano_ledger::{BlockStatus, Ledger};
use rsnano_messages::{
    AscPullAck, AscPullReq, AscPullReqType, BlocksAckPayload, HashType, Message,
};
use rsnano_network::{bandwidth_limiter::RateLimiter, ChannelId, Network, TrafficType};
use rsnano_nullable_clock::{SteadyClock, Timestamp};
use std::{
    cmp::min,
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

pub struct Bootstrapper {
    ledger: Arc<Ledger>,
    block_processor: Arc<BlockProcessor>,
    stats: Arc<Stats>,
    network: Arc<RwLock<Network>>,
    threads: Mutex<Option<Threads>>,
    state: Arc<Mutex<BootstrapState>>,
    condition: Arc<Condvar>,
    config: BootstrapConfig,
    clock: Arc<SteadyClock>,
    response_handler: ResponseHandler,
    workers: Arc<ThreadPoolImpl>,
    message_sender: MessageSender,
    limiter: Arc<RateLimiter>,
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

        let limiter = Arc::new(RateLimiter::new(config.rate_limit));

        let state = Arc::new(Mutex::new(BootstrapState {
            stopped: false,
            candidate_accounts: CandidateAccounts::new(config.candidate_accounts.clone()),
            scoring: PeerScoring::new(config.clone()),
            account_ranges: AccountRanges::new(config.frontier_scan.clone(), stats.clone()),
            running_queries: RunningQueryContainer::default(),
        }));

        let condition = Arc::new(Condvar::new());

        let response_handler = ResponseHandler::new(
            state.clone(),
            stats.clone(),
            block_processor.clone(),
            condition.clone(),
            workers.clone(),
            ledger.clone(),
            config.clone(),
        );

        Self {
            threads: Mutex::new(None),
            state,
            condition,
            config,
            stats,
            block_processor,
            ledger,
            clock,
            response_handler,
            workers,
            network,
            message_sender,
            limiter,
        }
    }

    pub fn stop(&self) {
        self.state.lock().unwrap().stopped = true;
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
        self.state
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
        let mut guard = self.state.lock().unwrap();
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

    fn run_queries<T: BootstrapAction<AscPullQuerySpec>>(
        &self,
        mut query_factory: T,
        mut message_sender: MessageSender,
    ) {
        loop {
            let Some(spec) = self.wait_for(&mut query_factory) else {
                return;
            };
            self.send_request(spec, &mut message_sender);
        }
    }

    fn send_request(&self, spec: AscPullQuerySpec, message_sender: &mut MessageSender) {
        let id = thread_rng().next_u64();
        let now = self.clock.now();
        let query = RunningQuery::from_request(id, &spec, now, self.config.request_timeout);

        let request = AscPullReq {
            id,
            req_type: spec.req_type,
        };

        let mut guard = self.state.lock().unwrap();
        guard.running_queries.insert(query);
        let message = Message::AscPullReq(request);
        let sent = message_sender.try_send_channel(
            &spec.channel,
            &message,
            TrafficType::BootstrapRequests,
        );

        if sent {
            self.stats.inc(StatType::Bootstrap, DetailType::Request);
            let query_type = QueryType::from(&message);
            self.stats
                .inc(StatType::BootstrapRequest, query_type.into());
        } else {
            self.stats
                .inc(StatType::Bootstrap, DetailType::RequestFailed);
        }

        if sent {
            // After the request has been sent, the peer has a limited time to respond
            let response_cutoff = now + self.config.request_timeout;
            guard.set_response_cutoff(id, response_cutoff);
        } else {
            guard.remove_query(id);
        }
        if sent && spec.cooldown_account {
            guard.candidate_accounts.timestamp_set(&spec.account, now);
        }
    }

    fn run_timeouts(&self) {
        let mut cleanup =
            BootstrapCleanup::new(self.clock.clone(), self.stats.clone(), self.network.clone());
        let mut guard = self.state.lock().unwrap();
        while !guard.stopped {
            cleanup.cleanup(&mut guard);

            guard = self
                .condition
                .wait_timeout_while(guard, Duration::from_secs(1), |g| !g.stopped)
                .unwrap()
                .0;
        }
    }

    /// Process `asc_pull_ack` message coming from network
    pub fn process(&self, message: AscPullAck, channel_id: ChannelId) {
        self.response_handler
            .process(message, channel_id, self.clock.now());
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
            let mut guard = self.state.lock().unwrap();
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

        let limiter = self.limiter.clone();
        let max_requests = self.config.max_requests;
        let channel_waiter = Arc::new(move || ChannelWaiter::new(limiter.clone(), max_requests));

        let frontiers = if self.config.enable_frontier_scan {
            Some(spawn_query(
                "Bootstrap front",
                FrontierQuery::new(
                    self.workers.clone(),
                    self.stats.clone(),
                    self.config.frontier_rate_limit,
                    self.config.frontier_scan.max_pending,
                    channel_waiter.clone(),
                ),
                self.clone(),
            ))
        } else {
            None
        };

        let priorities = if self.config.enable_scan {
            Some(spawn_query(
                "Bootstrap",
                PriorityQuery::new(
                    self.ledger.clone(),
                    self.block_processor.clone(),
                    self.stats.clone(),
                    channel_waiter.clone(),
                    self.config.clone(),
                ),
                self.clone(),
            ))
        } else {
            None
        };

        let dependencies = if self.config.enable_dependency_walker {
            Some(spawn_query(
                "Bootstrap walkr",
                DependencyQuery::new(self.stats.clone(), channel_waiter),
                self.clone(),
            ))
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

pub(super) struct BootstrapState {
    stopped: bool,
    pub candidate_accounts: CandidateAccounts,
    pub scoring: PeerScoring,
    pub running_queries: RunningQueryContainer,
    pub account_ranges: AccountRanges,
}

impl BootstrapState {
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

        blocking
    }

    pub fn set_response_cutoff(&mut self, id: u64, response_cutoff: Timestamp) {
        self.running_queries.modify(id, |query| {
            // After the request has been sent, the peer has a limited time to respond
            query.response_cutoff = response_cutoff;
        });
    }

    pub fn remove_query(&mut self, id: u64) {
        self.running_queries.remove(id);
    }

    pub fn container_info(&self) -> ContainerInfo {
        ContainerInfo::builder()
            .leaf(
                "tags",
                self.running_queries.len(),
                RunningQueryContainer::ELEMENT_SIZE,
            )
            .node("accounts", self.candidate_accounts.container_info())
            .node("frontiers", self.account_ranges.container_info())
            .node("peers", self.scoring.container_info())
            .finish()
    }
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

fn spawn_query<T>(
    name: impl Into<String>,
    query_factory: T,
    bootstrapper: Arc<Bootstrapper>,
) -> JoinHandle<()>
where
    T: BootstrapAction<AscPullQuerySpec> + Send + 'static,
{
    let message_sender = bootstrapper.message_sender.clone();
    std::thread::Builder::new()
        .name(name.into())
        .spawn(Box::new(move || {
            bootstrapper.run_queries(query_factory, message_sender)
        }))
        .unwrap()
}
