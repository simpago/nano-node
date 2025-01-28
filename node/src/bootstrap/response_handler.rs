use super::{
    bootstrap_state::BootstrapState,
    running_query_container::{QueryType, RunningQuery},
    BootstrapConfig, CandidateAccounts, PriorityDownResult,
};
use crate::{
    block_processing::{BlockProcessor, BlockSource},
    bootstrap::crawlers::{AccountDatabaseCrawler, PendingDatabaseCrawler},
    stats::{DetailType, Direction, Sample, StatType, Stats},
    utils::{ThreadPool, ThreadPoolImpl},
};
use rsnano_core::{Account, Frontier};
use rsnano_ledger::Ledger;
use rsnano_messages::{AccountInfoAckPayload, AscPullAck, AscPullAckType, BlocksAckPayload};
use rsnano_network::ChannelId;
use rsnano_nullable_clock::Timestamp;
use std::sync::{Arc, Condvar, Mutex};
use tracing::debug;

pub(super) struct ResponseHandler {
    state: Arc<Mutex<BootstrapState>>,
    stats: Arc<Stats>,
    block_processor: Arc<BlockProcessor>,
    condition: Arc<Condvar>,
    workers: Arc<ThreadPoolImpl>,
    ledger: Arc<Ledger>,
    config: BootstrapConfig,
}

impl ResponseHandler {
    pub fn new(
        state: Arc<Mutex<BootstrapState>>,
        stats: Arc<Stats>,
        block_processor: Arc<BlockProcessor>,
        condition: Arc<Condvar>,
        workers: Arc<ThreadPoolImpl>,
        ledger: Arc<Ledger>,
        config: BootstrapConfig,
    ) -> Self {
        Self {
            state,
            stats,
            block_processor,
            condition,
            workers,
            ledger,
            config,
        }
    }

    pub fn process(&self, message: AscPullAck, channel_id: ChannelId, now: Timestamp) {
        let mut guard = self.state.lock().unwrap();

        // Only process messages that have a known running query
        let Some(query) = guard.running_queries.remove(message.id) else {
            self.stats.inc(StatType::Bootstrap, DetailType::MissingTag);
            return;
        };

        self.stats.inc(StatType::Bootstrap, DetailType::Reply);

        let valid = match message.pull_type {
            AscPullAckType::Blocks(_) => matches!(
                query.query_type,
                QueryType::BlocksByHash | QueryType::BlocksByAccount
            ),
            AscPullAckType::AccountInfo(_) => query.query_type == QueryType::AccountInfoByHash,
            AscPullAckType::Frontiers(_) => query.query_type == QueryType::Frontiers,
        };

        if !valid {
            self.stats
                .inc(StatType::Bootstrap, DetailType::InvalidResponseType);
            return;
        }

        // Track bootstrap request response time
        self.stats
            .inc(StatType::BootstrapReply, query.query_type.into());

        self.stats.sample(
            Sample::BootstrapTagDuration,
            query.sent.elapsed(now).as_millis() as i64,
            (0, self.config.request_timeout.as_millis() as i64),
        );

        drop(guard);
        // Process the response payload
        let ok = match message.pull_type {
            AscPullAckType::Blocks(blocks) => self.process_blocks(&blocks, &query),
            AscPullAckType::AccountInfo(info) => self.process_accounts(&info, &query),
            AscPullAckType::Frontiers(frontiers) => self.process_frontiers(frontiers, &query),
        };

        if ok {
            self.state
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

    pub fn process_blocks(&self, response: &BlocksAckPayload, query: &RunningQuery) -> bool {
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
                        let data = self.state.clone();
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
                    let mut guard = self.state.lock().unwrap();
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

    pub fn process_accounts(&self, response: &AccountInfoAckPayload, query: &RunningQuery) -> bool {
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
            let mut guard = self.state.lock().unwrap();
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

    pub fn process_frontiers(&self, frontiers: Vec<Frontier>, query: &RunningQuery) -> bool {
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
                    let mut guard = self.state.lock().unwrap();
                    guard.account_ranges.process(query.start.into(), &frontiers);
                }

                // Allow some overfill to avoid unnecessarily dropping responses
                if self.workers.num_queued_tasks() < self.config.frontier_scan.max_pending * 4 {
                    let stats = self.stats.clone();
                    let ledger = self.ledger.clone();
                    let mutex = self.state.clone();
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
}

pub(super) enum VerifyResult {
    Ok,
    NothingNew,
    Invalid,
}

///
/// Verifies whether the received response is valid. Returns:
/// - invalid: when received blocks do not correspond to requested hash/account or they do not make a valid chain
/// - nothing_new: when received response indicates that the account chain does not have more blocks
/// - ok: otherwise, if all checks pass
pub(super) fn verify_response(response: &BlocksAckPayload, query: &RunningQuery) -> VerifyResult {
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

fn process_frontiers(
    ledger: Arc<Ledger>,
    stats: Arc<Stats>,
    frontiers: Vec<Frontier>,
    state: Arc<Mutex<BootstrapState>>,
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

    let mut guard = state.lock().unwrap();
    for account in result {
        // Use the lowest possible priority here
        guard
            .candidate_accounts
            .priority_set(&account, CandidateAccounts::PRIORITY_CUTOFF);
    }
}
