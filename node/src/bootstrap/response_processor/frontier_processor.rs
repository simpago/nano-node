use crate::{
    bootstrap::{
        response_processor::crawlers::{AccountDatabaseCrawler, PendingDatabaseCrawler},
        state::{BootstrapState, CandidateAccounts, QueryType, RunningQuery, VerifyResult},
        BootstrapConfig,
    },
    stats::{DetailType, Direction, StatType, Stats},
    utils::{ThreadPool, ThreadPoolImpl},
};
use rsnano_core::Frontier;
use rsnano_ledger::Ledger;
use std::sync::{Arc, Mutex};
use tracing::debug;

pub(crate) struct FrontierProcessor {
    stats: Arc<Stats>,
    ledger: Arc<Ledger>,
    state: Arc<Mutex<BootstrapState>>,
    config: BootstrapConfig,
    workers: Arc<ThreadPoolImpl>,
}

impl FrontierProcessor {
    pub(crate) fn new(
        stats: Arc<Stats>,
        ledger: Arc<Ledger>,
        state: Arc<Mutex<BootstrapState>>,
        config: BootstrapConfig,
        workers: Arc<ThreadPoolImpl>,
    ) -> Self {
        Self {
            stats,
            ledger,
            state,
            config,
            workers,
        }
    }

    pub fn process(&self, frontiers: Vec<Frontier>, query: &RunningQuery) -> bool {
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

        match query.verify_frontiers(&frontiers) {
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
                    self.stats
                        .inc(StatType::BootstrapFrontierScan, DetailType::Process);
                    let done = guard.account_ranges.process(query.start.into(), &frontiers);
                    if done {
                        self.stats
                            .inc(StatType::BootstrapFrontierScan, DetailType::Done);
                    }
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
