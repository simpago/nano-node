use crate::{
    bootstrap::{
        response_processor::crawlers::{AccountDatabaseCrawler, PendingDatabaseCrawler},
        state::{BootstrapState, CandidateAccounts},
    },
    stats::{DetailType, StatType, Stats},
};
use rsnano_core::Frontier;
use rsnano_ledger::Ledger;
use std::sync::{Arc, Mutex};
use tracing::debug;

pub(crate) struct FrontierWorker {
    ledger: Arc<Ledger>,
    stats: Arc<Stats>,
    state: Arc<Mutex<BootstrapState>>,
}

impl FrontierWorker {
    pub(crate) fn new(
        ledger: Arc<Ledger>,
        stats: Arc<Stats>,
        state: Arc<Mutex<BootstrapState>>,
    ) -> Self {
        Self {
            ledger,
            stats,
            state,
        }
    }

    pub fn process(&self, frontiers: Vec<Frontier>) {
        assert!(!frontiers.is_empty());

        self.stats
            .inc(StatType::Bootstrap, DetailType::ProcessingFrontiers);
        let mut outdated = 0;
        let mut pending = 0;

        // Accounts with outdated frontiers to sync
        let mut result = Vec::new();
        {
            let tx = self.ledger.read_txn();

            let start = frontiers[0].account;
            let mut account_crawler = AccountDatabaseCrawler::new(&self.ledger, &tx);
            let mut pending_crawler = PendingDatabaseCrawler::new(&self.ledger, &tx);
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
                            if !self
                                .ledger
                                .any()
                                .block_exists_or_pruned(&tx, &frontier.hash)
                            {
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

        self.stats.add(
            StatType::BootstrapFrontiers,
            DetailType::Processed,
            frontiers.len() as u64,
        );
        self.stats.add(
            StatType::BootstrapFrontiers,
            DetailType::Prioritized,
            result.len() as u64,
        );
        self.stats
            .add(StatType::BootstrapFrontiers, DetailType::Outdated, outdated);
        self.stats
            .add(StatType::BootstrapFrontiers, DetailType::Pending, pending);

        debug!(
            "Processed {} frontiers of which outdated: {}, pending: {}",
            frontiers.len(),
            outdated,
            pending
        );

        let mut guard = self.state.lock().unwrap();
        for account in result {
            // Use the lowest possible priority here
            guard
                .candidate_accounts
                .priority_set(&account, CandidateAccounts::PRIORITY_CUTOFF);
        }
    }
}
