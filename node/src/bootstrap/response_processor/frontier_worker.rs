use super::frontier_checker::FrontierChecker;
use crate::{
    bootstrap::state::{BootstrapState, CandidateAccounts},
    stats::{DetailType, StatType, Stats},
};
use rsnano_core::Frontier;
use rsnano_ledger::Ledger;
use rsnano_store_lmdb::LmdbReadTransaction;
use std::sync::Mutex;
use tracing::debug;

/// Handles received frontiers
pub(crate) struct FrontierWorker<'a> {
    stats: &'a Stats,
    state: &'a Mutex<BootstrapState>,
    checker: FrontierChecker<'a>,
}

impl<'a> FrontierWorker<'a> {
    pub(crate) fn new(
        ledger: &'a Ledger,
        tx: &'a LmdbReadTransaction,
        stats: &'a Stats,
        state: &'a Mutex<BootstrapState>,
    ) -> Self {
        Self {
            stats,
            state,
            checker: FrontierChecker::new(ledger, tx),
        }
    }

    pub fn process(&mut self, frontiers: Vec<Frontier>) {
        assert!(!frontiers.is_empty());

        self.stats
            .inc(StatType::Bootstrap, DetailType::ProcessingFrontiers);

        let outdated = self.checker.get_outdated_accounts(&frontiers);

        self.stats.add(
            StatType::BootstrapFrontiers,
            DetailType::Processed,
            frontiers.len() as u64,
        );
        self.stats.add(
            StatType::BootstrapFrontiers,
            DetailType::Prioritized,
            outdated.accounts.len() as u64,
        );
        self.stats.add(
            StatType::BootstrapFrontiers,
            DetailType::Outdated,
            outdated.outdated as u64,
        );
        self.stats.add(
            StatType::BootstrapFrontiers,
            DetailType::Pending,
            outdated.pending as u64,
        );

        debug!(
            "Processed {} frontiers of which outdated: {}, pending: {}",
            frontiers.len(),
            outdated.outdated,
            outdated.pending
        );

        let mut guard = self.state.lock().unwrap();
        for account in outdated.accounts {
            // Use the lowest possible priority here
            guard
                .candidate_accounts
                .priority_set(&account, CandidateAccounts::PRIORITY_CUTOFF);
        }
    }
}
