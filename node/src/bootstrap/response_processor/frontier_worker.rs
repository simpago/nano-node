use super::frontier_checker::{FrontierChecker, OutdatedAccounts};
use crate::{
    bootstrap::state::{BootstrapState, CandidateAccounts},
    stats::{DetailType, StatType, Stats},
};
use rsnano_core::Frontier;
use rsnano_ledger::Ledger;
use rsnano_store_lmdb::LmdbReadTransaction;
use std::sync::Mutex;

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
        let outdated = self.checker.get_outdated_accounts(&frontiers);
        self.update_stats(&frontiers, &outdated);
        let mut guard = self.state.lock().unwrap();
        for account in outdated.accounts {
            // Use the lowest possible priority here
            guard
                .candidate_accounts
                .priority_set(&account, CandidateAccounts::PRIORITY_CUTOFF);
        }
    }

    fn update_stats(&self, frontiers: &[Frontier], outdated: &OutdatedAccounts) {
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
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsnano_core::{Account, AccountInfo, BlockHash};

    #[test]
    fn empty() {
        let ledger = Ledger::new_null();
        let tx = ledger.read_txn();
        let stats = Stats::default();
        let state = Mutex::new(BootstrapState::default());
        let mut worker = FrontierWorker::new(&ledger, &tx, &stats, &state);

        worker.process(Vec::new());

        assert_eq!(state.lock().unwrap().candidate_accounts.priority_len(), 0);
    }

    #[test]
    fn prioritize_one_account() {
        let account = Account::from(1);
        let ledger = Ledger::new_null_builder()
            .account_info(
                &account,
                &AccountInfo {
                    head: BlockHash::from(2),
                    ..Default::default()
                },
            )
            .finish();
        let tx = ledger.read_txn();
        let stats = Stats::default();
        let state = Mutex::new(BootstrapState::default());
        let mut worker = FrontierWorker::new(&ledger, &tx, &stats, &state);

        worker.process(vec![Frontier::new(account, BlockHash::from(3))]);

        let guard = state.lock().unwrap();
        assert_eq!(guard.candidate_accounts.priority_len(), 1);
        assert_eq!(
            guard.candidate_accounts.priority(&account),
            CandidateAccounts::PRIORITY_CUTOFF
        );
    }
}
