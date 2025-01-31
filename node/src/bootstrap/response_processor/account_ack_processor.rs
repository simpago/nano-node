use crate::{
    bootstrap::state::{BootstrapState, CandidateAccounts, RunningQuery},
    stats::{DetailType, StatType, Stats},
};
use rsnano_messages::AccountInfoAckPayload;
use std::sync::{Arc, Mutex};

pub(crate) struct AccountAckProcessor {
    stats: Arc<Stats>,
    state: Arc<Mutex<BootstrapState>>,
}

impl AccountAckProcessor {
    pub(crate) fn new(stats: Arc<Stats>, state: Arc<Mutex<BootstrapState>>) -> Self {
        Self { stats, state }
    }

    pub fn process(&self, query: &RunningQuery, response: &AccountInfoAckPayload) -> bool {
        if response.account.is_zero() {
            self.stats
                .inc(StatType::BootstrapProcess, DetailType::AccountInfoEmpty);
            // OK, but nothing to do
            return true;
        }

        self.stats
            .inc(StatType::BootstrapProcess, DetailType::AccountInfo);

        // Prioritize account containing the dependency
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
}
