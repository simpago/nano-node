use crate::{
    bootstrap::state::{BootstrapState, CandidateAccounts, RunningQuery},
    stats::{DetailType, StatType, Stats},
};
use rsnano_core::{Account, BlockHash};
use rsnano_messages::AccountInfoAckPayload;
use std::sync::{Arc, Mutex};

/// Processes responses to an AscPullReq for account info
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
        self.update_dependency(&mut guard, &query.hash, response.account);
        self.prioritize(&mut guard, &response.account);

        // OK, no way to verify the response
        true
    }

    fn update_dependency(
        &self,
        state: &mut BootstrapState,
        hash: &BlockHash,
        dep_account: Account,
    ) {
        let updated = state
            .candidate_accounts
            .dependency_update(hash, dep_account);

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
    }

    fn prioritize(&self, state: &mut BootstrapState, account: &Account) {
        // Use the lowest possible priority here
        if state
            .candidate_accounts
            .priority_set(account, CandidateAccounts::PRIORITY_CUTOFF)
        {
            self.stats
                .inc(StatType::BootstrapAccountSets, DetailType::PriorityInsert);
        } else {
            self.stats
                .inc(StatType::BootstrapAccountSets, DetailType::PrioritizeFailed);
        };
    }
}

#[cfg(test)]
mod tests {
    use rsnano_core::Account;
    use rsnano_nullable_clock::Timestamp;

    use crate::stats::Direction;

    use super::*;

    #[test]
    fn empty_response() {
        let fixture = Fixture::new();

        let query = RunningQuery::new_test_instance();
        let response = AccountInfoAckPayload {
            account: Account::zero(),
            ..AccountInfoAckPayload::new_test_instance()
        };

        assert!(fixture.processor.process(&query, &response));
        assert_eq!(
            fixture.stats.count(
                StatType::BootstrapProcess,
                DetailType::AccountInfoEmpty,
                Direction::In
            ),
            1
        );
        assert_eq!(
            fixture.stats.count(
                StatType::BootstrapProcess,
                DetailType::AccountInfo,
                Direction::In
            ),
            0
        );
        assert_eq!(
            fixture
                .state
                .lock()
                .unwrap()
                .candidate_accounts
                .priority_len(),
            0
        );
    }

    #[test]
    fn when_not_blocked_should_only_prioritize() {
        let fixture = Fixture::new();
        let query = RunningQuery::new_test_instance();
        let response = AccountInfoAckPayload::new_test_instance();

        assert!(fixture.processor.process(&query, &response));

        assert_eq!(
            fixture.stats.count(
                StatType::BootstrapProcess,
                DetailType::AccountInfo,
                Direction::In
            ),
            1
        );

        assert!(fixture
            .state
            .lock()
            .unwrap()
            .candidate_accounts
            .prioritized(&response.account));

        assert_eq!(
            fixture.stats.count(
                StatType::BootstrapAccountSets,
                DetailType::DependencyUpdateFailed,
                Direction::In
            ),
            1
        );
        assert_eq!(
            fixture.stats.count(
                StatType::BootstrapAccountSets,
                DetailType::PriorityInsert,
                Direction::In
            ),
            1
        );
    }

    #[test]
    fn update_dependency() {
        let fixture = Fixture::new();

        let blocked_account = Account::from(100);
        let unknown_source = BlockHash::from(42);
        let source_account = Account::from(200);

        let query = RunningQuery {
            hash: unknown_source,
            ..RunningQuery::new_test_instance()
        };

        let response = AccountInfoAckPayload {
            account: source_account,
            ..AccountInfoAckPayload::new_test_instance()
        };

        {
            let mut guard = fixture.state.lock().unwrap();
            guard
                .candidate_accounts
                .priority_set_initial(&blocked_account);
            guard
                .candidate_accounts
                .block(blocked_account, unknown_source);
        }

        assert!(fixture.processor.process(&query, &response));

        let mut guard = fixture.state.lock().unwrap();
        assert!(guard.candidate_accounts.blocked(&blocked_account));
        assert!(guard.candidate_accounts.prioritized(&source_account));
        let query = guard.next_priority(Timestamp::new_test_instance());
        assert_eq!(query.account, source_account);

        assert_eq!(
            fixture.stats.count(
                StatType::BootstrapAccountSets,
                DetailType::DependencyUpdate,
                Direction::In
            ),
            1
        );
        assert_eq!(
            fixture.stats.count(
                StatType::BootstrapAccountSets,
                DetailType::PriorityInsert,
                Direction::In
            ),
            1
        );
    }

    #[test]
    fn dependency_update_fails() {
        let fixture = Fixture::new();

        let blocked_account = Account::from(100);
        let unknown_source = BlockHash::from(42);
        let source_account = Account::from(200);

        let query = RunningQuery {
            hash: unknown_source,
            ..RunningQuery::new_test_instance()
        };

        let response = AccountInfoAckPayload {
            account: source_account,
            ..AccountInfoAckPayload::new_test_instance()
        };

        {
            let mut guard = fixture.state.lock().unwrap();
            guard
                .candidate_accounts
                .priority_set_initial(&blocked_account);
            guard
                .candidate_accounts
                .block(blocked_account, unknown_source);
            guard
                .candidate_accounts
                .dependency_update(&unknown_source, source_account);
            guard
                .candidate_accounts
                .priority_set_initial(&source_account);
        }

        assert!(fixture.processor.process(&query, &response));

        assert_eq!(
            fixture.stats.count(
                StatType::BootstrapAccountSets,
                DetailType::DependencyUpdateFailed,
                Direction::In
            ),
            1
        );
        assert_eq!(
            fixture.stats.count(
                StatType::BootstrapAccountSets,
                DetailType::PrioritizeFailed,
                Direction::In
            ),
            1
        );
    }

    struct Fixture {
        stats: Arc<Stats>,
        state: Arc<Mutex<BootstrapState>>,
        processor: AccountAckProcessor,
    }

    impl Fixture {
        fn new() -> Self {
            let stats = Arc::new(Stats::default());
            let state = Arc::new(Mutex::new(BootstrapState::default()));
            let processor = AccountAckProcessor::new(stats.clone(), state.clone());

            Self {
                stats,
                state,
                processor,
            }
        }
    }
}
