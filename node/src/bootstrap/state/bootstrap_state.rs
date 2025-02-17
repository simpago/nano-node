use super::running_query::QuerySource;
use super::{CandidateAccounts, FrontierScan, PeerScoring, PriorityResult, RunningQueryContainer};
use crate::bootstrap::{AscPullQuerySpec, BootstrapConfig};
use rsnano_core::Account;
use rsnano_core::{utils::ContainerInfo, BlockHash};
use rsnano_messages::AscPullReqType;
use rsnano_network::Channel;
use rsnano_nullable_clock::Timestamp;
use std::collections::VecDeque;
use std::sync::Arc;

pub(crate) struct BootstrapState {
    pub candidate_accounts: CandidateAccounts,
    pub scoring: PeerScoring,
    pub running_queries: RunningQueryContainer,
    pub frontier_scan: FrontierScan,
    pub counters: BootstrapCounters,
    pub frontier_ack_processor_busy: bool,
    pub last_outdated_accounts: VecDeque<Account>,
    pub stopped: bool,
}

impl BootstrapState {
    pub fn new(config: BootstrapConfig) -> Self {
        let mut scoring = PeerScoring::new();
        scoring.set_channel_limit(config.channel_limit);

        Self {
            candidate_accounts: CandidateAccounts::new(config.candidate_accounts.clone()),
            scoring,
            frontier_scan: FrontierScan::new(config.frontier_scan.clone()),
            running_queries: RunningQueryContainer::default(),
            counters: BootstrapCounters::default(),
            frontier_ack_processor_busy: false,
            last_outdated_accounts: VecDeque::new(),
            stopped: false,
        }
    }

    pub fn next_blocking_query(&self, channel: &Arc<Channel>) -> Option<AscPullQuerySpec> {
        let next = self.next_blocking();
        if !next.is_zero() {
            Some(Self::create_blocking_query(next, channel.clone()))
        } else {
            None
        }
    }

    fn create_blocking_query(next: BlockHash, channel: Arc<Channel>) -> AscPullQuerySpec {
        AscPullQuerySpec {
            channel,
            req_type: AscPullReqType::account_info_by_hash(next),
            account: Account::zero(),
            hash: next,
            cooldown_account: false,
        }
    }

    fn count_queries_by_hash(&self, hash: &BlockHash, source: QuerySource) -> usize {
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
        self.candidate_accounts
            .next_blocking(|hash| self.count_queries_by_hash(hash, QuerySource::Dependencies) == 0)
    }

    pub fn frontiers_processed(&mut self, outdated: &OutdatedAccounts) {
        self.counters.received_frontiers += outdated.fontiers_received;
        self.counters.outdated_accounts_found += outdated.accounts.len();

        for account in &outdated.accounts {
            // Use the lowest possible priority here
            self.candidate_accounts
                .priority_set(account, CandidateAccounts::PRIORITY_CUTOFF);

            self.last_outdated_accounts.push_back(*account);
            if self.last_outdated_accounts.len() > 20 {
                self.last_outdated_accounts.pop_front();
            }
        }
    }

    pub fn container_info(&self) -> ContainerInfo {
        ContainerInfo::builder()
            .leaf(
                "tags",
                self.running_queries.len(),
                RunningQueryContainer::ELEMENT_SIZE,
            )
            .node("accounts", self.candidate_accounts.container_info())
            .node("frontiers", self.frontier_scan.container_info())
            .node("peers", self.scoring.container_info())
            .finish()
    }
}

impl Default for BootstrapState {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

#[derive(Default, Clone)]
pub struct BootstrapCounters {
    pub received_frontiers: usize,
    pub outdated_accounts_found: usize,
}

#[derive(Default, Debug, PartialEq, Eq)]
pub(crate) struct OutdatedAccounts {
    pub accounts: Vec<Account>,
    /// Accounts that exist but are outdated
    pub outdated: usize,
    /// Accounts that don't exist but have pending blocks in the ledger
    pub pending: usize,
    /// Total count of received frontiers
    pub fontiers_received: usize,
}
