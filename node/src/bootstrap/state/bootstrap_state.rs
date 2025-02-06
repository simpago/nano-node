use super::running_query::QuerySource;
use super::{CandidateAccounts, FrontierScan, PeerScoring, PriorityResult, RunningQueryContainer};
use crate::bootstrap::{AscPullQuerySpec, BootstrapConfig};
use rsnano_core::Account;
use rsnano_core::{utils::ContainerInfo, BlockHash};
use rsnano_messages::AscPullReqType;
use rsnano_network::{Channel, Network};
use rsnano_nullable_clock::Timestamp;
use std::sync::{Arc, RwLock};

pub(crate) struct BootstrapState {
    pub candidate_accounts: CandidateAccounts,
    pub scoring: PeerScoring,
    pub running_queries: RunningQueryContainer,
    pub frontier_scan: FrontierScan,
}

impl BootstrapState {
    pub fn new(config: BootstrapConfig, network: Arc<RwLock<Network>>) -> Self {
        Self {
            candidate_accounts: CandidateAccounts::new(config.candidate_accounts.clone()),
            scoring: PeerScoring::new(config.clone(), network),
            frontier_scan: FrontierScan::new(config.frontier_scan.clone()),
            running_queries: RunningQueryContainer::default(),
        }
    }

    #[cfg(test)]
    pub fn new_test_instance() -> Self {
        Self::new(
            BootstrapConfig::default(),
            Arc::new(RwLock::new(Network::new_test_instance())),
        )
    }

    #[cfg(test)]
    pub fn add_test_channel(&mut self) -> Arc<Channel> {
        self.scoring.add_test_channel()
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
            .node("frontiers", self.frontier_scan.container_info())
            .node("peers", self.scoring.container_info())
            .finish()
    }
}
