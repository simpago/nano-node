use super::{
    frontier_scan::AccountRanges,
    peer_scoring::PeerScoring,
    running_query_container::{QuerySource, RunningQueryContainer},
    BootstrapConfig, CandidateAccounts, PriorityResult, PriorityUpResult,
};
use crate::{
    block_processing::BlockSource,
    stats::{DetailType, StatType, Stats},
};
use rsnano_core::{utils::ContainerInfo, Account, Block, BlockHash, BlockType, SavedBlock};
use rsnano_ledger::BlockStatus;
use rsnano_nullable_clock::Timestamp;
use std::sync::Arc;

pub(super) struct BootstrapState {
    pub candidate_accounts: CandidateAccounts,
    pub scoring: PeerScoring,
    pub running_queries: RunningQueryContainer,
    pub account_ranges: AccountRanges,
}

impl BootstrapState {
    pub fn new(config: BootstrapConfig, stats: Arc<Stats>) -> Self {
        Self {
            candidate_accounts: CandidateAccounts::new(config.candidate_accounts.clone()),
            scoring: PeerScoring::new(config.clone()),
            account_ranges: AccountRanges::new(config.frontier_scan.clone(), stats),
            running_queries: RunningQueryContainer::default(),
        }
    }

    /// Inspects a block that has been processed by the block processor
    /// - Marks an account as blocked if the result code is gap source as there is no reason request additional blocks for this account until the dependency is resolved
    /// - Marks an account as forwarded if it has been recently referenced by a block that has been inserted.
    pub fn inspect(
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
