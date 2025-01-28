use super::{bootstrap_state::BootstrapState, PriorityUpResult};
use crate::{
    block_processing::{BlockContext, BlockSource},
    stats::{DetailType, StatType, Stats},
};
use rsnano_core::{Account, Block, BlockType, SavedBlock};
use rsnano_ledger::{BlockStatus, Ledger};
use std::sync::{Arc, Mutex};

/// Inspects a processed block and adjusts the bootstrap state accordingly
pub(super) struct BlockInspector {
    state: Arc<Mutex<BootstrapState>>,
    ledger: Arc<Ledger>,
    stats: Arc<Stats>,
}

impl BlockInspector {
    pub(super) fn new(
        state: Arc<Mutex<BootstrapState>>,
        ledger: Arc<Ledger>,
        stats: Arc<Stats>,
    ) -> Self {
        Self {
            state,
            ledger,
            stats,
        }
    }

    pub fn inspect(&self, batch: &[(BlockStatus, Arc<BlockContext>)]) {
        let mut guard = self.state.lock().unwrap();
        let tx = self.ledger.read_txn();
        for (result, context) in batch {
            let block = context.block.lock().unwrap().clone();
            let saved_block = context.saved_block.lock().unwrap().clone();
            let account = block.account_field().unwrap_or_else(|| {
                self.ledger
                    .any()
                    .block_account(&tx, &block.previous())
                    .unwrap_or_default()
            });

            self.inspect_block(
                &mut guard,
                &self.stats,
                *result,
                &block,
                saved_block,
                context.source,
                &account,
            );
        }
    }

    /// Inspects a block that has been processed by the block processor
    /// - Marks an account as blocked if the result code is gap source as there is no reason request additional blocks for this account until the dependency is resolved
    /// - Marks an account as forwarded if it has been recently referenced by a block that has been inserted.
    fn inspect_block(
        &self,
        guard: &mut BootstrapState,
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
                    if guard.candidate_accounts.unblock(account, None) {
                        stats.inc(StatType::BootstrapAccountSets, DetailType::Unblock);
                        stats.inc(
                            StatType::BootstrapAccountSets,
                            DetailType::PriorityUnblocked,
                        );
                    } else {
                        stats.inc(StatType::BootstrapAccountSets, DetailType::UnblockFailed);
                    }

                    match guard.candidate_accounts.priority_up(&account) {
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
                        if guard.candidate_accounts.unblock(destination, Some(hash)) {
                            stats.inc(StatType::BootstrapAccountSets, DetailType::Unblock);
                            stats.inc(
                                StatType::BootstrapAccountSets,
                                DetailType::PriorityUnblocked,
                            );
                        } else {
                            stats.inc(StatType::BootstrapAccountSets, DetailType::UnblockFailed);
                        }
                        if guard.candidate_accounts.priority_set_initial(&destination) {
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
                        let blocked = guard.candidate_accounts.block(*account, source);
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
                    && !guard.candidate_accounts.priority_half_full()
                    && !guard.candidate_accounts.blocked_half_full()
                {
                    if block.block_type() == BlockType::State {
                        let account = block.account_field().unwrap();
                        if guard.candidate_accounts.priority_set_initial(&account) {
                            stats.inc(StatType::BootstrapAccountSets, DetailType::PriorityInsert);
                        } else {
                            stats.inc(StatType::BootstrapAccountSets, DetailType::PrioritizeFailed);
                        }
                    }
                }
            }
            BlockStatus::GapEpochOpenPending => {
                // Epoch open blocks for accounts that don't have any pending blocks yet
                if guard.candidate_accounts.priority_erase(account) {
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
}
