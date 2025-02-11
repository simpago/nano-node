use super::state::{BootstrapState, PriorityUpResult};
use crate::{
    block_processing::{BlockContext, BlockSource},
    stats::{DetailType, StatType, Stats},
};
use rsnano_core::{Account, Block, BlockType, SavedBlock};
use rsnano_ledger::{BlockStatus, Ledger};
use rsnano_store_lmdb::LmdbReadTransaction;
use std::sync::{Arc, Condvar, Mutex};

/// Inspects a processed block and adjusts the bootstrap state accordingly
pub(super) struct BlockInspector {
    state: Arc<Mutex<BootstrapState>>,
    state_changed: Arc<Condvar>,
    ledger: Arc<Ledger>,
    stats: Arc<Stats>,
}

impl BlockInspector {
    pub(super) fn new(
        state: Arc<Mutex<BootstrapState>>,
        state_changed: Arc<Condvar>,
        ledger: Arc<Ledger>,
        stats: Arc<Stats>,
    ) -> Self {
        Self {
            state,
            state_changed,
            ledger,
            stats,
        }
    }

    pub fn inspect(&self, batch: &[(BlockStatus, Arc<BlockContext>)]) {
        let mut state = self.state.lock().unwrap();
        let tx = self.ledger.read_txn();
        for (result, context) in batch {
            let block = context.block.lock().unwrap().clone();
            let saved_block = context.saved_block.lock().unwrap().clone();
            let account = self.get_account(&tx, &block, &saved_block);

            self.inspect_block(
                &mut state,
                *result,
                &block,
                saved_block,
                context.source,
                &account,
            );
        }
    }

    fn get_account(
        &self,
        tx: &LmdbReadTransaction,
        block: &Block,
        saved_block: &Option<SavedBlock>,
    ) -> Account {
        match saved_block {
            Some(b) => b.account(),
            None => block.account_field().unwrap_or_else(|| {
                self.ledger
                    .any()
                    .block_account(tx, &block.previous())
                    .unwrap_or_default()
            }),
        }
    }

    /// Inspects a block that has been processed by the block processor
    /// - Marks an account as blocked if the result code is gap source as there is no reason request additional blocks for this account until the dependency is resolved
    /// - Marks an account as forwarded if it has been recently referenced by a block that has been inserted.
    fn inspect_block(
        &self,
        state: &mut BootstrapState,
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
                    if state.candidate_accounts.unblock(account, None) {
                        self.stats
                            .inc(StatType::BootstrapAccountSets, DetailType::Unblock);
                        self.stats.inc(
                            StatType::BootstrapAccountSets,
                            DetailType::PriorityUnblocked,
                        );
                    } else {
                        self.stats
                            .inc(StatType::BootstrapAccountSets, DetailType::UnblockFailed);
                    }

                    match state.candidate_accounts.priority_up(&account) {
                        PriorityUpResult::Updated => {
                            self.stats
                                .inc(StatType::BootstrapAccountSets, DetailType::Prioritize);
                        }
                        PriorityUpResult::Inserted => {
                            self.stats
                                .inc(StatType::BootstrapAccountSets, DetailType::Prioritize);
                            self.stats
                                .inc(StatType::BootstrapAccountSets, DetailType::PriorityInsert);
                        }
                        PriorityUpResult::AccountBlocked => {
                            self.stats
                                .inc(StatType::BootstrapAccountSets, DetailType::PrioritizeFailed);
                        }
                        PriorityUpResult::InvalidAccount => {}
                    }

                    if saved_block.is_send() {
                        let destination = saved_block.destination().unwrap();
                        // Unblocking automatically inserts account into priority set
                        if state.candidate_accounts.unblock(destination, Some(hash)) {
                            self.stats
                                .inc(StatType::BootstrapAccountSets, DetailType::Unblock);
                            self.stats.inc(
                                StatType::BootstrapAccountSets,
                                DetailType::PriorityUnblocked,
                            );
                        } else {
                            self.stats
                                .inc(StatType::BootstrapAccountSets, DetailType::UnblockFailed);
                        }
                        if state.candidate_accounts.priority_set_initial(&destination) {
                            self.stats
                                .inc(StatType::BootstrapAccountSets, DetailType::PriorityInsert);
                        } else {
                            self.stats
                                .inc(StatType::BootstrapAccountSets, DetailType::PrioritizeFailed);
                        };
                    }
                    self.state_changed.notify_all();
                }
            }
            BlockStatus::GapSource => {
                // Prevent malicious live traffic from filling up the blocked set
                if source == BlockSource::Bootstrap {
                    let source = block.source_or_link();

                    if !account.is_zero() && !source.is_zero() {
                        // Mark account as blocked because it is missing the source block
                        let blocked = state.candidate_accounts.block(*account, source);
                        if blocked {
                            self.state_changed.notify_all();
                            self.stats.inc(
                                StatType::BootstrapAccountSets,
                                DetailType::PriorityEraseBlock,
                            );
                            self.stats
                                .inc(StatType::BootstrapAccountSets, DetailType::Block);
                        } else {
                            self.stats
                                .inc(StatType::BootstrapAccountSets, DetailType::BlockFailed);
                        }
                    }
                }
            }
            BlockStatus::GapPrevious => {
                // Prevent live traffic from evicting accounts from the priority list
                if source == BlockSource::Live
                    && !state.candidate_accounts.priority_half_full()
                    && !state.candidate_accounts.blocked_half_full()
                {
                    if block.block_type() == BlockType::State {
                        let account = block.account_field().unwrap();
                        if state.candidate_accounts.priority_set_initial(&account) {
                            self.state_changed.notify_all();
                            self.stats
                                .inc(StatType::BootstrapAccountSets, DetailType::PriorityInsert);
                        } else {
                            self.stats
                                .inc(StatType::BootstrapAccountSets, DetailType::PrioritizeFailed);
                        }
                    }
                }
            }
            BlockStatus::GapEpochOpenPending => {
                // Epoch open blocks for accounts that don't have any pending blocks yet
                if state.candidate_accounts.priority_erase(account) {
                    self.state_changed.notify_all();
                    self.stats
                        .inc(StatType::BootstrapAccountSets, DetailType::PriorityErase);
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
