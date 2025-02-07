use crate::command_handler::RpcCommandHandler;
use anyhow::anyhow;
use rsnano_core::{Account, Block, BlockBase, BlockHash, SavedBlock};
use rsnano_ledger::Ledger;
use rsnano_rpc_messages::{
    unwrap_bool_or_false, unwrap_u64_or_zero, AccountHistoryArgs, AccountHistoryResponse,
    BlockSubTypeDto, BlockTypeDto, HistoryEntry,
};
use rsnano_store_lmdb::LmdbReadTransaction;

impl RpcCommandHandler {
    pub(crate) fn account_history(
        &self,
        args: AccountHistoryArgs,
    ) -> anyhow::Result<AccountHistoryResponse> {
        let helper = AccountHistoryHelper::new(&self.node.ledger, args);
        helper.account_history()
    }
}

pub(crate) struct AccountHistoryHelper<'a> {
    pub ledger: &'a Ledger,
    pub accounts_to_filter: Vec<Account>,
    pub reverse: bool,
    pub offset: u64,
    pub head: Option<BlockHash>,
    pub requested_account: Option<Account>,
    pub output_raw: bool,
    pub count: u64,
    pub current_block_hash: BlockHash,
    pub account: Account,
}

impl<'a> AccountHistoryHelper<'a> {
    fn new(ledger: &'a Ledger, args: AccountHistoryArgs) -> Self {
        Self {
            ledger,
            accounts_to_filter: args.account_filter.unwrap_or_default(),
            reverse: unwrap_bool_or_false(args.reverse),
            offset: unwrap_u64_or_zero(args.offset),
            head: args.head,
            requested_account: args.account,
            output_raw: unwrap_bool_or_false(args.raw),
            count: args.count.into(),
            current_block_hash: BlockHash::zero(),
            account: Account::zero(),
        }
    }

    fn initialize(&mut self, tx: &LmdbReadTransaction) -> anyhow::Result<()> {
        self.current_block_hash = self.hash_of_first_block(tx)?;
        self.account = self
            .ledger
            .any()
            .block_account(tx, &self.current_block_hash)
            .ok_or_else(|| anyhow!(RpcCommandHandler::BLOCK_NOT_FOUND))?;
        Ok(())
    }

    fn hash_of_first_block(&self, tx: &LmdbReadTransaction) -> anyhow::Result<BlockHash> {
        let hash = if let Some(head) = &self.head {
            *head
        } else {
            let account = self
                .requested_account
                .ok_or_else(|| anyhow!("account argument missing"))?;

            if self.reverse {
                self.ledger
                    .any()
                    .get_account(tx, &account)
                    .ok_or_else(|| anyhow!("Account not found"))?
                    .open_block
            } else {
                self.ledger
                    .any()
                    .account_head(tx, &account)
                    .ok_or_else(|| anyhow!("Account not found"))?
            }
        };

        Ok(hash)
    }

    pub(crate) fn account_history(mut self) -> anyhow::Result<AccountHistoryResponse> {
        let tx = self.ledger.read_txn();
        self.initialize(&tx)?;
        let mut history = Vec::new();
        let mut next_block = self.ledger.any().get_block(&tx, &self.current_block_hash);
        while let Some(block) = next_block {
            if self.count == 0 {
                break;
            }

            if self.offset > 0 {
                self.offset -= 1;
            } else {
                if let Some(entry) = self.entry_for(&block, &tx) {
                    history.push(entry);
                    self.count -= 1;
                }
            }

            next_block = self.go_to_next_block(&tx, &block);
        }

        Ok(self.create_response(history))
    }

    fn go_to_next_block(&mut self, tx: &LmdbReadTransaction, block: &Block) -> Option<SavedBlock> {
        self.current_block_hash = if self.reverse {
            self.ledger
                .any()
                .block_successor(tx, &self.current_block_hash)
                .unwrap_or_default()
        } else {
            block.previous()
        };
        self.ledger.any().get_block(tx, &self.current_block_hash)
    }

    fn should_ignore_account(&self, account: &Account) -> bool {
        if self.accounts_to_filter.is_empty() {
            return false;
        }
        !self.accounts_to_filter.contains(account)
    }

    pub(crate) fn entry_for(
        &self,
        block: &SavedBlock,
        tx: &LmdbReadTransaction,
    ) -> Option<HistoryEntry> {
        let mut entry = match &**block {
            Block::LegacySend(b) => {
                let mut entry = empty_entry();
                entry.block_type = Some(BlockTypeDto::Send);
                entry.account = Some(self.account);
                if let Some(amount) = self.ledger.any().block_amount_for(tx, block) {
                    entry.amount = Some(amount);
                } else {
                    entry.destination = Some(self.account);
                    entry.balance = Some(b.balance());
                    entry.previous = Some(b.previous());
                }
                Some(entry)
            }
            Block::LegacyReceive(b) => {
                let mut entry = empty_entry();
                entry.block_type = Some(BlockTypeDto::Receive);
                if let Some(amount) = self.ledger.any().block_amount_for(tx, block) {
                    if let Some(source_account) = self.ledger.any().block_account(tx, &b.source()) {
                        entry.account = Some(source_account);
                    }
                    entry.amount = Some(amount);
                }
                if self.output_raw {
                    entry.source = Some(b.source());
                    entry.previous = Some(b.previous());
                }
                Some(entry)
            }
            Block::LegacyOpen(b) => {
                let mut entry = empty_entry();
                if self.output_raw {
                    entry.block_type = Some(BlockTypeDto::Open);
                    entry.representative = Some(b.representative().into());
                    entry.source = Some(b.source());
                    entry.opened = Some(b.account());
                } else {
                    // Report opens as a receive
                    entry.block_type = Some(BlockTypeDto::Receive);
                }

                if b.source() != self.ledger.constants.genesis_account.into() {
                    if let Some(amount) = self.ledger.any().block_amount_for(tx, block) {
                        entry.account = self.ledger.any().block_account(tx, &b.source());
                        entry.amount = Some(amount);
                    }
                } else {
                    entry.account = Some(self.ledger.constants.genesis_account);
                    entry.amount = Some(self.ledger.constants.genesis_amount);
                }
                Some(entry)
            }
            Block::LegacyChange(b) => {
                if self.output_raw {
                    let mut entry = empty_entry();
                    entry.block_type = Some(BlockTypeDto::Change);
                    entry.representative = Some(b.mandatory_representative().into());
                    entry.previous = Some(b.previous());
                    Some(entry)
                } else {
                    None
                }
            }
            Block::State(b) => {
                let mut entry = empty_entry();
                if self.output_raw {
                    entry.block_type = Some(BlockTypeDto::State);
                    entry.representative = Some(b.representative().into());
                    entry.link = Some(b.link());
                    entry.balance = Some(b.balance());
                    entry.previous = Some(b.previous());
                }

                let balance = b.balance();
                let previous_balance_raw = self.ledger.any().block_balance(tx, &b.previous());
                let previous_balance = previous_balance_raw.unwrap_or_default();
                if !b.previous().is_zero() && previous_balance_raw.is_none() {
                    // If previous hash is non-zero and we can't query the balance, e.g. it's pruned, we can't determine the block type
                    if self.output_raw {
                        entry.subtype = Some(BlockSubTypeDto::Unknown);
                    } else {
                        entry.block_type = Some(BlockTypeDto::Unknown);
                    }
                    Some(entry)
                } else if balance < previous_balance {
                    if self.should_ignore_account(&b.link().into()) {
                        None
                    } else {
                        if self.output_raw {
                            entry.subtype = Some(BlockSubTypeDto::Send);
                        } else {
                            entry.block_type = Some(BlockTypeDto::Send);
                        }
                        entry.account = Some(b.link().into());
                        entry.amount = Some(previous_balance - b.balance());
                        Some(entry)
                    }
                } else {
                    if b.link().is_zero() {
                        if self.output_raw && self.accounts_to_filter.is_empty() {
                            entry.subtype = Some(BlockSubTypeDto::Change);
                            Some(entry)
                        } else {
                            None
                        }
                    } else if balance == previous_balance && self.ledger.is_epoch_link(&b.link()) {
                        if self.output_raw && self.accounts_to_filter.is_empty() {
                            entry.subtype = Some(BlockSubTypeDto::Epoch);
                            entry.account = self.ledger.epoch_signer(&b.link());
                            Some(entry)
                        } else {
                            None
                        }
                    } else {
                        let source_account_opt =
                            self.ledger.any().block_account(tx, &b.link().into());
                        let source_account = source_account_opt.unwrap_or_default();

                        if source_account_opt.is_some()
                            && self.should_ignore_account(&source_account)
                        {
                            None
                        } else {
                            if self.output_raw {
                                entry.subtype = Some(BlockSubTypeDto::Receive);
                            } else {
                                entry.block_type = Some(BlockTypeDto::Receive);
                            }
                            if source_account_opt.is_some() {
                                entry.account = Some(source_account);
                            }
                            entry.amount = Some(balance - previous_balance);
                            Some(entry)
                        }
                    }
                }
            }
        };

        if let Some(entry) = &mut entry {
            self.set_common_fields(entry, block, tx);
        }
        entry
    }

    fn set_common_fields(
        &self,
        entry: &mut HistoryEntry,
        block: &SavedBlock,
        tx: &LmdbReadTransaction,
    ) {
        entry.local_timestamp = block.timestamp().as_u64().into();
        entry.height = block.height().into();
        entry.hash = block.hash();
        entry.confirmed = self
            .ledger
            .confirmed()
            .block_exists_or_pruned(tx, &block.hash())
            .into();
        if self.output_raw {
            entry.work = Some(block.work().into());
            entry.signature = Some(block.signature().clone());
        }
    }

    fn create_response(&self, history: Vec<HistoryEntry>) -> AccountHistoryResponse {
        let mut response = AccountHistoryResponse {
            account: self.account,
            history,
            previous: None,
            next: None,
        };

        if !self.current_block_hash.is_zero() {
            if self.reverse {
                response.next = Some(self.current_block_hash);
            } else {
                response.previous = Some(self.current_block_hash);
            }
        }
        response
    }
}

fn empty_entry() -> HistoryEntry {
    HistoryEntry {
        block_type: None,
        amount: None,
        account: None,
        block_account: None,
        local_timestamp: 0.into(),
        height: 0.into(),
        hash: BlockHash::zero(),
        confirmed: false.into(),
        work: None,
        signature: None,
        representative: None,
        previous: None,
        balance: None,
        source: None,
        opened: None,
        destination: None,
        link: None,
        subtype: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command_handler::test_rpc_command;
    use rsnano_rpc_messages::{RpcCommand, RpcError};

    #[test]
    fn history_rpc_call() {
        let cmd = RpcCommand::account_history(
            AccountHistoryArgs::build_for_account(Account::from(42), 3).finish(),
        );

        let result: RpcError = test_rpc_command(cmd);

        assert_eq!(result.error, "Account not found");
    }
}
