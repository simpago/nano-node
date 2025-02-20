use rsnano_core::{Account, BlockHash, DetailedBlock};
use rsnano_ledger::Ledger;

pub(crate) struct Explorer {
    state: ExplorerState,
}

impl Explorer {
    pub(crate) fn new() -> Self {
        Self {
            state: ExplorerState::Empty,
        }
    }

    pub(crate) fn search(&mut self, ledger: &Ledger, input: &str) -> bool {
        if let Ok(hash) = BlockHash::decode_hex(input.trim()) {
            let tx = ledger.read_txn();
            self.state = match ledger.detailed_block(&tx, &hash) {
                Some(block) => ExplorerState::Block(block),
                None => ExplorerState::NotFound,
            };
            return true;
        };

        if let Ok(account) = Account::decode_account(input) {
            let tx = ledger.read_txn();
            self.state = if let Some(head) = ledger.any().account_head(&tx, &account) {
                match ledger.detailed_block(&tx, &head) {
                    Some(block) => ExplorerState::Block(block),
                    None => ExplorerState::NotFound,
                }
            } else {
                ExplorerState::NotFound
            };
            return true;
        }

        false
    }

    pub(crate) fn state(&self) -> &ExplorerState {
        &self.state
    }
}

pub(crate) enum ExplorerState {
    Empty,
    NotFound,
    Block(DetailedBlock),
}
