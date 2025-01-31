use rsnano_core::{Account, AccountInfo};
use rsnano_ledger::Ledger;
use rsnano_store_lmdb::LmdbReadTransaction;

pub(super) struct AccountDatabaseCrawler<'a> {
    ledger: &'a Ledger,
    tx: &'a LmdbReadTransaction,
    it: Option<Box<dyn Iterator<Item = (Account, AccountInfo)> + 'a>>,
    pub current: Option<(Account, AccountInfo)>,
}

impl<'a> AccountDatabaseCrawler<'a> {
    const SEQUENTIAL_ATTEMPTS: usize = 10;

    pub fn new(ledger: &'a Ledger, tx: &'a LmdbReadTransaction) -> Self {
        Self {
            ledger,
            tx,
            it: None,
            current: None,
        }
    }

    pub fn initialize(&mut self, start: Account) {
        self.seek(start);
    }

    fn seek(&mut self, start: Account) {
        self.it = Some(Box::new(
            self.ledger.store.account.iter_range(self.tx, start..),
        ));
        self.advance();
    }

    pub fn advance(&mut self) {
        if let Some(it) = &mut self.it {
            self.current = it.next();
            if self.current.is_none() {
                self.it = None;
            }
        } else {
            self.current = None;
        }
    }

    pub fn advance_to(&mut self, account: &Account) {
        let Some(it) = &mut self.it else {
            return;
        };

        if let Some((acc, _)) = &self.current {
            if acc == account {
                return; // already at correct account
            }
        }

        // First try advancing sequentially
        for _ in 0..Self::SEQUENTIAL_ATTEMPTS {
            self.current = it.next();
            match &self.current {
                Some((acc, _)) => {
                    // Break if we've reached or overshoot the target account
                    if acc.number() >= account.number() {
                        return;
                    }
                }
                None => {
                    self.it = None;
                    self.current = None;
                    break;
                }
            }
        }

        // If that fails, perform a fresh lookup
        self.seek(*account);
    }
}
