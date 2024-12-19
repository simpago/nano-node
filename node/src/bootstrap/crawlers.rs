use rsnano_core::{Account, AccountInfo, BlockHash, PendingInfo, PendingKey};
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

pub(super) struct PendingDatabaseCrawler<'a> {
    ledger: &'a Ledger,
    tx: &'a LmdbReadTransaction,
    it: Option<Box<dyn Iterator<Item = (PendingKey, PendingInfo)> + 'a>>,
    pub current: Option<(PendingKey, PendingInfo)>,
}

impl<'a> PendingDatabaseCrawler<'a> {
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
        self.it =
            Some(Box::new(self.ledger.store.pending.iter_range(
                self.tx,
                PendingKey::new(start, BlockHash::zero())..,
            )));
        if let Some(it) = &mut self.it {
            self.current = it.next();
            if self.current.is_none() {
                self.it = None;
            }
        } else {
            self.current = None;
        }
    }

    // Advance to the next account
    pub fn advance(&mut self) {
        let Some(it) = &mut self.it else {
            return;
        };

        let starting_account = self.current.as_ref().unwrap().0.receiving_account;

        // First try advancing sequentially
        for _ in 0..Self::SEQUENTIAL_ATTEMPTS {
            self.current = it.next();
            match &self.current {
                Some((key, _)) => {
                    // Break if we've reached the next account
                    if key.receiving_account != starting_account {
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

        if self.it.is_some() {
            // If that fails, perform a fresh lookup
            self.seek(starting_account.inc_or_max());
        }
    }

    pub fn advance_to(&mut self, account: &Account) {
        let Some(it) = &mut self.it else {
            return;
        };

        if let Some((key, _)) = &self.current {
            if key.receiving_account == *account {
                return; // already at correct account
            }
        }

        // First try advancing sequentially
        for _ in 0..Self::SEQUENTIAL_ATTEMPTS {
            self.current = it.next();
            match &self.current {
                Some((key, _)) => {
                    // Break if we've reached or overshoot the target account
                    if key.receiving_account.number() >= account.number() {
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
