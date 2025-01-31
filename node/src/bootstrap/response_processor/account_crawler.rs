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

    pub fn seek(&mut self, start: Account) {
        self.it = Some(Box::new(
            self.ledger.store.account.iter_range(self.tx, start..),
        ));
        self.advance();
    }

    fn advance(&mut self) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_ledger() {
        let ledger = Ledger::new_null();
        let tx = ledger.read_txn();
        let mut crawler = AccountDatabaseCrawler::new(&ledger, &tx);
        crawler.seek(Account::from(123));
        assert_eq!(crawler.current, None);
    }

    #[test]
    fn seek_exact() {
        let account = Account::from(1);
        let info = AccountInfo::new_test_instance();
        let ledger = Ledger::new_null_builder()
            .account_info(&account, &info)
            .finish();
        let tx = ledger.read_txn();
        let mut crawler = AccountDatabaseCrawler::new(&ledger, &tx);
        crawler.seek(account);
        assert_eq!(crawler.current, Some((account, info)));
    }

    #[test]
    fn seek_before() {
        let account = Account::from(2);
        let info = AccountInfo::new_test_instance();
        let ledger = Ledger::new_null_builder()
            .account_info(&account, &info)
            .finish();
        let tx = ledger.read_txn();
        let mut crawler = AccountDatabaseCrawler::new(&ledger, &tx);
        crawler.seek(Account::from(1));
        assert_eq!(crawler.current, Some((account, info)));
    }

    #[test]
    fn seek_after() {
        let account = Account::from(1);
        let info = AccountInfo::new_test_instance();
        let ledger = Ledger::new_null_builder()
            .account_info(&account, &info)
            .finish();
        let tx = ledger.read_txn();
        let mut crawler = AccountDatabaseCrawler::new(&ledger, &tx);
        crawler.seek(Account::from(2));
        assert_eq!(crawler.current, None);
    }

    #[test]
    fn advance_not_found() {
        let account = Account::from(1);
        let info = AccountInfo::new_test_instance();
        let ledger = Ledger::new_null_builder()
            .account_info(&account, &info)
            .finish();
        let tx = ledger.read_txn();
        let mut crawler = AccountDatabaseCrawler::new(&ledger, &tx);
        crawler.seek(Account::from(2));
        crawler.advance_to(&Account::from(99999));
        assert_eq!(crawler.current, None);
    }

    #[test]
    fn advance_exact() {
        let account1 = Account::from(1);
        let account2 = Account::from(2);
        let info = AccountInfo::new_test_instance();
        let ledger = Ledger::new_null_builder()
            .account_info(&account1, &info)
            .account_info(&account2, &info)
            .finish();
        let tx = ledger.read_txn();
        let mut crawler = AccountDatabaseCrawler::new(&ledger, &tx);
        crawler.seek(account1);
        crawler.advance_to(&account2);
        assert_eq!(crawler.current, Some((account2, info)));
    }

    #[test]
    fn advance_seek() {
        let first_account = Account::from(1);
        let info = AccountInfo::new_test_instance();
        let mut ledger_builder = Ledger::new_null_builder().account_info(&first_account, &info);

        for i in 0..AccountDatabaseCrawler::SEQUENTIAL_ATTEMPTS {
            ledger_builder = ledger_builder.account_info(&Account::from(i as u64 + 2), &info);
        }

        let ledger = ledger_builder.finish();

        let tx = ledger.read_txn();
        let mut crawler = AccountDatabaseCrawler::new(&ledger, &tx);
        crawler.seek(first_account);
        let target = Account::from(AccountDatabaseCrawler::SEQUENTIAL_ATTEMPTS as u64 + 1);
        crawler.advance_to(&target);
        assert_eq!(crawler.current, Some((target, info)));
    }

    #[test]
    fn advance_noop() {
        let account = Account::from(1);
        let info = AccountInfo::new_test_instance();
        let ledger = Ledger::new_null_builder()
            .account_info(&account, &info)
            .finish();
        let tx = ledger.read_txn();
        let mut crawler = AccountDatabaseCrawler::new(&ledger, &tx);
        crawler.seek(account);
        crawler.advance_to(&account);
        assert_eq!(crawler.current, Some((account, info)));
    }

    #[test]
    fn advance_after_nothing_found() {
        let account = Account::from(1);
        let info = AccountInfo::new_test_instance();
        let ledger = Ledger::new_null_builder()
            .account_info(&account, &info)
            .finish();
        let tx = ledger.read_txn();
        let mut crawler = AccountDatabaseCrawler::new(&ledger, &tx);
        crawler.seek(account);
        crawler.advance_to(&account.inc_or_max());
        assert_eq!(crawler.current, None);
        crawler.advance_to(&account.inc_or_max());
        assert_eq!(crawler.current, None);
    }
}
