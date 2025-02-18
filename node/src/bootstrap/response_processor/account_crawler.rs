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

    pub fn seek(&mut self, target: Account) {
        let mut it = Box::new(self.ledger.store.account.iter_range(self.tx, target..));
        self.current = it.next();
        self.it = if self.current.is_some() {
            Some(it)
        } else {
            None
        };
    }

    pub fn advance_to(&mut self, target: Account) {
        if Self::target_reached(&self.current, &target) {
            return;
        }

        if !self.advance_sequentially(target) {
            // perform a fresh lookup
            self.seek(target);
        }
    }

    fn target_reached(current: &Option<(Account, AccountInfo)>, target: &Account) -> bool {
        if let Some((account, _)) = current {
            if account >= target {
                return true;
            }
        }
        false
    }

    /// It is faster to try to reuse the existing iterator than to
    /// perform a fresh seek.
    /// Only if this doesn't succeed we perform a fresh seek.
    fn advance_sequentially(&mut self, target: Account) -> bool {
        let Some(it) = &mut self.it else {
            return false;
        };

        for _ in 0..Self::SEQUENTIAL_ATTEMPTS {
            self.current = it.next();
            if Self::target_reached(&self.current, &target) {
                return true;
            }

            if self.current.is_none() {
                self.it = None;
                return true;
            }
        }

        false
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
        crawler.advance_to(Account::from(99999));
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
        crawler.advance_to(account2);
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
        crawler.advance_to(target);
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
        crawler.advance_to(account);
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
        crawler.advance_to(account.inc_or_max());
        assert_eq!(crawler.current, None);
        crawler.advance_to(account.inc_or_max());
        assert_eq!(crawler.current, None);
    }
}
