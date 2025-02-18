use rsnano_core::{Account, AccountInfo, BlockHash, PendingInfo, PendingKey};
use rsnano_ledger::Ledger;
use rsnano_store_lmdb::LmdbReadTransaction;

pub(super) struct DatabaseCrawler<'a, T>
where
    T: CrawlSource<'a>,
{
    source: T,
    it: Option<Box<dyn Iterator<Item = (Account, T::Value)> + 'a>>,
    pub current: Option<(Account, T::Value)>,
}

const SEQUENTIAL_ATTEMPTS: usize = 10;

impl<'a, T> DatabaseCrawler<'a, T>
where
    T: CrawlSource<'a>,
{
    pub fn new(source: T) -> Self {
        Self {
            it: None,
            current: None,
            source,
        }
    }

    pub fn seek(&mut self, target: Account) {
        let mut it = self.source.iter(target);
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

    fn target_reached(current: &Option<(Account, T::Value)>, target: &Account) -> bool {
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

        for _ in 0..SEQUENTIAL_ATTEMPTS {
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

pub(crate) trait CrawlSource<'a> {
    type Value;
    fn iter(&self, start: Account) -> Box<dyn Iterator<Item = (Account, Self::Value)> + 'a>;
}

pub(crate) struct AccountCrawlSource<'a> {
    ledger: &'a Ledger,
    tx: &'a LmdbReadTransaction,
}

impl<'a> AccountCrawlSource<'a> {
    pub(crate) fn new(ledger: &'a Ledger, tx: &'a LmdbReadTransaction) -> Self {
        Self { ledger, tx }
    }
}

impl<'a> CrawlSource<'a> for AccountCrawlSource<'a> {
    type Value = AccountInfo;

    fn iter(&self, start: Account) -> Box<dyn Iterator<Item = (Account, AccountInfo)> + 'a> {
        Box::new(self.ledger.store.account.iter_range(self.tx, start..))
    }
}

pub(crate) struct PendingCrawlSource<'a> {
    ledger: &'a Ledger,
    tx: &'a LmdbReadTransaction,
}

impl<'a> PendingCrawlSource<'a> {
    pub(crate) fn new(ledger: &'a Ledger, tx: &'a LmdbReadTransaction) -> Self {
        Self { ledger, tx }
    }
}

impl<'a> CrawlSource<'a> for PendingCrawlSource<'a> {
    type Value = PendingInfo;

    fn iter(&self, start: Account) -> Box<dyn Iterator<Item = (Account, PendingInfo)> + 'a> {
        let start_key = PendingKey::new(start, BlockHash::zero());
        Box::new(
            self.ledger
                .store
                .pending
                .iter_range(self.tx, start_key..)
                .map(|(k, v)| (k.receiving_account, v)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_ledger() {
        let ledger = Ledger::new_null();
        let tx = ledger.read_txn();
        let source = AccountCrawlSource::new(&ledger, &tx);
        let mut crawler = DatabaseCrawler::new(source);
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
        let source = AccountCrawlSource::new(&ledger, &tx);
        let mut crawler = DatabaseCrawler::new(source);
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
        let source = AccountCrawlSource::new(&ledger, &tx);
        let mut crawler = DatabaseCrawler::new(source);
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
        let source = AccountCrawlSource::new(&ledger, &tx);
        let mut crawler = DatabaseCrawler::new(source);
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
        let source = AccountCrawlSource::new(&ledger, &tx);
        let mut crawler = DatabaseCrawler::new(source);
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
        let source = AccountCrawlSource::new(&ledger, &tx);
        let mut crawler = DatabaseCrawler::new(source);
        crawler.seek(account1);
        crawler.advance_to(account2);
        assert_eq!(crawler.current, Some((account2, info)));
    }

    #[test]
    fn advance_seek() {
        let first_account = Account::from(1);
        let info = AccountInfo::new_test_instance();
        let mut ledger_builder = Ledger::new_null_builder().account_info(&first_account, &info);

        for i in 0..SEQUENTIAL_ATTEMPTS + 1 {
            ledger_builder = ledger_builder.account_info(&Account::from(i as u64 + 2), &info);
        }

        let ledger = ledger_builder.finish();

        let tx = ledger.read_txn();
        let source = AccountCrawlSource::new(&ledger, &tx);
        let mut crawler = DatabaseCrawler::new(source);
        crawler.seek(first_account);
        let target = Account::from(SEQUENTIAL_ATTEMPTS as u64 + 2);
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
        let source = AccountCrawlSource::new(&ledger, &tx);
        let mut crawler = DatabaseCrawler::new(source);
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
        let source = AccountCrawlSource::new(&ledger, &tx);
        let mut crawler = DatabaseCrawler::new(source);
        crawler.seek(account);
        crawler.advance_to(account.inc_or_max());
        assert_eq!(crawler.current, None);
        crawler.advance_to(account.inc_or_max());
        assert_eq!(crawler.current, None);
    }

    #[test]
    fn pending_crawler() {
        let key = PendingKey::new_test_instance();
        let info = PendingInfo::new_test_instance();
        let ledger = Ledger::new_null_builder().pending(&key, &info).finish();
        let tx = ledger.read_txn();
        let mut crawler = DatabaseCrawler::new(PendingCrawlSource::new(&ledger, &tx));
        crawler.seek(key.receiving_account);
        assert_eq!(crawler.current, Some((key.receiving_account, info)));
    }
}
