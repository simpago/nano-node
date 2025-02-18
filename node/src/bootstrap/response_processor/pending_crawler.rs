use rsnano_core::{Account, BlockHash, PendingInfo, PendingKey};
use rsnano_ledger::Ledger;
use rsnano_store_lmdb::LmdbReadTransaction;

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

    pub fn seek(&mut self, start: Account) {
        let start_key = PendingKey::new(start, BlockHash::zero());
        let mut it = Box::new(self.ledger.store.pending.iter_range(self.tx, start_key..));
        self.current = it.next();
        self.it = if self.current.is_some() {
            Some(it)
        } else {
            None
        }
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

    fn target_reached(current: &Option<(PendingKey, PendingInfo)>, target: &Account) -> bool {
        if let Some((key, _)) = current {
            if key.receiving_account >= *target {
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
        let mut crawler = PendingDatabaseCrawler::new(&ledger, &tx);
        crawler.seek(Account::from(123));
        assert_eq!(crawler.current, None);
    }

    #[test]
    fn seek_exact() {
        let key = PendingKey::new_test_instance();
        let info = PendingInfo::new_test_instance();
        let ledger = Ledger::new_null_builder().pending(&key, &info).finish();
        let tx = ledger.read_txn();
        let mut crawler = PendingDatabaseCrawler::new(&ledger, &tx);
        crawler.seek(key.receiving_account);
        assert_eq!(crawler.current, Some((key, info)));
    }

    #[test]
    fn seek_before() {
        let key = PendingKey::new_test_instance();
        let info = PendingInfo::new_test_instance();
        let ledger = Ledger::new_null_builder().pending(&key, &info).finish();
        let tx = ledger.read_txn();
        let mut crawler = PendingDatabaseCrawler::new(&ledger, &tx);
        crawler.seek(Account::zero());
        assert_eq!(crawler.current, Some((key, info)));
    }

    #[test]
    fn seek_after() {
        let key = PendingKey::new_test_instance();
        let info = PendingInfo::new_test_instance();
        let ledger = Ledger::new_null_builder().pending(&key, &info).finish();
        let tx = ledger.read_txn();
        let mut crawler = PendingDatabaseCrawler::new(&ledger, &tx);
        crawler.seek(key.receiving_account.inc_or_max());
        assert_eq!(crawler.current, None);
    }

    #[test]
    fn advance_not_found() {
        let key = PendingKey::new_test_instance();
        let info = PendingInfo::new_test_instance();
        let ledger = Ledger::new_null_builder().pending(&key, &info).finish();
        let tx = ledger.read_txn();
        let mut crawler = PendingDatabaseCrawler::new(&ledger, &tx);
        crawler.seek(key.receiving_account);
        crawler.advance_to(Account::from(99999));
        assert_eq!(crawler.current, None);
    }

    #[test]
    fn advance_exact() {
        let key1 = PendingKey::new(Account::from(1), BlockHash::from(2));
        let key2 = PendingKey::new(Account::from(2), BlockHash::from(3));
        let info = PendingInfo::new_test_instance();
        let ledger = Ledger::new_null_builder()
            .pending(&key1, &info)
            .pending(&key2, &info)
            .finish();
        let tx = ledger.read_txn();
        let mut crawler = PendingDatabaseCrawler::new(&ledger, &tx);
        crawler.seek(key1.receiving_account);
        crawler.advance_to(key2.receiving_account);
        assert_eq!(crawler.current, Some((key2, info)));
    }

    #[test]
    fn advance_seek() {
        let first_account = Account::from(1);
        let info = PendingInfo::new_test_instance();
        let block = BlockHash::from(3);
        let mut ledger_builder =
            Ledger::new_null_builder().pending(&PendingKey::new(first_account, block), &info);

        for i in 0..PendingDatabaseCrawler::SEQUENTIAL_ATTEMPTS {
            ledger_builder =
                ledger_builder.pending(&PendingKey::new(Account::from(i as u64 + 2), block), &info);
        }

        let ledger = ledger_builder.finish();
        let tx = ledger.read_txn();
        let mut crawler = PendingDatabaseCrawler::new(&ledger, &tx);
        crawler.seek(first_account);
        let target = Account::from(PendingDatabaseCrawler::SEQUENTIAL_ATTEMPTS as u64 + 1);
        crawler.advance_to(target);
        assert_eq!(
            crawler.current,
            Some((PendingKey::new(target, block), info))
        );
    }

    #[test]
    fn advance_noop() {
        let key = PendingKey::new(Account::from(1), BlockHash::from(2));
        let info = PendingInfo::new_test_instance();
        let ledger = Ledger::new_null_builder().pending(&key, &info).finish();
        let tx = ledger.read_txn();
        let mut crawler = PendingDatabaseCrawler::new(&ledger, &tx);
        crawler.seek(key.receiving_account);
        crawler.advance_to(key.receiving_account);
        assert_eq!(crawler.current, Some((key, info)));
    }

    #[test]
    fn advance_after_nothing_found() {
        let key = PendingKey::new(Account::from(1), BlockHash::from(2));
        let info = PendingInfo::new_test_instance();
        let ledger = Ledger::new_null_builder().pending(&key, &info).finish();
        let tx = ledger.read_txn();
        let mut crawler = PendingDatabaseCrawler::new(&ledger, &tx);
        crawler.seek(key.receiving_account);
        crawler.advance_to(key.receiving_account.inc_or_max());
        assert_eq!(crawler.current, None);
        crawler.advance_to(key.receiving_account.inc_or_max());
        assert_eq!(crawler.current, None);
    }
}
