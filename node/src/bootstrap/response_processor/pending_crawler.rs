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
        let mut iter = self
            .ledger
            .store
            .pending
            .iter_range(self.tx, PendingKey::new(start, BlockHash::zero())..);

        self.current = iter.next();
        if self.current.is_some() {
            self.it = Some(Box::new(iter));
        } else {
            self.it = None;
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
        crawler.advance_to(&Account::from(99999));
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
        crawler.advance_to(&key2.receiving_account);
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
        crawler.advance_to(&target);
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
        crawler.advance_to(&key.receiving_account);
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
        crawler.advance_to(&key.receiving_account.inc_or_max());
        assert_eq!(crawler.current, None);
        crawler.advance_to(&key.receiving_account.inc_or_max());
        assert_eq!(crawler.current, None);
    }
}
