use std::sync::Arc;

use crate::{as_write_txn, count, LmdbEnv, LmdbIteratorImpl, EnvironmentStrategy, EnvironmentWrapper};
use lmdb::{Database, DatabaseFlags, WriteFlags};
use rsnano_core::Amount;
use rsnano_store_traits::{OnlineWeightIterator, OnlineWeightStore, Transaction, WriteTransaction};

pub struct LmdbOnlineWeightStore<T:EnvironmentStrategy = EnvironmentWrapper> {
    _env: Arc<LmdbEnv<T>>,
    database: Database,
}

impl<T: EnvironmentStrategy> LmdbOnlineWeightStore<T> {
    pub fn new(env: Arc<LmdbEnv<T>>) -> anyhow::Result<Self> {
        let database = env
            .environment
            .create_db(Some("online_weight"), DatabaseFlags::empty())?;
        Ok(Self {
            _env: env,
            database,
        })
    }

    pub fn database(&self) -> Database {
        self.database
    }
}

impl<T: EnvironmentStrategy + 'static> OnlineWeightStore for LmdbOnlineWeightStore<T> {
    fn put(&self, txn: &mut dyn WriteTransaction, time: u64, amount: &Amount) {
        let time_bytes = time.to_be_bytes();
        let amount_bytes = amount.to_be_bytes();
        as_write_txn::<T>(txn)
            .put(
                self.database,
                &time_bytes,
                &amount_bytes,
                WriteFlags::empty(),
            )
            .unwrap();
    }

    fn del(&self, txn: &mut dyn WriteTransaction, time: u64) {
        let time_bytes = time.to_be_bytes();
        as_write_txn::<T>(txn)
            .del(self.database, &time_bytes, None)
            .unwrap();
    }

    fn begin(&self, txn: &dyn Transaction) -> OnlineWeightIterator {
        LmdbIteratorImpl::new_iterator::<T, _, _>(txn, self.database, None, true)
    }

    fn rbegin(&self, txn: &dyn Transaction) -> OnlineWeightIterator {
        LmdbIteratorImpl::new_iterator::<T, _, _>(txn, self.database, None, false)
    }

    fn count(&self, txn: &dyn Transaction) -> u64 {
        count::<T>(txn, self.database)
    }

    fn clear(&self, txn: &mut dyn WriteTransaction) {
        as_write_txn::<T>(txn).clear_db(self.database).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use crate::TestLmdbEnv;

    use super::*;

    #[test]
    fn empty_store() -> anyhow::Result<()> {
        let env = TestLmdbEnv::new();
        let store = LmdbOnlineWeightStore::new(env.env())?;
        let txn = env.tx_begin_read()?;
        assert_eq!(store.count(&txn), 0);
        assert!(store.begin(&txn).is_end());
        assert!(store.rbegin(&txn).is_end());
        Ok(())
    }

    #[test]
    fn add_one() -> anyhow::Result<()> {
        let env = TestLmdbEnv::new();
        let store = LmdbOnlineWeightStore::new(env.env())?;
        let mut txn = env.tx_begin_write()?;

        let time = 1;
        let amount = Amount::raw(2);
        store.put(&mut txn, time, &amount);

        assert_eq!(store.count(&txn), 1);
        assert_eq!(store.begin(&txn).current(), Some((&time, &amount)));
        assert_eq!(store.rbegin(&txn).current(), Some((&time, &amount)));
        Ok(())
    }

    #[test]
    fn add_two() -> anyhow::Result<()> {
        let env = TestLmdbEnv::new();
        let store = LmdbOnlineWeightStore::new(env.env())?;
        let mut txn = env.tx_begin_write()?;

        let time1 = 1;
        let time2 = 2;
        let amount1 = Amount::raw(3);
        let amount2 = Amount::raw(4);
        store.put(&mut txn, time1, &amount1);
        store.put(&mut txn, time2, &amount2);

        assert_eq!(store.count(&txn), 2);
        assert_eq!(store.begin(&txn).current(), Some((&time1, &amount1)));
        assert_eq!(store.rbegin(&txn).current(), Some((&time2, &amount2)));
        Ok(())
    }

    #[test]
    fn delete() -> anyhow::Result<()> {
        let env = TestLmdbEnv::new();
        let store = LmdbOnlineWeightStore::new(env.env())?;
        let mut txn = env.tx_begin_write()?;

        let time1 = 1;
        let time2 = 2;
        let amount1 = Amount::raw(3);
        let amount2 = Amount::raw(4);
        store.put(&mut txn, time1, &amount1);
        store.put(&mut txn, time2, &amount2);

        store.del(&mut txn, time1);

        assert_eq!(store.count(&txn), 1);
        assert_eq!(store.begin(&txn).current(), Some((&time2, &amount2)));
        Ok(())
    }
}
