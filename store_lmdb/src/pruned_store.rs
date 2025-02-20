use crate::{
    LmdbDatabase, LmdbEnv, LmdbIterator, LmdbRangeIterator, LmdbWriteTransaction, Transaction,
    PRUNED_TEST_DATABASE,
};
use lmdb::{DatabaseFlags, WriteFlags};
use rsnano_core::{BlockHash, NoValue};
use rsnano_nullable_lmdb::ConfiguredDatabase;
use std::{ops::RangeBounds, sync::Arc};

pub struct LmdbPrunedStore {
    database: LmdbDatabase,
}

impl LmdbPrunedStore {
    pub fn new(env: Arc<LmdbEnv>) -> anyhow::Result<Self> {
        let database = env
            .environment
            .create_db(Some("pruned"), DatabaseFlags::empty())?;
        Ok(Self { database })
    }

    pub fn database(&self) -> LmdbDatabase {
        self.database
    }

    pub fn put(&self, tx: &mut LmdbWriteTransaction, hash: &BlockHash) {
        tx.put(self.database, hash.as_bytes(), &[0; 0], WriteFlags::empty())
            .unwrap();
    }

    pub fn del(&self, tx: &mut LmdbWriteTransaction, hash: &BlockHash) {
        tx.delete(self.database, hash.as_bytes(), None).unwrap();
    }

    pub fn exists(&self, tx: &dyn Transaction, hash: &BlockHash) -> bool {
        tx.exists(self.database, hash.as_bytes())
    }

    pub fn iter<'tx>(&self, tx: &'tx dyn Transaction) -> impl Iterator<Item = BlockHash> + 'tx {
        let cursor = tx.open_ro_cursor(self.database).unwrap();

        LmdbIterator::new(cursor, |key, _| {
            let hash = BlockHash::from_slice(key).unwrap();
            (hash, NoValue {})
        })
        .map(|(k, _)| k)
    }

    pub fn iter_range<'tx>(
        &self,
        tx: &'tx dyn Transaction,
        range: impl RangeBounds<BlockHash> + 'static,
    ) -> impl Iterator<Item = BlockHash> + 'tx {
        let cursor = tx.open_ro_cursor(self.database).unwrap();
        LmdbRangeIterator::<BlockHash, NoValue, _>::new(cursor, range).map(|(k, _)| k)
    }

    pub fn random(&self, tx: &dyn Transaction) -> Option<BlockHash> {
        let random_hash = BlockHash::random();
        self.iter_range(tx, random_hash..)
            .next()
            .or_else(|| self.iter(tx).next())
    }

    pub fn count(&self, tx: &dyn Transaction) -> u64 {
        tx.count(self.database)
    }

    pub fn clear(&self, tx: &mut LmdbWriteTransaction) {
        tx.clear_db(self.database).unwrap();
    }
}

pub struct ConfiguredPrunedDatabaseBuilder {
    database: ConfiguredDatabase,
}

impl ConfiguredPrunedDatabaseBuilder {
    pub fn new() -> Self {
        Self {
            database: ConfiguredDatabase::new(PRUNED_TEST_DATABASE, "pruned"),
        }
    }

    pub fn pruned(mut self, hash: &BlockHash) -> Self {
        self.database
            .entries
            .insert(hash.as_bytes().to_vec(), Vec::new());
        self
    }

    pub fn build(self) -> ConfiguredDatabase {
        self.database
    }

    pub fn create(hashes: Vec<BlockHash>) -> ConfiguredDatabase {
        let mut builder = Self::new();
        for hash in hashes {
            builder = builder.pruned(&hash);
        }
        builder.build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DeleteEvent, PutEvent};

    struct Fixture {
        env: Arc<LmdbEnv>,
        store: LmdbPrunedStore,
    }

    impl Fixture {
        pub fn new() -> Self {
            Self::with_stored_data(Vec::new())
        }

        pub fn with_stored_data(entries: Vec<BlockHash>) -> Self {
            let env = LmdbEnv::new_null_with()
                .configured_database(ConfiguredPrunedDatabaseBuilder::create(entries))
                .build();
            let env = Arc::new(env);
            Self {
                env: env.clone(),
                store: LmdbPrunedStore::new(env).unwrap(),
            }
        }
    }

    #[test]
    fn empty_store() {
        let fixture = Fixture::new();
        let tx = fixture.env.tx_begin_read();
        let store = &fixture.store;

        assert_eq!(store.count(&tx), 0);
        assert_eq!(store.exists(&tx, &BlockHash::from(1)), false);
        assert!(store.iter(&tx).next().is_none());
        assert!(store.random(&tx).is_none());
    }

    #[test]
    fn add_pruned_info() {
        let fixture = Fixture::new();
        let mut tx = fixture.env.tx_begin_write();
        let put_tracker = tx.track_puts();
        let hash = BlockHash::from(1);

        fixture.store.put(&mut tx, &hash);

        assert_eq!(
            put_tracker.output(),
            vec![PutEvent {
                database: PRUNED_TEST_DATABASE.into(),
                key: hash.as_bytes().to_vec(),
                value: Vec::new(),
                flags: WriteFlags::empty()
            }]
        );
    }

    #[test]
    fn count() {
        let fixture = Fixture::with_stored_data(vec![BlockHash::from(1), BlockHash::from(2)]);
        let tx = fixture.env.tx_begin_read();

        assert_eq!(fixture.store.count(&tx), 2);
        assert_eq!(fixture.store.exists(&tx, &BlockHash::from(1)), true);
        assert_eq!(fixture.store.exists(&tx, &BlockHash::from(3)), false);
    }

    #[test]
    fn iterate() {
        let fixture = Fixture::with_stored_data(vec![BlockHash::from(1), BlockHash::from(2)]);
        let tx = fixture.env.tx_begin_read();

        assert_eq!(fixture.store.iter(&tx).next(), Some(BlockHash::from(1)));
        assert_eq!(
            fixture.store.iter_range(&tx, BlockHash::from(2)..).next(),
            Some(BlockHash::from(2))
        );
    }

    #[test]
    fn delete() {
        let fixture = Fixture::new();
        let mut tx = fixture.env.tx_begin_write();
        let delete_tracker = tx.track_deletions();
        let hash = BlockHash::from(1);

        fixture.store.del(&mut tx, &hash);

        assert_eq!(
            delete_tracker.output(),
            vec![DeleteEvent {
                database: PRUNED_TEST_DATABASE.into(),
                key: hash.as_bytes().to_vec()
            }]
        )
    }

    #[test]
    fn pruned_random() {
        let fixture = Fixture::with_stored_data(vec![BlockHash::from(42)]);
        let tx = fixture.env.tx_begin_read();
        let random_hash = fixture.store.random(&tx);
        assert_eq!(random_hash, Some(BlockHash::from(42)));
    }
}
