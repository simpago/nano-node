use std::sync::{Arc, Mutex};

use crate::{
    datastore::{
        final_vote_store::FinalVoteIterator,
        lmdb::{assert_success, mdb_put, MDB_NOTFOUND, MDB_SUCCESS},
        parallel_traversal_u512, FinalVoteStore,
    },
    utils::Serialize,
    BlockHash, QualifiedRoot, Root,
};

use super::{
    ensure_success, mdb_count, mdb_dbi_open, mdb_del, mdb_drop, mdb_get, LmdbEnv, LmdbIteratorImpl,
    LmdbReadTransaction, LmdbWriteTransaction, MdbVal, Transaction,
};

/// Maps root to block hash for generated final votes.
/// nano::qualified_root -> nano::block_hash
pub struct LmdbFinalVoteStore {
    env: Arc<LmdbEnv>,
    db_handle: Mutex<u32>,
}

impl LmdbFinalVoteStore {
    pub fn new(env: Arc<LmdbEnv>) -> Self {
        Self {
            env,
            db_handle: Mutex::new(0),
        }
    }

    pub fn db_handle(&self) -> u32 {
        *self.db_handle.lock().unwrap()
    }

    pub fn open_db(&self, txn: &Transaction, flags: u32) -> anyhow::Result<()> {
        let mut handle = 0;
        let status = unsafe { mdb_dbi_open(txn.handle(), Some("final_votes"), flags, &mut handle) };
        *self.db_handle.lock().unwrap() = handle;
        ensure_success(status)
    }
}

impl<'a> FinalVoteStore<'a, LmdbReadTransaction, LmdbWriteTransaction, LmdbIteratorImpl>
    for LmdbFinalVoteStore
{
    fn put(&self, txn: &mut LmdbWriteTransaction, root: &QualifiedRoot, hash: &BlockHash) -> bool {
        let mut value = MdbVal::new();
        let root_bytes = root.to_bytes();
        let status = unsafe {
            mdb_get(
                txn.handle,
                self.db_handle(),
                &mut MdbVal::from_slice(&root_bytes),
                &mut value,
            )
        };
        assert!(status == MDB_SUCCESS || status == MDB_NOTFOUND);
        if status == MDB_SUCCESS {
            BlockHash::try_from(&value).unwrap() == *hash
        } else {
            let status = unsafe {
                mdb_put(
                    txn.as_txn().handle(),
                    self.db_handle(),
                    &mut MdbVal::from_slice(&root_bytes),
                    &mut MdbVal::from(hash),
                    0,
                )
            };
            assert_success(status);
            true
        }
    }

    fn begin(&self, txn: &Transaction) -> FinalVoteIterator<LmdbIteratorImpl> {
        FinalVoteIterator::new(LmdbIteratorImpl::new(
            txn,
            self.db_handle(),
            MdbVal::new(),
            QualifiedRoot::serialized_size(),
            true,
        ))
    }

    fn begin_at_root(
        &self,
        txn: &Transaction,
        root: &QualifiedRoot,
    ) -> FinalVoteIterator<LmdbIteratorImpl> {
        let key_bytes = root.to_bytes();
        FinalVoteIterator::new(LmdbIteratorImpl::new(
            txn,
            self.db_handle(),
            MdbVal::from_slice(&key_bytes),
            QualifiedRoot::serialized_size(),
            true,
        ))
    }

    fn get(&self, txn: &Transaction, root: Root) -> Vec<BlockHash> {
        let mut result = Vec::new();
        let key_start = QualifiedRoot {
            root,
            previous: BlockHash::new(),
        };

        let mut i = self.begin_at_root(txn, &key_start);
        while let Some((k, v)) = i.current() {
            if k.root != root {
                break;
            }

            result.push(*v);
            i.next();
        }

        result
    }

    fn del(&self, txn: &mut LmdbWriteTransaction, root: Root) {
        let mut final_vote_qualified_roots = Vec::new();

        let mut it = self.begin_at_root(
            &txn.as_txn(),
            &QualifiedRoot {
                root,
                previous: BlockHash::new(),
            },
        );
        while let Some((k, _)) = it.current() {
            if k.root != root {
                break;
            }
            final_vote_qualified_roots.push(k.clone());
            it.next();
        }

        for qualified_root in final_vote_qualified_roots {
            let root_bytes = qualified_root.to_bytes();
            let status = unsafe {
                mdb_del(
                    txn.as_txn().handle(),
                    self.db_handle(),
                    &mut MdbVal::from_slice(&root_bytes),
                    None,
                )
            };
            assert_success(status);
        }
    }

    fn count(&self, txn: &Transaction) -> usize {
        unsafe { mdb_count(txn.handle(), self.db_handle()) }
    }

    fn clear(&self, txn: &mut LmdbWriteTransaction) {
        unsafe {
            mdb_drop(txn.handle, self.db_handle(), 0);
        }
    }

    fn for_each_par(
        &self,
        action: &(dyn Fn(
            LmdbReadTransaction,
            FinalVoteIterator<LmdbIteratorImpl>,
            FinalVoteIterator<LmdbIteratorImpl>,
        ) + Send
              + Sync),
    ) {
        parallel_traversal_u512(&|start, end, is_last| {
            let transaction = self.env.tx_begin_read();
            let begin_it = self.begin_at_root(&transaction.as_txn(), &start.into());
            let end_it = if !is_last {
                self.begin_at_root(&transaction.as_txn(), &end.into())
            } else {
                self.end()
            };
            action(transaction, begin_it, end_it);
        });
    }

    fn end(&self) -> FinalVoteIterator<LmdbIteratorImpl> {
        FinalVoteIterator::new(LmdbIteratorImpl::null())
    }
}
