use super::{iterator::DbIteratorImpl, DbIterator, Transaction};
use crate::core::{Account, PendingInfo, PendingKey};

pub type PendingIterator<I> = DbIterator<PendingKey, PendingInfo, I>;

/// Maps (destination account, pending block) to (source account, amount, version).
/// nano::account, nano::block_hash -> nano::account, nano::amount, nano::epoch
pub trait PendingStore<'a, R, W, I>
where
    R: 'a,
    W: 'a,
    I: DbIteratorImpl,
{
    fn put(&self, txn: &mut W, key: &PendingKey, pending: &PendingInfo);
    fn del(&self, txn: &mut W, key: &PendingKey);
    fn get(&self, txn: &Transaction<R, W>, key: &PendingKey) -> Option<PendingInfo>;
    fn begin(&self, txn: &Transaction<R, W>) -> PendingIterator<I>;
    fn begin_at_key(&self, txn: &Transaction<R, W>, key: &PendingKey) -> PendingIterator<I>;
    fn end(&self) -> PendingIterator<I>;
    fn exists(&self, txn: &Transaction<R, W>, key: &PendingKey) -> bool;
    fn any(&self, txn: &Transaction<R, W>, account: &Account) -> bool;
    fn for_each_par(
        &'a self,
        action: &(dyn Fn(R, PendingIterator<I>, PendingIterator<I>) + Send + Sync),
    );
}
