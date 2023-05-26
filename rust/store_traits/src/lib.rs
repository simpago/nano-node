mod iterator;
pub use iterator::{BinaryDbIterator, DbIterator, DbIteratorImpl};

use rsnano_core::{utils::PropertyTreeWriter, Account, AccountInfo, BlockHash, BlockWithSideband, ConfirmationHeightInfo, QualifiedRoot, Amount, EndpointKey, NoValue, PendingKey, PendingInfo};

pub enum Table {
    ConfirmationHeight,
}

pub type PrunedIterator = Box<dyn DbIterator<BlockHash, NoValue>>;
pub type PendingIterator = Box<dyn DbIterator<PendingKey, PendingInfo>>;
pub type PeerIterator = Box<dyn DbIterator<EndpointKey, NoValue>>;
pub type OnlineWeightIterator = Box<dyn DbIterator<u64, Amount>>;
pub type AccountIterator = Box<dyn DbIterator<Account, AccountInfo>>;
pub type BlockIterator = Box<dyn DbIterator<BlockHash, BlockWithSideband>>;
pub type ConfirmationHeightIterator = Box<dyn DbIterator<Account, ConfirmationHeightInfo>>;
pub type FinalVoteIterator = Box<dyn DbIterator<QualifiedRoot, BlockHash>>;
pub type FrontierIterator = Box<dyn DbIterator<BlockHash, Account>>;

use std::{any::Any, time::Duration};

pub trait Transaction {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
    fn refresh(&mut self);
}

pub trait ReadTransaction {
    fn txn(&self) -> &dyn Transaction;
    fn txn_mut(&mut self) -> &mut dyn Transaction;
    fn reset(&mut self);
    fn renew(&mut self);
    fn refresh(&mut self);
}

pub trait WriteTransaction {
    fn txn(&self) -> &dyn Transaction;
    fn txn_mut(&mut self) -> &mut dyn Transaction;
    fn refresh(&mut self);
    fn renew(&mut self);
    fn commit(&mut self);
}

pub trait TransactionTracker: Send + Sync {
    fn txn_start(&self, txn_id: u64, is_write: bool);
    fn txn_end(&self, txn_id: u64, is_write: bool);
    fn serialize_json(
        &self,
        json: &mut dyn PropertyTreeWriter,
        min_read_time: Duration,
        min_write_time: Duration,
    ) -> anyhow::Result<()>;
}

pub struct NullTransactionTracker {}

impl NullTransactionTracker {
    pub fn new() -> Self {
        Self {}
    }
}

impl TransactionTracker for NullTransactionTracker {
    fn txn_start(&self, _txn_id: u64, _is_write: bool) {}

    fn txn_end(&self, _txn_id: u64, _is_write: bool) {}

    fn serialize_json(
        &self,
        _json: &mut dyn PropertyTreeWriter,
        _min_read_time: Duration,
        _min_write_time: Duration,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}
