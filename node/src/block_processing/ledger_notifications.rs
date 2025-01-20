use super::BlockContext;
use rsnano_ledger::BlockStatus;
use std::sync::{Arc, RwLock};

pub(crate) struct LedgerNotifications {
    pub block_processed: RwLock<Vec<Box<dyn Fn(BlockStatus, &BlockContext) + Send + Sync>>>,

    /// All processed blocks including forks, rejected etc
    pub batch_processed:
        RwLock<Vec<Box<dyn Fn(&[(BlockStatus, Arc<BlockContext>)]) + Send + Sync>>>,
}

impl LedgerNotifications {
    pub(crate) fn new() -> Self {
        Self {
            block_processed: RwLock::new(Vec::new()),
            batch_processed: RwLock::new(Vec::new()),
        }
    }

    pub fn on_block_processed(
        &self,
        observer: Box<dyn Fn(BlockStatus, &BlockContext) + Send + Sync>,
    ) {
        self.block_processed.write().unwrap().push(observer);
    }

    pub fn on_batch_processed(
        &self,
        observer: Box<dyn Fn(&[(BlockStatus, Arc<BlockContext>)]) + Send + Sync>,
    ) {
        self.batch_processed.write().unwrap().push(observer);
    }

    pub fn notify_batch_processed(&self, blocks: &Vec<(BlockStatus, Arc<BlockContext>)>) {
        {
            let guard = self.block_processed.read().unwrap();
            for observer in guard.iter() {
                for (status, context) in blocks {
                    observer(*status, context);
                }
            }
        }
        {
            let guard = self.batch_processed.read().unwrap();
            for observer in guard.iter() {
                observer(&blocks);
            }
        }
    }
}

impl Default for LedgerNotifications {
    fn default() -> Self {
        Self::new()
    }
}
