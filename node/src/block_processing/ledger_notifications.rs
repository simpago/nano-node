use super::BlockContext;
use rsnano_core::{QualifiedRoot, SavedBlock};
use rsnano_ledger::BlockStatus;
use std::sync::{Arc, RwLock};

pub struct LedgerNotifications {
    block_processed: RwLock<Vec<Box<dyn Fn(BlockStatus, &BlockContext) + Send + Sync>>>,

    /// All processed blocks including forks, rejected etc
    batch_processed: RwLock<Vec<Box<dyn Fn(&[(BlockStatus, Arc<BlockContext>)]) + Send + Sync>>>,

    /// Rolled back blocks <rolled back block, root of rollback>
    roll_back_observers: Arc<RwLock<Vec<Box<dyn Fn(&[SavedBlock], QualifiedRoot) + Send + Sync>>>>,
}

impl LedgerNotifications {
    pub fn new() -> Self {
        Self {
            block_processed: RwLock::new(Vec::new()),
            batch_processed: RwLock::new(Vec::new()),
            roll_back_observers: Arc::new(RwLock::new(Vec::new())),
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

    pub fn on_blocks_rolled_back(
        &self,
        callback: impl Fn(&[SavedBlock], QualifiedRoot) + Send + Sync + 'static,
    ) {
        self.roll_back_observers
            .write()
            .unwrap()
            .push(Box::new(callback));
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

    pub fn notify_rollback(&self, rolled_back: &[SavedBlock], root: QualifiedRoot) {
        let guard = self.roll_back_observers.read().unwrap();
        for callback in &*guard {
            callback(rolled_back, root.clone());
        }
    }
}

impl Default for LedgerNotifications {
    fn default() -> Self {
        Self::new()
    }
}
