use super::BlockContext;
use rsnano_core::{QualifiedRoot, SavedBlock};
use rsnano_ledger::BlockStatus;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct LedgerNotifications {
    callbacks: Arc<RwLock<Callbacks>>,
}

impl LedgerNotifications {
    pub(crate) fn new() -> (Self, LedgerNotifier) {
        let callbacks = Arc::new(RwLock::new(Callbacks::default()));
        let notifier = LedgerNotifier {
            callbacks: callbacks.clone(),
        };
        let notifications = Self { callbacks };
        (notifications, notifier)
    }

    pub fn on_block_processed(
        &self,
        observer: Box<dyn Fn(BlockStatus, &BlockContext) + Send + Sync>,
    ) {
        self.callbacks
            .write()
            .unwrap()
            .block_processed
            .push(observer);
    }

    /// All processed blocks including forks, rejected etc
    pub fn on_batch_processed(
        &self,
        observer: Box<dyn Fn(&[(BlockStatus, Arc<BlockContext>)]) + Send + Sync>,
    ) {
        self.callbacks
            .write()
            .unwrap()
            .batch_processed
            .push(observer);
    }

    /// Rolled back blocks <rolled back block, root of rollback>
    pub fn on_blocks_rolled_back(
        &self,
        callback: impl Fn(&[SavedBlock], QualifiedRoot) + Send + Sync + 'static,
    ) {
        self.callbacks
            .write()
            .unwrap()
            .rollback_observers
            .push(Box::new(callback));
    }

    pub fn notify_batch_processed(&self, blocks: &Vec<(BlockStatus, Arc<BlockContext>)>) {
        let guard = self.callbacks.read().unwrap();
        for observer in guard.block_processed.iter() {
            for (status, context) in blocks {
                observer(*status, context);
            }
        }
        for observer in guard.batch_processed.iter() {
            observer(&blocks);
        }
    }

    pub fn notify_rollback(&self, rolled_back: &[SavedBlock], root: QualifiedRoot) {
        let guard = self.callbacks.read().unwrap();
        for callback in guard.rollback_observers.iter() {
            callback(rolled_back, root.clone());
        }
    }
}

#[derive(Default)]
struct Callbacks {
    block_processed: Vec<Box<dyn Fn(BlockStatus, &BlockContext) + Send + Sync>>,
    batch_processed: Vec<Box<dyn Fn(&[(BlockStatus, Arc<BlockContext>)]) + Send + Sync>>,
    rollback_observers: Vec<Box<dyn Fn(&[SavedBlock], QualifiedRoot) + Send + Sync>>,
}

// publishes ledger notifications
pub(crate) struct LedgerNotifier {
    callbacks: Arc<RwLock<Callbacks>>,
}
