use crate::block_processing::LedgerNotifications;
use rsnano_core::SavedBlock;
use rsnano_ledger::BlockStatus;
use std::sync::{Arc, RwLock};

/// Observes confirmed blocks and dispatches the process_live function.
pub struct ProcessLiveDispatcher {
    new_unconfirmed_block_observer: RwLock<Vec<Arc<dyn Fn(&SavedBlock) + Send + Sync>>>,
}

impl ProcessLiveDispatcher {
    pub fn new() -> Self {
        Self {
            new_unconfirmed_block_observer: RwLock::new(Vec::new()),
        }
    }

    fn process_live(&self, block: &SavedBlock) {
        let callbacks = self.new_unconfirmed_block_observer.read().unwrap();
        for callback in callbacks.iter() {
            callback(block);
        }
    }

    pub fn add_new_unconfirmed_block_callback(&self, f: Arc<dyn Fn(&SavedBlock) + Send + Sync>) {
        self.new_unconfirmed_block_observer.write().unwrap().push(f);
    }
}

pub trait ProcessLiveDispatcherExt {
    fn connect(&self, notifications: &LedgerNotifications);
}

impl ProcessLiveDispatcherExt for Arc<ProcessLiveDispatcher> {
    fn connect(&self, notifications: &LedgerNotifications) {
        let self_w = Arc::downgrade(self);
        notifications.on_batch_processed(Box::new(move |batch| {
            if let Some(self_l) = self_w.upgrade() {
                for (result, context) in batch {
                    if *result == BlockStatus::Progress {
                        let block = context
                            .saved_block
                            .lock()
                            .unwrap()
                            .as_ref()
                            .unwrap()
                            .clone();
                        self_l.process_live(&block);
                    }
                }
            }
        }));
    }
}
