use super::{
    ledger_notifications::LedgerNotifications, BlockContext, BlockProcessorCallback, BlockSource,
    UncheckedMap,
};
use crate::{
    stats::{DetailType, StatType, Stats},
    utils::{ThreadPool, ThreadPoolImpl},
};
use rsnano_core::{
    utils::{ContainerInfo, FairQueue, FairQueueInfo},
    work::WorkThresholds,
    Block, BlockType, Epoch, Networks, SavedBlock, UncheckedInfo,
};
use rsnano_ledger::{BlockStatus, Ledger, Writer};
use rsnano_network::{ChannelId, DeadChannelCleanupStep};
use rsnano_store_lmdb::LmdbWriteTransaction;
use std::{
    collections::VecDeque,
    mem::size_of,
    sync::{Arc, Condvar, Mutex, MutexGuard},
    thread::JoinHandle,
    time::{Duration, Instant},
};
use strum::IntoEnumIterator;
use tracing::{debug, error, info, trace};

#[derive(Clone, Debug, PartialEq)]
pub struct BlockProcessorConfig {
    // Maximum number of blocks to queue from network peers
    pub max_peer_queue: usize,
    //
    // Maximum number of blocks to queue from system components (local RPC, bootstrap)
    pub max_system_queue: usize,

    // Higher priority gets processed more frequently
    pub priority_live: usize,
    pub priority_bootstrap: usize,
    pub priority_local: usize,
    pub priority_system: usize,
    pub batch_max_time: Duration,
    pub full_size: usize,
    pub batch_size: usize,
    pub max_queued_notifications: usize,
    pub work_thresholds: WorkThresholds,
}

impl BlockProcessorConfig {
    pub const DEFAULT_BATCH_SIZE: usize = 0;
    pub const DEFAULT_FULL_SIZE: usize = 65536;

    pub fn new(work_thresholds: WorkThresholds) -> Self {
        Self {
            work_thresholds,
            max_peer_queue: 128,
            max_system_queue: 16 * 1024,
            priority_live: 1,
            priority_bootstrap: 8,
            priority_local: 16,
            priority_system: 32,
            batch_max_time: Duration::from_millis(500),
            full_size: Self::DEFAULT_FULL_SIZE,
            batch_size: 256,
            max_queued_notifications: 8,
        }
    }

    pub fn new_for(network: Networks) -> Self {
        Self::new(WorkThresholds::default_for(network))
    }
}

pub struct BlockProcessor {
    thread: Mutex<Option<JoinHandle<()>>>,
    pub(crate) processor_loop: Arc<BlockProcessorLoopImpl>,
}

impl BlockProcessor {
    pub fn new(
        config: BlockProcessorConfig,
        ledger: Arc<Ledger>,
        unchecked_map: Arc<UncheckedMap>,
        stats: Arc<Stats>,
        notifications: LedgerNotifications,
    ) -> Self {
        let config_l = config.clone();
        let max_size_query = move |origin: &(BlockSource, ChannelId)| match origin.0 {
            BlockSource::Live | BlockSource::LiveOriginator => config_l.max_peer_queue,
            _ => config_l.max_system_queue,
        };

        let config_l = config.clone();
        let priority_query = move |origin: &(BlockSource, ChannelId)| match origin.0 {
            BlockSource::Live | BlockSource::LiveOriginator => config.priority_live,
            BlockSource::Bootstrap | BlockSource::BootstrapLegacy | BlockSource::Unchecked => {
                config_l.priority_bootstrap
            }
            BlockSource::Local => config_l.priority_local,
            BlockSource::Election | BlockSource::Forced | BlockSource::Unknown => {
                config.priority_system
            }
        };

        Self {
            processor_loop: Arc::new(BlockProcessorLoopImpl {
                mutex: Mutex::new(BlockProcessorImpl {
                    queue: FairQueue::new(max_size_query, priority_query),
                    last_log: None,
                    stopped: false,
                }),
                condition: Condvar::new(),
                ledger,
                unchecked: unchecked_map,
                config,
                stats,
                workers: ThreadPoolImpl::create(1, "Blck proc notif"),
                notifications,
            }),
            thread: Mutex::new(None),
        }
    }

    pub fn new_test_instance(ledger: Arc<Ledger>) -> Self {
        BlockProcessor::new(
            BlockProcessorConfig::new_for(Networks::NanoDevNetwork),
            ledger,
            Arc::new(UncheckedMap::default()),
            Arc::new(Stats::default()),
            LedgerNotifications::new().0,
        )
    }

    pub fn new_null() -> Self {
        Self::new_test_instance(Arc::new(Ledger::new_null()))
    }

    pub fn start(&self) {
        debug_assert!(self.thread.lock().unwrap().is_none());
        let processor_loop = Arc::clone(&self.processor_loop);
        *self.thread.lock().unwrap() = Some(
            std::thread::Builder::new()
                .name("Blck processing".to_string())
                .spawn(move || {
                    processor_loop.run();
                })
                .unwrap(),
        );
    }

    pub fn stop(&self) {
        self.processor_loop.mutex.lock().unwrap().stopped = true;
        self.processor_loop.condition.notify_all();
        let join_handle = self.thread.lock().unwrap().take();
        if let Some(join_handle) = join_handle {
            join_handle.join().unwrap();
        }
        self.processor_loop.workers.stop();
    }

    pub fn total_queue_len(&self) -> usize {
        self.processor_loop.total_queue_len()
    }

    pub fn queue_len(&self, source: BlockSource) -> usize {
        self.processor_loop.queue_len(source)
    }

    pub fn add(&self, block: Block, source: BlockSource, channel_id: ChannelId) -> bool {
        self.processor_loop.add(block, source, channel_id, None)
    }

    pub fn add_with_callback(
        &self,
        block: Block,
        source: BlockSource,
        channel_id: ChannelId,
        callback: BlockProcessorCallback,
    ) -> bool {
        self.processor_loop
            .add(block, source, channel_id, Some(callback))
    }

    pub fn add_blocking(
        &self,
        block: Arc<Block>,
        source: BlockSource,
    ) -> anyhow::Result<Result<SavedBlock, BlockStatus>> {
        self.processor_loop.add_blocking(block, source)
    }

    pub fn process_active(&self, block: Block) {
        self.processor_loop.process_active(block);
    }

    pub fn force(&self, block: Block) {
        self.processor_loop.force(block);
    }

    pub fn info(&self) -> FairQueueInfo<BlockSource> {
        self.processor_loop.info()
    }

    pub fn container_info(&self) -> ContainerInfo {
        self.processor_loop.container_info()
    }
}

impl Drop for BlockProcessor {
    fn drop(&mut self) {
        // Thread must be stopped before destruction
        debug_assert!(self.thread.lock().unwrap().is_none());
    }
}

pub(crate) struct BlockProcessorLoopImpl {
    mutex: Mutex<BlockProcessorImpl>,
    condition: Condvar,
    ledger: Arc<Ledger>,
    unchecked: Arc<UncheckedMap>,
    config: BlockProcessorConfig,
    stats: Arc<Stats>,
    workers: ThreadPoolImpl,
    notifications: LedgerNotifications,
}

trait BlockProcessorLoop {
    fn run(&self);
}

impl BlockProcessorLoop for Arc<BlockProcessorLoopImpl> {
    fn run(&self) {
        let mut guard = self.mutex.lock().unwrap();
        while !guard.stopped {
            if !guard.queue.is_empty() {
                // It's possible that ledger processing happens faster than the
                // notifications can be processed by other components, cooldown here
                if self.workers.num_queued_tasks() >= self.config.max_queued_notifications {
                    self.stats
                        .inc(StatType::BlockProcessor, DetailType::Cooldown);
                    guard = self
                        .condition
                        .wait_timeout_while(guard, Duration::from_millis(100), |i| !i.stopped)
                        .unwrap()
                        .0;
                }

                if guard.should_log() {
                    info!(
                        "{} blocks (+ {} forced) in processing_queue",
                        guard.queue.len(),
                        guard
                            .queue
                            .queue_len(&(BlockSource::Forced, ChannelId::LOOPBACK))
                    );
                }

                let mut processed = self.process_batch(guard);
                guard = self.mutex.lock().unwrap();

                // Queue notifications to be dispatched in the background
                let stats = self.stats.clone();
                let self_l = Arc::clone(self);

                self.workers.post(Box::new(move || {
                    stats.inc(StatType::BlockProcessor, DetailType::Notify);
                    // Set results for futures when not holding the lock
                    for (result, context) in processed.iter_mut() {
                        if let Some(cb) = &context.callback {
                            cb(*result);
                        }
                        context.set_result(*result);
                    }
                    self_l.notifications.notify_batch_processed(&processed);
                }));
            } else {
                guard = self
                    .condition
                    .wait_while(guard, |i| !i.stopped && i.queue.is_empty())
                    .unwrap();
            }
        }
    }
}

impl BlockProcessorLoopImpl {
    pub fn process_active(&self, block: Block) {
        self.add(block, BlockSource::Live, ChannelId::LOOPBACK, None);
    }

    pub fn add(
        &self,
        block: Block,
        source: BlockSource,
        channel_id: ChannelId,
        callback: Option<BlockProcessorCallback>,
    ) -> bool {
        if !self.config.work_thresholds.validate_entry_block(&block) {
            self.stats
                .inc(StatType::BlockProcessor, DetailType::InsufficientWork);
            return false; // Not added
        }

        self.stats
            .inc(StatType::BlockProcessor, DetailType::Process);
        debug!(
            "Processing block (async): {} (source: {:?} channel id: {})",
            block.hash(),
            source,
            channel_id
        );

        self.add_impl(
            Arc::new(BlockContext::new(block, source, callback)),
            channel_id,
        )
    }

    pub fn add_blocking(
        &self,
        block: Arc<Block>,
        source: BlockSource,
    ) -> anyhow::Result<Result<SavedBlock, BlockStatus>> {
        self.stats
            .inc(StatType::BlockProcessor, DetailType::ProcessBlocking);
        debug!(
            "Processing block (blocking): {} (source: {:?})",
            block.hash(),
            source
        );

        let hash = block.hash();
        let ctx = Arc::new(BlockContext::new(block.as_ref().clone(), source, None));
        let waiter = ctx.get_waiter();
        self.add_impl(ctx.clone(), ChannelId::LOOPBACK);

        match waiter.wait_result() {
            Some(BlockStatus::Progress) => Ok(Ok(ctx.saved_block.lock().unwrap().clone().unwrap())),
            Some(status) => Ok(Err(status)),
            None => {
                self.stats
                    .inc(StatType::BlockProcessor, DetailType::ProcessBlockingTimeout);
                error!("Block dropped when processing: {}", hash);
                Err(anyhow!("Block dropped when processing"))
            }
        }
    }

    pub fn force(&self, block: Block) {
        self.stats.inc(StatType::BlockProcessor, DetailType::Force);
        debug!("Forcing block: {}", block.hash());
        let ctx = Arc::new(BlockContext::new(block, BlockSource::Forced, None));
        self.add_impl(ctx, ChannelId::LOOPBACK);
    }

    // TODO: Remove and replace all checks with calls to size (block_source)
    pub fn total_queue_len(&self) -> usize {
        self.mutex.lock().unwrap().queue.len()
    }

    pub fn queue_len(&self, source: BlockSource) -> usize {
        self.mutex
            .lock()
            .unwrap()
            .queue
            .sum_queue_len((source, ChannelId::MIN)..=(source, ChannelId::MAX))
    }

    fn add_impl(&self, context: Arc<BlockContext>, channel_id: ChannelId) -> bool {
        let source = context.source;
        let added;
        {
            let mut guard = self.mutex.lock().unwrap();
            added = guard.queue.push((source, channel_id), context);
        }
        if added {
            self.condition.notify_all();
        } else {
            self.stats
                .inc(StatType::BlockProcessor, DetailType::Overfill);
            self.stats
                .inc(StatType::BlockProcessorOverfill, source.into());
        }
        added
    }

    fn next_batch(
        &self,
        data: &mut BlockProcessorImpl,
        max_count: usize,
    ) -> VecDeque<Arc<BlockContext>> {
        let mut results = VecDeque::new();
        while !data.queue.is_empty() && results.len() < max_count {
            results.push_back(data.next());
        }
        results
    }

    fn process_batch(
        &self,
        mut guard: MutexGuard<BlockProcessorImpl>,
    ) -> Vec<(BlockStatus, Arc<BlockContext>)> {
        let batch = self.next_batch(&mut guard, self.config.batch_size);
        drop(guard);

        let mut write_guard = self.ledger.write_queue.wait(Writer::BlockProcessor);
        let mut tx = self.ledger.rw_txn();

        let timer = Instant::now();

        // Processing blocks
        let mut number_of_blocks_processed = 0;
        let mut number_of_forced_processed = 0;

        let mut processed = Vec::new();
        for ctx in batch {
            let force = ctx.source == BlockSource::Forced;

            (write_guard, tx) = self.ledger.refresh_if_needed(write_guard, tx);

            if force {
                number_of_forced_processed += 1;
                let block = ctx.block.lock().unwrap().clone();
                self.rollback_competitor(&mut tx, &block);
            }

            number_of_blocks_processed += 1;

            let result = self.process_one(&mut tx, &ctx);
            processed.push((result, ctx));
        }

        if number_of_blocks_processed != 0 && timer.elapsed() > Duration::from_millis(100) {
            debug!(
                "Processed {} blocks ({} blocks were forced) in {} ms",
                number_of_blocks_processed,
                number_of_forced_processed,
                timer.elapsed().as_millis(),
            );
        }
        processed
    }

    pub fn process_one(
        &self,
        txn: &mut LmdbWriteTransaction,
        context: &BlockContext,
    ) -> BlockStatus {
        let mut block = context.block.lock().unwrap().clone();
        let hash = block.hash();
        let mut saved_block = None;

        let result = match self.ledger.process(txn, &mut block) {
            Ok(saved) => {
                saved_block = Some(saved.clone());
                *context.saved_block.lock().unwrap() = Some(saved);
                BlockStatus::Progress
            }
            Err(r) => r,
        };

        // reassign to copy sideband
        *context.block.lock().unwrap() = block.clone();

        self.stats
            .inc(StatType::BlockProcessorResult, result.into());
        self.stats
            .inc(StatType::BlockProcessorSource, context.source.into());
        trace!(?result, block = %block.hash(), source = ?context.source, "Block processed");

        match result {
            BlockStatus::Progress => {
                self.unchecked.trigger(&hash.into());

                /*
                 * For send blocks check epoch open unchecked (gap pending).
                 * For state blocks check only send subtype and only if block epoch is not last epoch.
                 * If epoch is last, then pending entry shouldn't trigger same epoch open block for destination account.
                 * */
                let block = saved_block.unwrap();
                if block.block_type() == BlockType::LegacySend
                    || block.block_type() == BlockType::State
                        && block.is_send()
                        && block.epoch() < Epoch::MAX
                {
                    self.unchecked.trigger(&block.destination_or_link().into());
                }
            }
            BlockStatus::GapPrevious => {
                self.unchecked
                    .put(block.previous().into(), UncheckedInfo::new(block));
                self.stats.inc(StatType::Ledger, DetailType::GapPrevious);
            }
            BlockStatus::GapSource => {
                self.unchecked.put(
                    block
                        .source_field()
                        .unwrap_or(block.link_field().unwrap_or_default().into())
                        .into(),
                    UncheckedInfo::new(block),
                );
                self.stats.inc(StatType::Ledger, DetailType::GapSource);
            }
            BlockStatus::GapEpochOpenPending => {
                // Specific unchecked key starting with epoch open block account public key
                self.unchecked.put(
                    block.account_field().unwrap().into(),
                    UncheckedInfo::new(block),
                );
                self.stats.inc(StatType::Ledger, DetailType::GapSource);
            }
            BlockStatus::Old => {
                self.stats.inc(StatType::Ledger, DetailType::Old);
            }
            // These are unexpected and indicate erroneous/malicious behavior, log debug info to highlight the issue
            BlockStatus::BadSignature => {
                debug!("Block signature is invalid: {}", hash)
            }
            BlockStatus::NegativeSpend => {
                debug!("Block spends negative amount: {}", hash)
            }
            BlockStatus::Unreceivable => {
                debug!("Block is unreceivable: {}", hash)
            }
            BlockStatus::Fork => {
                self.stats.inc(StatType::Ledger, DetailType::Fork);
                debug!("Block is a fork: {}", hash)
            }
            BlockStatus::OpenedBurnAccount => {
                debug!("Block opens burn account: {}", hash)
            }
            BlockStatus::BalanceMismatch => {
                debug!("Block balance mismatch: {}", hash)
            }
            BlockStatus::RepresentativeMismatch => {
                debug!("Block representative mismatch: {}", hash)
            }
            BlockStatus::BlockPosition => {
                debug!("Block is in incorrect position: {}", hash)
            }
            BlockStatus::InsufficientWork => {
                debug!("Block has insufficient work: {}", hash)
            }
        }

        result
    }

    fn rollback_competitor(&self, transaction: &mut LmdbWriteTransaction, fork_block: &Block) {
        let hash = fork_block.hash();
        if let Some(successor) = self
            .ledger
            .any()
            .block_successor_by_qualified_root(transaction, &fork_block.qualified_root())
        {
            if successor != hash {
                // Replace our block with the winner and roll back any dependent blocks
                debug!("Rolling back: {} and replacing with: {}", successor, hash);
                let rollback_list = match self.ledger.rollback(transaction, &successor) {
                    Ok(rollback_list) => {
                        self.stats.inc(StatType::Ledger, DetailType::Rollback);
                        debug!("Blocks rolled back: {}", rollback_list.len());
                        rollback_list
                    }
                    Err((e, rollback_list)) => {
                        self.stats.inc(StatType::Ledger, DetailType::RollbackFailed);
                        error!(
                            ?e,
                            "Failed to roll back: {} because it or a successor was confirmed",
                            successor
                        );
                        rollback_list
                    }
                };

                // Notify observers of the rolled back blocks on a background thread while not holding the ledger write lock
                let notifications = self.notifications.clone();
                let fork_block = fork_block.clone();
                self.workers.post(Box::new(move || {
                    notifications.notify_rollback(&rollback_list, fork_block.qualified_root());
                }));
            }
        }
    }

    pub fn info(&self) -> FairQueueInfo<BlockSource> {
        let guard = self.mutex.lock().unwrap();
        guard.info()
    }

    pub fn container_info(&self) -> ContainerInfo {
        let guard = self.mutex.lock().unwrap();
        ContainerInfo::builder()
            .leaf("blocks", guard.queue.len(), size_of::<Arc<Block>>())
            .leaf(
                "forced",
                guard
                    .queue
                    .queue_len(&(BlockSource::Forced, ChannelId::LOOPBACK)),
                size_of::<Arc<Block>>(),
            )
            .node("queue", guard.queue.container_info())
            .finish()
    }
}

struct BlockProcessorImpl {
    pub queue: FairQueue<(BlockSource, ChannelId), Arc<BlockContext>>,
    pub last_log: Option<Instant>,
    stopped: bool,
}

impl BlockProcessorImpl {
    fn next(&mut self) -> Arc<BlockContext> {
        debug_assert!(!self.queue.is_empty()); // This should be checked before calling next
        if !self.queue.is_empty() {
            let ((source, _), request) = self.queue.next().unwrap();
            assert!(source != BlockSource::Forced || request.source == BlockSource::Forced);
            return request;
        }

        panic!("next() called when no blocks are ready");
    }

    pub fn should_log(&mut self) -> bool {
        if let Some(last) = &self.last_log {
            if last.elapsed() >= Duration::from_secs(15) {
                self.last_log = Some(Instant::now());
                return true;
            }
        }

        false
    }

    pub fn info(&self) -> FairQueueInfo<BlockSource> {
        self.queue.compacted_info(|(source, _)| *source)
    }
}

pub(crate) struct BlockProcessorCleanup(Arc<BlockProcessorLoopImpl>);

impl BlockProcessorCleanup {
    pub fn new(processor_loop: Arc<BlockProcessorLoopImpl>) -> Self {
        Self(processor_loop)
    }
}

impl DeadChannelCleanupStep for BlockProcessorCleanup {
    fn clean_up_dead_channels(&self, dead_channel_ids: &[ChannelId]) {
        let mut guard = self.0.mutex.lock().unwrap();
        for channel_id in dead_channel_ids {
            for source in BlockSource::iter() {
                guard.queue.remove(&(source, *channel_id))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::Direction;

    #[test]
    fn insufficient_work() {
        let config = BlockProcessorConfig::new(WorkThresholds::new_stub());
        let ledger = Arc::new(Ledger::new_null());
        let unchecked = Arc::new(UncheckedMap::default());
        let stats = Arc::new(Stats::default());
        let (notifications, _) = LedgerNotifications::new();
        let block_processor =
            BlockProcessor::new(config, ledger, unchecked, stats.clone(), notifications);

        let mut block = Block::new_test_instance();
        block.set_work(3);

        block_processor.add(block, BlockSource::Live, ChannelId::LOOPBACK);

        assert_eq!(
            stats.count(
                StatType::BlockProcessor,
                DetailType::InsufficientWork,
                Direction::In
            ),
            1
        );

        assert_eq!(block_processor.total_queue_len(), 0);
    }
}
