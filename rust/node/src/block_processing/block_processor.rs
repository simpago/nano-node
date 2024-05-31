use super::UncheckedMap;
use crate::{
    config::{NodeConfig, NodeFlags},
    stats::{DetailType, StatType, Stats},
    transport::{ChannelEnum, FairQueue, Origin},
};
use rsnano_core::{
    utils::{ContainerInfo, ContainerInfoComponent},
    work::{WorkThresholds, WORK_THRESHOLDS_STUB},
    BlockEnum, BlockType, Epoch, HackyUnsafeMutBlock, HashOrAccount, UncheckedInfo,
};
use rsnano_ledger::{BlockStatus, Ledger, Writer};
use rsnano_store_lmdb::LmdbWriteTransaction;
use std::{
    mem::size_of,
    sync::{Arc, Condvar, Mutex},
    thread::JoinHandle,
    time::{Duration, Instant},
};
use tracing::{debug, error, info, trace};

#[derive(FromPrimitive, Copy, Clone, PartialEq, Eq, Debug, PartialOrd, Ord)]
pub enum BlockSource {
    Unknown = 0,
    Live,
    Bootstrap,
    BootstrapLegacy,
    Unchecked,
    Local,
    Forced,
}

impl From<BlockSource> for DetailType {
    fn from(value: BlockSource) -> Self {
        match value {
            BlockSource::Unknown => DetailType::Unknown,
            BlockSource::Live => DetailType::Live,
            BlockSource::Bootstrap => DetailType::Bootstrap,
            BlockSource::BootstrapLegacy => DetailType::BootstrapLegacy,
            BlockSource::Unchecked => DetailType::Unchecked,
            BlockSource::Local => DetailType::Local,
            BlockSource::Forced => DetailType::Forced,
        }
    }
}

pub struct BlockProcessorContext {
    pub block: Arc<BlockEnum>,
    pub source: BlockSource,
    pub arrival: Instant,
    pub result: Mutex<Option<BlockStatus>>,
    condition: Condvar,
}

impl BlockProcessorContext {
    pub fn new(block: Arc<BlockEnum>, source: BlockSource) -> Self {
        Self {
            block,
            source,
            arrival: Instant::now(),
            result: Mutex::new(None),
            condition: Condvar::new(),
        }
    }

    pub fn set_result(&self, result: BlockStatus) {
        *self.result.lock().unwrap() = Some(result);
        self.condition.notify_all();
    }

    pub fn wait_result(&self, timeout: Duration) -> Option<BlockStatus> {
        let guard = self.result.lock().unwrap();
        if guard.is_some() {
            return *guard;
        }

        *self
            .condition
            .wait_timeout_while(guard, timeout, |i| i.is_none())
            .unwrap()
            .0
    }
}

pub struct BlockProcessor {
    thread: Mutex<Option<JoinHandle<()>>>,
    processor_loop: Arc<BlockProcessorLoop>,
}

impl BlockProcessor {
    pub fn new(
        config: Arc<NodeConfig>,
        flags: Arc<NodeFlags>,
        ledger: Arc<Ledger>,
        unchecked_map: Arc<UncheckedMap>,
        stats: Arc<Stats>,
        work: Arc<WorkThresholds>,
    ) -> Self {
        let processor_config = config.block_processor.clone();
        let max_size_query = Box::new(move |origin: &Origin<BlockSource>| match origin.source {
            BlockSource::Live => processor_config.max_peer_queue,
            _ => processor_config.max_system_queue,
        });

        let processor_config = config.block_processor.clone();
        let priority_query = Box::new(move |origin: &Origin<BlockSource>| match origin.source {
            BlockSource::Live => processor_config.priority_live,
            BlockSource::Bootstrap | BlockSource::BootstrapLegacy | BlockSource::Unchecked => {
                processor_config.priority_bootstrap
            }
            BlockSource::Local => processor_config.priority_local,
            _ => 1,
        });

        Self {
            processor_loop: Arc::new(BlockProcessorLoop {
                mutex: Mutex::new(BlockProcessorImpl {
                    queue: FairQueue::new(max_size_query, priority_query),
                    last_log: None,
                    config: Arc::clone(&config),
                    stopped: false,
                }),
                condition: Condvar::new(),
                ledger,
                unchecked_map,
                config,
                stats,
                work,
                flags,
                blocks_rolled_back: Mutex::new(None),
                block_rolled_back: Mutex::new(Vec::new()),
                block_processed: Mutex::new(Vec::new()),
                batch_processed: Mutex::new(Vec::new()),
            }),
            thread: Mutex::new(None),
        }
    }

    pub fn new_test_instance(ledger: Arc<Ledger>) -> Self {
        BlockProcessor::new(
            Arc::new(NodeConfig::new_null()),
            Arc::new(NodeFlags::default()),
            ledger,
            Arc::new(UncheckedMap::default()),
            Arc::new(Stats::default()),
            Arc::new(WORK_THRESHOLDS_STUB.clone()),
        )
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
        if let Some(join_handle) = self.thread.lock().unwrap().take() {
            join_handle.join().unwrap();
        }
    }

    pub fn full(&self) -> bool {
        self.processor_loop.full()
    }

    pub fn half_full(&self) -> bool {
        self.processor_loop.half_full()
    }

    pub fn total_queue_len(&self) -> usize {
        self.processor_loop.total_queue_len()
    }

    pub fn queue_len(&self, source: BlockSource) -> usize {
        self.processor_loop.queue_len(source)
    }

    pub fn add_block_processed_observer(
        &self,
        observer: Box<dyn Fn(BlockStatus, &BlockProcessorContext) + Send + Sync>,
    ) {
        self.processor_loop.add_block_processed_observer(observer);
    }

    pub fn add_batch_processed_observer(
        &self,
        observer: Box<dyn Fn(&[(BlockStatus, Arc<BlockProcessorContext>)]) + Send + Sync>,
    ) {
        self.processor_loop.add_batch_processed_observer(observer);
    }

    pub fn add_rolled_back_observer(&self, observer: Box<dyn Fn(&BlockEnum) + Send + Sync>) {
        self.processor_loop.add_rolled_back_observer(observer);
    }

    pub fn add(
        &self,
        block: Arc<BlockEnum>,
        source: BlockSource,
        channel: Option<Arc<ChannelEnum>>,
    ) -> bool {
        self.processor_loop.add(block, source, channel)
    }

    pub fn add_blocking(&self, block: Arc<BlockEnum>, source: BlockSource) -> Option<BlockStatus> {
        self.processor_loop.add_blocking(block, source)
    }

    pub fn process_active(&self, block: Arc<BlockEnum>) {
        self.processor_loop.process_active(block);
    }

    pub fn notify_block_rolled_back(&self, block: &BlockEnum) {
        self.processor_loop.notify_block_rolled_back(block)
    }

    pub fn set_blocks_rolled_back_callback(
        &self,
        callback: Box<dyn Fn(Vec<BlockEnum>, BlockEnum) + Send + Sync>,
    ) {
        self.processor_loop
            .set_blocks_rolled_back_callback(callback);
    }
    pub fn force(&self, block: Arc<BlockEnum>) {
        self.processor_loop.force(block);
    }

    pub fn collect_container_info(&self, name: impl Into<String>) -> ContainerInfoComponent {
        self.processor_loop.collect_container_info(name)
    }
}

impl Drop for BlockProcessor {
    fn drop(&mut self) {
        // Thread must be stopped before destruction
        debug_assert!(self.thread.lock().unwrap().is_none());
    }
}

struct BlockProcessorLoop {
    mutex: Mutex<BlockProcessorImpl>,
    condition: Condvar,
    ledger: Arc<Ledger>,
    unchecked_map: Arc<UncheckedMap>,
    config: Arc<NodeConfig>,
    stats: Arc<Stats>,
    work: Arc<WorkThresholds>,
    flags: Arc<NodeFlags>,
    blocks_rolled_back: Mutex<Option<Box<dyn Fn(Vec<BlockEnum>, BlockEnum) + Send + Sync>>>,
    block_rolled_back: Mutex<Vec<Box<dyn Fn(&BlockEnum) + Send + Sync>>>,
    block_processed: Mutex<Vec<Box<dyn Fn(BlockStatus, &BlockProcessorContext) + Send + Sync>>>,
    batch_processed:
        Mutex<Vec<Box<dyn Fn(&[(BlockStatus, Arc<BlockProcessorContext>)]) + Send + Sync>>>,
}

impl BlockProcessorLoop {
    pub fn run(&self) {
        let mut guard = self.mutex.lock().unwrap();
        while !guard.stopped {
            if !guard.queue.is_empty() {
                drop(guard);

                let mut processed = self.process_batch();

                // Set results for futures when not holding the lock
                for (result, context) in processed.iter_mut() {
                    context.set_result(*result);
                }

                self.notify_batch_processed(&processed);

                guard = self.mutex.lock().unwrap();
            } else {
                self.condition.notify_one();
                guard = self.condition.wait(guard).unwrap();
            }
        }
    }

    fn notify_batch_processed(&self, blocks: &Vec<(BlockStatus, Arc<BlockProcessorContext>)>) {
        {
            let guard = self.block_processed.lock().unwrap();
            for observer in guard.iter() {
                for (status, context) in blocks {
                    observer(*status, context);
                }
            }
        }
        {
            let guard = self.batch_processed.lock().unwrap();
            for observer in guard.iter() {
                observer(&blocks);
            }
        }
    }

    pub fn add_block_processed_observer(
        &self,
        observer: Box<dyn Fn(BlockStatus, &BlockProcessorContext) + Send + Sync>,
    ) {
        self.block_processed.lock().unwrap().push(observer);
    }

    pub fn add_batch_processed_observer(
        &self,
        observer: Box<dyn Fn(&[(BlockStatus, Arc<BlockProcessorContext>)]) + Send + Sync>,
    ) {
        self.batch_processed.lock().unwrap().push(observer);
    }

    pub fn add_rolled_back_observer(&self, observer: Box<dyn Fn(&BlockEnum) + Send + Sync>) {
        self.block_rolled_back.lock().unwrap().push(observer);
    }

    pub fn notify_block_rolled_back(&self, block: &BlockEnum) {
        for observer in self.block_rolled_back.lock().unwrap().iter() {
            observer(block)
        }
    }

    pub fn set_blocks_rolled_back_callback(
        &self,
        callback: Box<dyn Fn(Vec<BlockEnum>, BlockEnum) + Send + Sync>,
    ) {
        *self.blocks_rolled_back.lock().unwrap() = Some(callback);
    }

    pub fn process_active(&self, block: Arc<BlockEnum>) {
        self.add(block, BlockSource::Live, None);
    }

    pub fn add(
        &self,
        block: Arc<BlockEnum>,
        source: BlockSource,
        channel: Option<Arc<ChannelEnum>>,
    ) -> bool {
        if self.work.validate_entry_block(&block) {
            // true => error
            self.stats
                .inc(StatType::Blockprocessor, DetailType::InsufficientWork);
            return false; // Not added
        }

        self.stats
            .inc(StatType::Blockprocessor, DetailType::Process);
        debug!(
            "Processing block (async): {} (source: {:?} {})",
            block.hash(),
            source,
            channel
                .as_ref()
                .map(|c| c.remote_endpoint().to_string())
                .unwrap_or_else(|| "<unknown>".to_string())
        );

        self.add_impl(Arc::new(BlockProcessorContext::new(block, source)), channel)
    }

    pub fn add_blocking(&self, block: Arc<BlockEnum>, source: BlockSource) -> Option<BlockStatus> {
        self.stats
            .inc(StatType::Blockprocessor, DetailType::ProcessBlocking);
        debug!(
            "Processing block (blocking): {} (source: {:?})",
            block.hash(),
            source
        );

        let ctx = Arc::new(BlockProcessorContext::new(block, source));
        let ctx_clone = Arc::clone(&ctx);
        self.add_impl(ctx, None);

        match ctx_clone.wait_result(Duration::from_secs(
            self.config.block_process_timeout_s as u64,
        )) {
            Some(status) => Some(status),
            None => {
                self.stats
                    .inc(StatType::Blockprocessor, DetailType::ProcessBlockingTimeout);
                error!("Timeout processing block: {}", ctx_clone.block.hash());
                None
            }
        }
    }

    pub fn force(&self, block: Arc<BlockEnum>) {
        self.stats.inc(StatType::Blockprocessor, DetailType::Force);
        debug!("Forcing block: {}", block.hash());
        let ctx = Arc::new(BlockProcessorContext::new(block, BlockSource::Forced));
        self.add_impl(ctx, None);
    }

    pub fn full(&self) -> bool {
        self.total_queue_len() >= self.flags.block_processor_full_size
    }

    pub fn half_full(&self) -> bool {
        self.total_queue_len() >= self.flags.block_processor_full_size / 2
    }

    // TODO: Remove and replace all checks with calls to size (block_source)
    pub fn total_queue_len(&self) -> usize {
        self.mutex.lock().unwrap().queue.len()
    }

    pub fn queue_len(&self, source: BlockSource) -> usize {
        self.mutex.lock().unwrap().queue.queue_len(&source.into())
    }

    fn add_impl(
        &self,
        context: Arc<BlockProcessorContext>,
        channel: Option<Arc<ChannelEnum>>,
    ) -> bool {
        let source = context.source;
        let added;
        {
            let mut guard = self.mutex.lock().unwrap();
            added = guard.queue.push(context, Origin::new_opt(source, channel));
        }
        if added {
            self.condition.notify_all();
        } else {
            self.stats
                .inc(StatType::Blockprocessor, DetailType::Overfill);
            self.stats
                .inc(StatType::BlockprocessorOverfill, source.into());
        }
        added
    }

    pub fn queue_unchecked(&self, hash_or_account: &HashOrAccount) {
        self.unchecked_map.trigger(hash_or_account);
    }

    pub fn process_batch(&self) -> Vec<(BlockStatus, Arc<BlockProcessorContext>)> {
        let mut processed = Vec::new();

        let _scoped_write_guard = self.ledger.write_queue.wait(Writer::ProcessBatch);
        let mut transaction = self.ledger.rw_txn();
        let mut lock_a = self.mutex.lock().unwrap();

        lock_a.queue.periodic_update(Duration::from_secs(30));

        let timer_l = Instant::now();

        // Processing blocks
        let mut number_of_blocks_processed = 0;
        let mut number_of_forced_processed = 0;

        let deadline_reached = || {
            timer_l.elapsed()
                > Duration::from_millis(self.config.block_processor_batch_max_time_ms as u64)
        };

        while !lock_a.queue.is_empty()
            && (!deadline_reached()
                || number_of_blocks_processed < self.flags.block_processor_batch_size)
        {
            // TODO: Cleaner periodical logging
            if lock_a.should_log() {
                info!(
                    "{} blocks (+ {} forced) in processing queue",
                    lock_a.queue.len(),
                    lock_a.queue.queue_len(&BlockSource::Forced.into())
                );
            }
            let context = lock_a.next();
            let force = context.source == BlockSource::Forced;

            drop(lock_a);

            if force {
                number_of_forced_processed += 1;
                self.rollback_competitor(&mut transaction, &context.block);
            }

            number_of_blocks_processed += 1;

            let result = self.process_one(&mut transaction, &context);
            processed.push((result, context));

            lock_a = self.mutex.lock().unwrap();
        }

        drop(lock_a);

        if number_of_blocks_processed != 0 && timer_l.elapsed() > Duration::from_millis(100) {
            debug!(
                "Processed {} blocks ({} blocks were forced) in {} ms",
                number_of_blocks_processed,
                number_of_forced_processed,
                timer_l.elapsed().as_millis(),
            );
        }
        processed
    }

    pub fn process_one(
        &self,
        txn: &mut LmdbWriteTransaction,
        context: &BlockProcessorContext,
    ) -> BlockStatus {
        let block = &context.block;
        let hash = block.hash();
        let mutable_block = unsafe { block.undefined_behavior_mut() };

        let result = match self.ledger.process(txn, mutable_block) {
            Ok(()) => BlockStatus::Progress,
            Err(r) => r,
        };

        self.stats
            .inc(StatType::BlockprocessorResult, result.into());
        self.stats
            .inc(StatType::BlockprocessorSource, context.source.into());
        trace!(?result, block = %block.hash(), source = ?context.source, "Block processed");

        match result {
            BlockStatus::Progress => {
                self.queue_unchecked(&hash.into());
                /* For send blocks check epoch open unchecked (gap pending).
                For state blocks check only send subtype and only if block epoch is not last epoch.
                If epoch is last, then pending entry shouldn't trigger same epoch open block for destination account. */
                if block.block_type() == BlockType::LegacySend
                    || block.block_type() == BlockType::State
                        && block.is_send()
                        && block.sideband().unwrap().details.epoch < Epoch::MAX
                {
                    /* block->destination () for legacy send blocks
                    block->link () for state blocks (send subtype) */
                    self.queue_unchecked(&block.destination_or_link().into());
                }
            }
            BlockStatus::GapPrevious => {
                self.unchecked_map.put(
                    block.previous().into(),
                    UncheckedInfo::new(Arc::clone(block)),
                );
                self.stats.inc(StatType::Ledger, DetailType::GapPrevious);
            }
            BlockStatus::GapSource => {
                self.unchecked_map.put(
                    block
                        .source_field()
                        .unwrap_or(block.link_field().unwrap_or_default().into())
                        .into(),
                    UncheckedInfo::new(Arc::clone(block)),
                );
                self.stats.inc(StatType::Ledger, DetailType::GapSource);
            }
            BlockStatus::GapEpochOpenPending => {
                // Specific unchecked key starting with epoch open block account public key
                self.unchecked_map.put(
                    block.account().into(),
                    UncheckedInfo::new(Arc::clone(block)),
                );
                self.stats.inc(StatType::Ledger, DetailType::GapSource);
            }
            BlockStatus::Old => {
                self.stats.inc(StatType::Ledger, DetailType::Old);
            }
            BlockStatus::BadSignature => {}
            BlockStatus::NegativeSpend => {}
            BlockStatus::Unreceivable => {}
            BlockStatus::Fork => {
                self.stats.inc(StatType::Ledger, DetailType::Fork);
            }
            BlockStatus::OpenedBurnAccount => {}
            BlockStatus::BalanceMismatch => {}
            BlockStatus::RepresentativeMismatch => {}
            BlockStatus::BlockPosition => {}
            BlockStatus::InsufficientWork => {}
        }

        result
    }

    fn rollback_competitor(&self, transaction: &mut LmdbWriteTransaction, block: &Arc<BlockEnum>) {
        let hash = block.hash();
        if let Some(successor) = self
            .ledger
            .successor_by_root(transaction, &block.qualified_root())
        {
            let successor_block = self
                .ledger
                .any()
                .get_block(transaction, &successor)
                .unwrap();
            if successor != hash {
                // Replace our block with the winner and roll back any dependent blocks
                debug!("Rolling back: {} and replacing with: {}", successor, hash);
                let rollback_list = match self.ledger.rollback(transaction, &successor) {
                    Ok(rollback_list) => {
                        self.stats.inc(StatType::Ledger, DetailType::Rollback);
                        debug!("Blocks rolled back: {}", rollback_list.len());
                        rollback_list
                    }
                    Err(_) => {
                        self.stats.inc(StatType::Ledger, DetailType::RollbackFailed);
                        error!(
                            "Failed to roll back: {} because it or a successor was confirmed",
                            successor
                        );
                        Vec::new()
                    }
                };

                let callback_guard = self.blocks_rolled_back.lock().unwrap();
                if let Some(callback) = callback_guard.as_ref() {
                    callback(rollback_list, successor_block);
                }
            }
        }
    }

    pub fn collect_container_info(&self, name: impl Into<String>) -> ContainerInfoComponent {
        let guard = self.mutex.lock().unwrap();
        ContainerInfoComponent::Composite(
            name.into(),
            vec![
                ContainerInfoComponent::Leaf(ContainerInfo {
                    name: "blocks".to_owned(),
                    count: guard.queue.len(),
                    sizeof_element: size_of::<Arc<BlockEnum>>(),
                }),
                ContainerInfoComponent::Leaf(ContainerInfo {
                    name: "forced".to_owned(),
                    count: guard.queue.queue_len(&BlockSource::Forced.into()),
                    sizeof_element: size_of::<Arc<BlockEnum>>(),
                }),
                guard.queue.collect_container_info("queue"),
            ],
        )
    }
}

struct BlockProcessorImpl {
    pub queue: FairQueue<Arc<BlockProcessorContext>, BlockSource>,
    pub last_log: Option<Instant>,
    config: Arc<NodeConfig>,
    stopped: bool,
}

impl BlockProcessorImpl {
    fn next(&mut self) -> Arc<BlockProcessorContext> {
        debug_assert!(!self.queue.is_empty()); // This should be checked before calling next
        if !self.queue.is_empty() {
            let (request, origin) = self.queue.next().unwrap();
            assert!(origin.source != BlockSource::Forced || request.source == BlockSource::Forced);
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
}
