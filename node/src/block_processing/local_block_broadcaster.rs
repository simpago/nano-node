use super::{BlockSource, LedgerNotifications};
use crate::{
    cementation::ConfirmingSet,
    stats::{DetailType, Direction, StatType, Stats},
    transport::MessageFlooder,
};
use rsnano_core::{utils::ContainerInfo, Block, BlockHash, Networks};
use rsnano_ledger::{BlockStatus, Ledger};
use rsnano_messages::{Message, Publish};
use rsnano_network::{bandwidth_limiter::RateLimiter, TrafficType};
use std::{
    cmp::min,
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    mem::size_of,
    sync::{Arc, Condvar, Mutex, MutexGuard},
    thread::JoinHandle,
    time::{Duration, Instant},
};
use tracing::debug;

#[derive(Clone, Debug, PartialEq)]
pub struct LocalBlockBroadcasterConfig {
    pub max_size: usize,
    pub rebroadcast_interval: Duration,
    pub max_rebroadcast_interval: Duration,
    pub broadcast_rate_limit: usize,
    pub broadcast_rate_burst_ratio: f64,
    pub cleanup_interval: Duration,
}

impl LocalBlockBroadcasterConfig {
    pub fn new(network: Networks) -> Self {
        match network {
            Networks::NanoDevNetwork => Self::default_for_dev_network(),
            _ => Default::default(),
        }
    }

    fn default_for_dev_network() -> Self {
        Self {
            rebroadcast_interval: Duration::from_secs(1),
            cleanup_interval: Duration::from_secs(1),
            ..Default::default()
        }
    }
}

impl Default for LocalBlockBroadcasterConfig {
    fn default() -> Self {
        Self {
            max_size: 1024 * 8,
            rebroadcast_interval: Duration::from_secs(3),
            max_rebroadcast_interval: Duration::from_secs(60),
            broadcast_rate_limit: 32,
            broadcast_rate_burst_ratio: 3.0,
            cleanup_interval: Duration::from_secs(60),
        }
    }
}

///  Broadcasts blocks to the network
/// Tracks local blocks for more aggressive propagation
pub struct LocalBlockBroadcaster {
    config: LocalBlockBroadcasterConfig,
    notifications: LedgerNotifications,
    stats: Arc<Stats>,
    ledger: Arc<Ledger>,
    confirming_set: Arc<ConfirmingSet>,
    thread: Mutex<Option<JoinHandle<()>>>,
    enabled: bool,
    mutex: Mutex<LocalBlockBroadcasterData>,
    condition: Condvar,
    limiter: RateLimiter,
    message_flooder: Mutex<MessageFlooder>,
}

impl LocalBlockBroadcaster {
    pub(crate) fn new(
        config: LocalBlockBroadcasterConfig,
        notifications: LedgerNotifications,
        stats: Arc<Stats>,
        ledger: Arc<Ledger>,
        confirming_set: Arc<ConfirmingSet>,
        message_flooder: MessageFlooder,
        enabled: bool,
    ) -> Self {
        Self {
            limiter: RateLimiter::with_burst_ratio(
                config.broadcast_rate_limit,
                config.broadcast_rate_burst_ratio,
            ),
            config,
            notifications,
            stats,
            ledger,
            confirming_set,
            thread: Mutex::new(None),
            enabled,
            mutex: Mutex::new(LocalBlockBroadcasterData {
                stopped: false,
                local_blocks: Default::default(),
                cleanup_interval: Instant::now(),
            }),
            condition: Condvar::new(),
            message_flooder: Mutex::new(message_flooder),
        }
    }

    pub fn stop(&self) {
        self.mutex.lock().unwrap().stopped = true;
        self.condition.notify_all();
        let handle = self.thread.lock().unwrap().take();
        if let Some(handle) = handle {
            handle.join().unwrap();
        }
    }

    pub fn len(&self) -> usize {
        self.mutex.lock().unwrap().local_blocks.len()
    }

    pub fn contains(&self, hash: &BlockHash) -> bool {
        self.mutex.lock().unwrap().local_blocks.contains(hash)
    }

    fn run(&self) {
        let mut guard = self.mutex.lock().unwrap();
        while !guard.stopped {
            guard = self
                .condition
                .wait_timeout(guard, Duration::from_secs(1))
                .unwrap()
                .0;

            if !guard.stopped && !guard.local_blocks.is_empty() {
                self.stats
                    .inc(StatType::LocalBlockBroadcaster, DetailType::Loop);

                if guard.cleanup_interval.elapsed() >= self.config.cleanup_interval {
                    guard.cleanup_interval = Instant::now();
                    guard = self.cleanup(guard);
                }

                guard = self.run_broadcasts(guard);
            }
        }
    }

    fn rebroadcast_interval(&self, rebroadcasts: u32) -> Duration {
        min(
            self.config.rebroadcast_interval * rebroadcasts,
            self.config.max_rebroadcast_interval,
        )
    }

    fn run_broadcasts<'a>(
        &'a self,
        mut guard: MutexGuard<'a, LocalBlockBroadcasterData>,
    ) -> MutexGuard<'a, LocalBlockBroadcasterData> {
        let mut to_broadcast = Vec::new();

        // Iterate blocks with next_broadcast <= now
        for entry in guard.local_blocks.iter_by_next_broadcast(Instant::now()) {
            to_broadcast.push(entry.clone());
        }

        // Modify multi index container outside of the loop to avoid invalidating iterators
        for entry in &to_broadcast {
            guard.local_blocks.modify_entry(&entry.block.hash(), |i| {
                i.rebroadcasts += 1;
                let now = Instant::now();
                i.last_broadcast = Some(now);
                i.next_broadcast = now + self.rebroadcast_interval(i.rebroadcasts);
            });
        }

        drop(guard);

        for entry in to_broadcast {
            while !self.limiter.should_pass(1) {
                guard = self.mutex.lock().unwrap();
                guard = self
                    .condition
                    .wait_timeout_while(guard, Duration::from_millis(100), |g| !g.stopped)
                    .unwrap()
                    .0;
                if guard.stopped {
                    return guard;
                }
            }

            debug!(
                "Broadcasting block: {} (rebroadcasts so far: {})",
                entry.block.hash(),
                entry.rebroadcasts + 1,
            );

            self.stats.inc_dir(
                StatType::LocalBlockBroadcaster,
                DetailType::Broadcast,
                Direction::Out,
            );

            self.flood_block_initial((*entry.block).clone());
        }

        self.mutex.lock().unwrap()
    }

    fn cleanup<'a>(
        &'a self,
        mut data: MutexGuard<'a, LocalBlockBroadcasterData>,
    ) -> MutexGuard<'a, LocalBlockBroadcasterData> {
        // Copy the local blocks to avoid holding the mutex during IO
        let local_blocks_copy = data.local_blocks.all_entries();
        drop(data);
        let mut already_confirmed = HashSet::new();
        {
            let tx = self.ledger.read_txn();
            for entry in local_blocks_copy {
                // This block has never been broadcasted, keep it so it's broadcasted at least once
                if entry.last_broadcast.is_none() {
                    continue;
                }

                if self.confirming_set.contains(&entry.block.hash())
                    || self
                        .ledger
                        .confirmed()
                        .block_exists_or_pruned(&tx, &entry.block.hash())
                {
                    self.stats.inc(
                        StatType::LocalBlockBroadcaster,
                        DetailType::AlreadyConfirmed,
                    );
                    already_confirmed.insert(entry.block.hash());
                }
            }
        }

        data = self.mutex.lock().unwrap();
        // Erase blocks that have been confirmed

        data.local_blocks
            .retain(|e| !already_confirmed.contains(&e.block.hash()));

        data
    }

    pub fn container_info(&self) -> ContainerInfo {
        let guard = self.mutex.lock().unwrap();
        [(
            "local",
            guard.local_blocks.len(),
            OrderedLocals::ELEMENT_SIZE,
        )]
        .into()
    }

    /// Flood block to all PRs and a random selection of non-PRs
    pub fn flood_block_initial(&self, block: Block) {
        let message = Message::Publish(Publish::new_from_originator(block));
        let mut publisher = self.message_flooder.lock().unwrap();
        publisher.flood_prs_and_some_non_prs(&message, TrafficType::BlockBroadcastInitial, 1.0);
    }
}

impl Drop for LocalBlockBroadcaster {
    fn drop(&mut self) {
        // Thread must be stopped before destruction
        debug_assert!(self.thread.lock().unwrap().is_none())
    }
}

pub trait LocalBlockBroadcasterExt {
    fn initialize(&self);
    fn start(&self);
}

impl LocalBlockBroadcasterExt for Arc<LocalBlockBroadcaster> {
    fn initialize(&self) {
        if !self.enabled {
            return;
        }

        let self_w = Arc::downgrade(self);
        self.notifications
            .on_batch_processed(Box::new(move |batch| {
                let Some(self_l) = self_w.upgrade() else {
                    return;
                };
                let mut should_notify = false;
                for (result, context) in batch {
                    // Only rebroadcast local blocks that were successfully processed (no forks or gaps)
                    if *result == BlockStatus::Progress && context.source == BlockSource::Local {
                        let mut guard = self_l.mutex.lock().unwrap();
                        guard.local_blocks.push_back(LocalEntry {
                            block: Arc::new(context.block.lock().unwrap().clone()),
                            last_broadcast: None,
                            next_broadcast: Instant::now(),
                            rebroadcasts: 0,
                        });
                        self_l
                            .stats
                            .inc(StatType::LocalBlockBroadcaster, DetailType::Insert);

                        // Erase oldest blocks if the queue gets too big
                        while guard.local_blocks.len() > self_l.config.max_size {
                            self_l
                                .stats
                                .inc(StatType::LocalBlockBroadcaster, DetailType::EraseOldest);
                            guard.local_blocks.pop_front();
                        }

                        should_notify = true;
                    }
                }
                if should_notify {
                    self_l.condition.notify_all();
                }
            }));

        let self_w = Arc::downgrade(self);
        self.notifications
            .on_blocks_rolled_back(move |blocks, _rollback_root| {
                let Some(self_l) = self_w.upgrade() else {
                    return;
                };

                let mut guard = self_l.mutex.lock().unwrap();
                for block in blocks {
                    if guard.local_blocks.remove(&block.hash()) {
                        self_l.stats.inc_dir(
                            StatType::LocalBlockBroadcaster,
                            DetailType::Rollback,
                            Direction::In,
                        );
                    }
                }
            });

        let self_w = Arc::downgrade(self);
        self.confirming_set.on_cemented(Box::new(move |block| {
            let Some(self_l) = self_w.upgrade() else {
                return;
            };

            let mut guard = self_l.mutex.lock().unwrap();
            if guard.local_blocks.remove(&block.hash()) {
                self_l
                    .stats
                    .inc(StatType::LocalBlockBroadcaster, DetailType::Cemented);
            }
        }));
    }

    fn start(&self) {
        if !self.enabled {
            return;
        }

        debug_assert!(self.thread.lock().unwrap().is_none());
        let self_l = Arc::clone(self);
        *self.thread.lock().unwrap() = Some(
            std::thread::Builder::new()
                .name("Local broadcast".to_string())
                .spawn(move || self_l.run())
                .unwrap(),
        );
    }
}

struct LocalBlockBroadcasterData {
    stopped: bool,
    local_blocks: OrderedLocals,
    cleanup_interval: Instant,
}

#[derive(Clone)]
struct LocalEntry {
    block: Arc<Block>,
    last_broadcast: Option<Instant>,
    next_broadcast: Instant,
    rebroadcasts: u32,
}

#[derive(Default)]
struct OrderedLocals {
    by_hash: HashMap<BlockHash, LocalEntry>,
    sequenced: VecDeque<BlockHash>,
    by_next_broadcast: BTreeMap<Instant, Vec<BlockHash>>,
}

impl OrderedLocals {
    pub const ELEMENT_SIZE: usize = size_of::<LocalEntry>() + size_of::<BlockHash>() * 2;
    fn len(&self) -> usize {
        self.sequenced.len()
    }

    fn is_empty(&self) -> bool {
        self.sequenced.is_empty()
    }

    fn contains(&self, hash: &BlockHash) -> bool {
        self.by_hash.contains_key(hash)
    }

    fn push_back(&mut self, entry: LocalEntry) {
        let hash = entry.block.hash();
        let next_broadcast = entry.next_broadcast;
        if let Some(old) = self.by_hash.insert(entry.block.hash(), entry) {
            self.sequenced.retain(|i| *i != old.block.hash());
        }
        self.sequenced.push_back(hash);
        self.by_next_broadcast
            .entry(next_broadcast)
            .or_default()
            .push(hash);
    }

    fn iter_by_next_broadcast(&self, upper_bound: Instant) -> impl Iterator<Item = &LocalEntry> {
        self.by_next_broadcast
            .values()
            .flat_map(|hashes| hashes.iter().map(|h| self.by_hash.get(h).unwrap()))
            .take_while(move |i| i.next_broadcast <= upper_bound)
    }

    fn modify_entry(&mut self, hash: &BlockHash, mut f: impl FnMut(&mut LocalEntry)) {
        if let Some(entry) = self.by_hash.get_mut(hash) {
            let old_next_broadcast = entry.next_broadcast;
            f(entry);
            if entry.next_broadcast != old_next_broadcast {
                remove_by_next_broadcast(&mut self.by_next_broadcast, old_next_broadcast, hash);
                self.by_next_broadcast
                    .entry(entry.next_broadcast)
                    .or_default()
                    .push(*hash);
            }
        }
    }

    fn pop_front(&mut self) -> Option<LocalEntry> {
        let hash = self.sequenced.pop_front()?;
        let entry = self.by_hash.remove(&hash).unwrap();
        remove_by_next_broadcast(&mut self.by_next_broadcast, entry.next_broadcast, &hash);
        Some(entry)
    }

    fn remove(&mut self, hash: &BlockHash) -> bool {
        if let Some(entry) = self.by_hash.remove(hash) {
            self.sequenced.retain(|i| i != hash);
            remove_by_next_broadcast(&mut self.by_next_broadcast, entry.next_broadcast, hash);
            true
        } else {
            false
        }
    }

    fn retain(&mut self, mut f: impl FnMut(&LocalEntry) -> bool) {
        self.by_hash.retain(|hash, entry| {
            let retain = f(entry);
            if !retain {
                self.sequenced.retain(|i| i != hash);
                remove_by_next_broadcast(&mut self.by_next_broadcast, entry.next_broadcast, hash);
            }
            retain
        });
    }

    fn all_entries(&self) -> Vec<LocalEntry> {
        self.by_hash.values().cloned().collect()
    }
}

fn remove_by_next_broadcast(
    map: &mut BTreeMap<Instant, Vec<BlockHash>>,
    next: Instant,
    hash: &BlockHash,
) {
    let mut hashes = map.remove(&next).unwrap();

    if hashes.len() > 1 {
        hashes.retain(|i| i != hash);
        map.insert(next, hashes);
    }
}
