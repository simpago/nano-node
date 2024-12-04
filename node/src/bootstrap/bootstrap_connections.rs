use super::{
    bootstrap_limits, BootstrapAttempts, BootstrapClient, BootstrapInitiator,
    BootstrapInitiatorConfig, BootstrapMode, BootstrapStrategy, BulkPullClient,
    BulkPullClientConfig, BulkPullClientExt, PullInfo, PullsCache,
};
use crate::{
    block_processing::BlockProcessor,
    stats::{DetailType, Direction, StatType, Stats},
    transport::MessagePublisher,
    utils::{ThreadPool, ThreadPoolImpl},
};
use async_trait::async_trait;
use ordered_float::OrderedFloat;
use rsnano_core::{Account, BlockHash, Networks};
use rsnano_network::{
    ChannelDirection, ChannelMode, Network, NetworkInfo, NetworkObserver, NullNetworkObserver,
};
use rsnano_nullable_clock::SteadyClock;
use rsnano_nullable_tcp::TcpStreamFactory;
use std::{
    cmp::{max, min},
    collections::{BinaryHeap, HashSet, VecDeque},
    net::{Ipv6Addr, SocketAddrV6},
    ops::Deref,
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc, Condvar, Mutex, MutexGuard, RwLock, Weak,
    },
    time::Duration,
};
use tracing::debug;

/// Container for bootstrap_client objects. Owned by bootstrap_initiator which pools open connections and makes them available
/// for use by different bootstrap sessions.
pub struct BootstrapConnections {
    condition: Condvar,
    populate_connections_started: AtomicBool,
    attempts: Arc<Mutex<BootstrapAttempts>>,
    mutex: Mutex<BootstrapConnectionsData>,
    config: BootstrapInitiatorConfig,
    pub connections_count: AtomicU32,
    new_connections_empty: AtomicBool,
    stopped: AtomicBool,
    network: Arc<Network>,
    network_info: Arc<RwLock<NetworkInfo>>,
    network_stats: Arc<dyn NetworkObserver>,
    workers: Arc<dyn ThreadPool>,
    tokio: tokio::runtime::Handle,
    stats: Arc<Stats>,
    block_processor: Arc<BlockProcessor>,
    bootstrap_initiator: Mutex<Option<Weak<BootstrapInitiator>>>,
    pulls_cache: Arc<Mutex<PullsCache>>,
    message_publisher: MessagePublisher,
    clock: Arc<SteadyClock>,
}

impl BootstrapConnections {
    pub fn new(
        attempts: Arc<Mutex<BootstrapAttempts>>,
        config: BootstrapInitiatorConfig,
        network: Arc<Network>,
        network_info: Arc<RwLock<NetworkInfo>>,
        network_stats: Arc<dyn NetworkObserver>,
        tokio: tokio::runtime::Handle,
        workers: Arc<dyn ThreadPool>,
        stats: Arc<Stats>,
        block_processor: Arc<BlockProcessor>,
        pulls_cache: Arc<Mutex<PullsCache>>,
        message_publisher: MessagePublisher,
        clock: Arc<SteadyClock>,
    ) -> Self {
        Self {
            condition: Condvar::new(),
            populate_connections_started: AtomicBool::new(false),
            attempts,
            mutex: Mutex::new(BootstrapConnectionsData {
                pulls: VecDeque::new(),
                clients: VecDeque::new(),
                idle: VecDeque::new(),
            }),
            config,
            connections_count: AtomicU32::new(0),
            new_connections_empty: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            network,
            network_info,
            network_stats,
            workers,
            tokio,
            stats,
            block_processor,
            pulls_cache,
            bootstrap_initiator: Mutex::new(None),
            message_publisher,
            clock,
        }
    }

    pub fn new_null(tokio_handle: tokio::runtime::Handle) -> Self {
        Self {
            condition: Condvar::new(),
            populate_connections_started: AtomicBool::new(false),
            attempts: Arc::new(Mutex::new(BootstrapAttempts::new())),
            mutex: Mutex::new(BootstrapConnectionsData::default()),
            config: BootstrapInitiatorConfig::default_for(Networks::NanoDevNetwork),
            connections_count: AtomicU32::new(0),
            new_connections_empty: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            network: Arc::new(Network::new_null(tokio_handle.clone())),
            network_info: Arc::new(RwLock::new(NetworkInfo::new_test_instance())),
            network_stats: Arc::new(NullNetworkObserver::new()),
            workers: Arc::new(ThreadPoolImpl::new_null()),
            tokio: tokio_handle.clone(),
            stats: Arc::new(Stats::default()),
            block_processor: Arc::new(BlockProcessor::new_null()),
            bootstrap_initiator: Mutex::new(None),
            pulls_cache: Arc::new(Mutex::new(PullsCache::new())),
            message_publisher: MessagePublisher::new_null(tokio_handle.clone()),
            clock: Arc::new(SteadyClock::new_null()),
        }
    }

    pub fn set_bootstrap_initiator(&self, initiator: Arc<BootstrapInitiator>) {
        *self.bootstrap_initiator.lock().unwrap() = Some(Arc::downgrade(&initiator));
    }

    pub fn target_connections(&self, pulls_remaining: usize, attempts_count: usize) -> u32 {
        let attempts_factor = self.config.bootstrap_connections * attempts_count as u32;
        if attempts_factor >= self.config.bootstrap_connections_max {
            return max(1, self.config.bootstrap_connections_max);
        }

        // Only scale up to bootstrap_connections_max for large pulls.
        let step_scale = min(
            OrderedFloat(1f64),
            max(
                OrderedFloat(0f64),
                OrderedFloat(
                    pulls_remaining as f64
                        / bootstrap_limits::BOOTSTRAP_CONNECTION_SCALE_TARGET_BLOCKS as f64,
                ),
            ),
        );
        let target = attempts_factor as f64
            + (self.config.bootstrap_connections_max - attempts_factor) as f64 * step_scale.0;
        return max(1, (target + 0.5) as u32);
    }

    pub fn connection(&self, use_front_connection: bool) -> (Option<Arc<BootstrapClient>>, bool) {
        let mut guard = self.mutex.lock().unwrap();
        guard = self
            .condition
            .wait_while(guard, |i| {
                !self.stopped.load(Ordering::SeqCst)
                    && i.idle.is_empty()
                    && !self.new_connections_empty.load(Ordering::SeqCst)
            })
            .unwrap();

        let mut result = None;
        if !self.stopped.load(Ordering::SeqCst) && !guard.idle.is_empty() {
            if !use_front_connection {
                result = guard.idle.pop_back();
            } else {
                result = guard.idle.pop_front();
            }
        }
        if result.is_none()
            && self.connections_count.load(Ordering::SeqCst) == 0
            && self.new_connections_empty.load(Ordering::SeqCst)
        {
            (result, true) // should stop
        } else {
            (result, false) //don't stop
        }
    }

    pub fn bootstrap_client_closed(&self) {
        self.connections_count.fetch_sub(1, Ordering::SeqCst);
    }

    pub fn clear_pulls(&self, bootstrap_id_a: u64) {
        {
            let mut guard = self.mutex.lock().unwrap();

            guard.pulls.retain(|i| i.bootstrap_id != bootstrap_id_a);
        }
        self.condition.notify_all();
    }

    pub fn stop(&self) {
        let lock = self.mutex.lock().unwrap();
        self.stopped.store(true, Ordering::SeqCst);
        drop(lock);
        self.condition.notify_all();
        let mut lock = self.mutex.lock().unwrap();
        for i in &lock.clients {
            if let Some(client) = i.upgrade() {
                client.close();
            }
        }
        lock.clients.clear();
        lock.idle.clear();
    }
}

#[async_trait]
pub trait BootstrapConnectionsExt {
    fn pool_connection(&self, client: Arc<BootstrapClient>, new_client: bool, push_front: bool);
    fn requeue_pull(&self, pull: PullInfo, network_error: bool);
    fn run(&self);
    fn start_populate_connections(&self);
    fn populate_connections(&self, repeat: bool);
    fn add_pull(&self, pull: PullInfo);
    fn connection(&self, use_front_connection: bool) -> (Option<Arc<BootstrapClient>>, bool);
    fn find_connection(&self, endpoint: SocketAddrV6) -> Option<Arc<BootstrapClient>>;
    async fn add_connection(&self, endpoint: SocketAddrV6) -> bool;
    async fn connect_client(&self, endpoint: SocketAddrV6, push_front: bool) -> bool;
    fn request_pull<'a>(
        &'a self,
        guard: MutexGuard<'a, BootstrapConnectionsData>,
    ) -> MutexGuard<'a, BootstrapConnectionsData>;
}

#[async_trait]
impl BootstrapConnectionsExt for Arc<BootstrapConnections> {
    fn pool_connection(&self, client_a: Arc<BootstrapClient>, new_client: bool, push_front: bool) {
        let excluded = self
            .network_info
            .write()
            .unwrap()
            .is_excluded(&client_a.remote_addr(), self.clock.now());

        let mut guard = self.mutex.lock().unwrap();

        if !self.stopped.load(Ordering::SeqCst) && !client_a.pending_stop() && !excluded {
            client_a.set_timeout(self.config.idle_timeout);
            // Push into idle deque
            if !push_front {
                guard.idle.push_back(Arc::clone(&client_a));
            } else {
                guard.idle.push_front(Arc::clone(&client_a));
            }
            if new_client {
                guard.clients.push_back(Arc::downgrade(&client_a));
            }
        } else {
            client_a.close();
        }
        drop(guard);
        self.condition.notify_all();
    }

    fn requeue_pull(&self, pull_a: PullInfo, network_error: bool) {
        let mut pull = pull_a;
        if !network_error {
            pull.attempts += 1;
        }
        let attempt_l = self
            .attempts
            .lock()
            .unwrap()
            .find(pull.bootstrap_id as usize)
            .cloned();

        if let Some(attempt_l) = attempt_l {
            attempt_l.inc_requeued_pulls();
            let mut is_lazy = false;
            if let BootstrapStrategy::Lazy(lazy) = &*attempt_l {
                is_lazy = true;
                pull.count = lazy.lazy_batch_size();
            }
            if attempt_l.mode() == BootstrapMode::Legacy
                && (pull.attempts
                    < pull.retry_limit
                        + (pull.processed
                            / bootstrap_limits::REQUEUED_PULLS_PROCESSED_BLOCKS_FACTOR as u64)
                            as u32)
            {
                {
                    let mut guard = self.mutex.lock().unwrap();
                    guard.pulls.push_front(pull);
                }
                attempt_l.pull_started();
                self.condition.notify_all();
            } else if is_lazy
                && (pull.attempts
                    <= pull.retry_limit
                        + (pull.processed as u32 / self.config.lazy_max_pull_blocks))
            {
                debug_assert_eq!(BlockHash::from(pull.account_or_head), pull.head);

                let BootstrapStrategy::Lazy(lazy) = &*attempt_l else {
                    unreachable!()
                };
                if !lazy.lazy_processed_or_exists(&pull.account_or_head.into()) {
                    {
                        let mut guard = self.mutex.lock().unwrap();
                        guard.pulls.push_back(pull);
                    }
                    attempt_l.pull_started();
                    self.condition.notify_all();
                }
            } else {
                self.stats.inc_dir(
                    StatType::Bootstrap,
                    DetailType::BulkPullFailedAccount,
                    Direction::In,
                );
                debug!("Failed to pull account {} or head block {} down to {} after {} attempts and {} blocks processed",
                		Account::from(pull.account_or_head).encode_account(),
                		pull.account_or_head,
                		pull.end,
                		pull.attempts,
                		pull.processed);

                if is_lazy && pull.processed > 0 {
                    let BootstrapStrategy::Lazy(lazy) = &*attempt_l else {
                        unreachable!()
                    };
                    lazy.lazy_add(&pull);
                } else if attempt_l.mode() == BootstrapMode::Legacy {
                    self.pulls_cache.lock().unwrap().add(&pull);
                }
            }
        }
    }

    fn add_pull(&self, mut pull: PullInfo) {
        self.pulls_cache.lock().unwrap().update_pull(&mut pull);
        {
            let mut guard = self.mutex.lock().unwrap();
            guard.pulls.push_back(pull);
        }
        self.condition.notify_all();
    }

    fn connection(&self, use_front_connection: bool) -> (Option<Arc<BootstrapClient>>, bool) {
        let mut guard = self.mutex.lock().unwrap();
        guard = self
            .condition
            .wait_while(guard, |g| {
                !self.stopped.load(Ordering::SeqCst)
                    && g.idle.is_empty()
                    && !self.new_connections_empty.load(Ordering::SeqCst)
            })
            .unwrap();
        let mut result: Option<Arc<BootstrapClient>> = None;
        if !self.stopped.load(Ordering::SeqCst) && !guard.idle.is_empty() {
            if !use_front_connection {
                result = guard.idle.pop_back();
            } else {
                result = guard.idle.pop_front();
            }
        }
        if result.is_none()
            && self.connections_count.load(Ordering::SeqCst) == 0
            && self.new_connections_empty.load(Ordering::SeqCst)
        {
            (result, true) // should stop attempt
        } else {
            (result, false) // should not stop attempt
        }
    }

    fn run(&self) {
        self.start_populate_connections();
        let mut guard = self.mutex.lock().unwrap();
        while !self.stopped.load(Ordering::SeqCst) {
            if !guard.pulls.is_empty() {
                guard = self.request_pull(guard);
            } else {
                guard = self.condition.wait(guard).unwrap();
            }
        }
        self.stopped.store(true, Ordering::SeqCst);
        drop(guard);
        self.condition.notify_all();
    }

    fn start_populate_connections(&self) {
        if !self.populate_connections_started.load(Ordering::SeqCst) {
            self.populate_connections(true);
        }
    }

    fn find_connection(&self, endpoint: SocketAddrV6) -> Option<Arc<BootstrapClient>> {
        let mut guard = self.mutex.lock().unwrap();
        let mut result = None;
        for (i, client) in guard.idle.iter().enumerate() {
            if self.stopped.load(Ordering::SeqCst) {
                break;
            }
            if client.remote_addr() == endpoint {
                result = Some(Arc::clone(client));
                guard.idle.remove(i);
                break;
            }
        }
        result
    }

    fn populate_connections(&self, repeat: bool) {
        let mut rate_sum = 0f64;
        let num_pulls;
        let attempts_count = self.attempts.lock().unwrap().size();
        let mut sorted_connections: BinaryHeap<OrderedByBlockRateDesc> = BinaryHeap::new();
        let mut endpoints = HashSet::new();
        {
            let mut guard = self.mutex.lock().unwrap();
            num_pulls = guard.pulls.len();
            let mut new_clients = VecDeque::new();
            for c in &guard.clients {
                if let Some(client) = c.upgrade() {
                    new_clients.push_back(Arc::downgrade(&client));
                    endpoints.insert(client.remote_addr());
                    let elapsed = client.elapsed();
                    let blocks_per_sec = client.sample_block_rate();
                    rate_sum += blocks_per_sec;
                    if client.elapsed().as_secs_f64()
                        > bootstrap_limits::BOOTSTRAP_CONNECTION_WARMUP_TIME_SEC
                        && client.block_count() > 0
                    {
                        sorted_connections.push(OrderedByBlockRateDesc(Arc::clone(&client)));
                    }
                    // Force-stop the slowest peers, since they can take the whole bootstrap hostage by dribbling out blocks on the last remaining pull.
                    // This is ~1.5kilobits/sec.
                    if elapsed.as_secs_f64()
                        > bootstrap_limits::BOOTSTRAP_MINIMUM_TERMINATION_TIME_SEC
                        && blocks_per_sec < bootstrap_limits::BOOTSTRAP_MINIMUM_BLOCKS_PER_SEC
                    {
                        debug!("Stopping slow peer {} (elapsed sec {} > {} and {} blocks per second < {})",
                        				client.channel_string(),
                        				elapsed.as_secs_f64(),
                        				bootstrap_limits::BOOTSTRAP_MINIMUM_TERMINATION_TIME_SEC,
                        				blocks_per_sec,
                        				bootstrap_limits::BOOTSTRAP_MINIMUM_BLOCKS_PER_SEC);

                        client.stop(true);
                        new_clients.pop_back();
                    }
                }
            }
            // Cleanup expired clients
            std::mem::swap(&mut guard.clients, &mut new_clients);
        }

        let target = self.target_connections(num_pulls, attempts_count);

        // We only want to drop slow peers when more than 2/3 are active. 2/3 because 1/2 is too aggressive, and 100% rarely happens.
        // Probably needs more tuning.
        if sorted_connections.len() >= (target as usize * 2) / 3 && target >= 4 {
            // 4 -> 1, 8 -> 2, 16 -> 4, arbitrary, but seems to work well.
            let drop = (target as f32 - 2.0).sqrt().round() as i32;

            debug!(
                "Dropping {} bulk pull peers, target connections {}",
                drop, target
            );

            for _ in 0..drop {
                if let Some(client) = sorted_connections.pop() {
                    debug!(
                        "Dropping peer with block rate {} and block count {} ({})",
                        client.block_rate(),
                        client.block_count(),
                        client.channel_string()
                    );

                    client.stop(false);
                }
            }
        }

        debug!("Bulk pull connections: {}, rate: {} blocks/sec, bootstrap attempts {}, remaining pulls: {}",
            self.connections_count.load(Ordering::SeqCst),
            rate_sum as f32,
            attempts_count,
            num_pulls);

        if self.connections_count.load(Ordering::SeqCst) < target
            && (attempts_count != 0 || self.new_connections_empty.load(Ordering::SeqCst))
            && !self.stopped.load(Ordering::SeqCst)
        {
            let delta = min(
                (target - self.connections_count.load(Ordering::SeqCst)) * 2,
                bootstrap_limits::BOOTSTRAP_MAX_NEW_CONNECTIONS,
            );
            // TODO - tune this better
            // Not many peers respond, need to try to make more connections than we need.
            for _ in 0..delta {
                let (endpoint, excluded) = {
                    let mut network = self.network_info.write().unwrap();
                    let endpoint = network.bootstrap_peer(self.clock.now()); // Legacy bootstrap is compatible with older version of protocol
                    let excluded = network.is_excluded(&endpoint, self.clock.now());
                    (endpoint, excluded)
                };
                if endpoint != SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0)
                    && (self.config.allow_bootstrap_peers_duplicates
                        || !endpoints.contains(&endpoint))
                    && !excluded
                {
                    let success = self.tokio.block_on(self.connect_client(endpoint, false));
                    if success {
                        endpoints.insert(endpoint);
                        let _guard = self.mutex.lock().unwrap();
                        self.new_connections_empty.store(false, Ordering::SeqCst);
                    }
                } else if self.connections_count.load(Ordering::SeqCst) == 0 {
                    {
                        let _guard = self.mutex.lock().unwrap();
                        self.new_connections_empty.store(true, Ordering::SeqCst);
                    }
                    self.condition.notify_all();
                }
            }
        }
        if !self.stopped.load(Ordering::SeqCst) && repeat {
            let self_w = Arc::downgrade(self);
            self.workers.add_delayed_task(
                Duration::from_secs(1),
                Box::new(move || {
                    if let Some(self_l) = self_w.upgrade() {
                        self_l.populate_connections(true);
                    }
                }),
            );
        }
    }

    async fn add_connection(&self, endpoint: SocketAddrV6) -> bool {
        self.connect_client(endpoint, true).await
    }

    async fn connect_client(&self, peer_addr: SocketAddrV6, push_front: bool) -> bool {
        {
            let mut network_info = self.network_info.write().unwrap();
            if let Err(e) = network_info.add_outbound_attempt(
                peer_addr,
                ChannelMode::Bootstrap,
                self.clock.now(),
            ) {
                drop(network_info);
                self.network_stats
                    .error(e, &peer_addr, ChannelDirection::Outbound);
                return false;
            }

            if let Err(e) = network_info.validate_new_connection(
                &peer_addr,
                ChannelDirection::Outbound,
                ChannelMode::Bootstrap,
                self.clock.now(),
            ) {
                network_info.remove_attempt(&peer_addr);
                drop(network_info);
                debug!(
                    "Could not create outbound bootstrap connection to {}, because of failed limit check",
                    peer_addr);
                self.network_stats
                    .error(e, &peer_addr, ChannelDirection::Outbound);
                return false;
            }
        }

        self.network_stats.connection_attempt(&peer_addr);

        let tcp_stream_factory = Arc::new(TcpStreamFactory::new());
        let tcp_stream = match tokio::time::timeout(
            self.config.tcp_io_timeout,
            tcp_stream_factory.connect(peer_addr),
        )
        .await
        {
            Ok(Ok(stream)) => stream,
            Ok(Err(e)) => {
                debug!(
                    "Error initiating bootstrap connection to: {} ({:?})",
                    peer_addr, e
                );
                self.connections_count.fetch_sub(1, Ordering::SeqCst);
                self.network_info
                    .write()
                    .unwrap()
                    .remove_attempt(&peer_addr);
                return false;
            }
            Err(_) => {
                debug!("Timeout connecting to: {}", peer_addr);
                self.connections_count.fetch_sub(1, Ordering::SeqCst);
                self.network_info
                    .write()
                    .unwrap()
                    .remove_attempt(&peer_addr);
                return false;
            }
        };

        let Ok(channel) = self.network.add(
            tcp_stream,
            ChannelDirection::Outbound,
            ChannelMode::Bootstrap,
        ) else {
            debug!(remote_addr = ?peer_addr, "Bootstrap connection rejected");
            self.network_info
                .write()
                .unwrap()
                .remove_attempt(&peer_addr);
            return false;
        };
        debug!("Bootstrap connection established to: {}", peer_addr);

        channel.info.set_mode(ChannelMode::Bootstrap);

        let client = Arc::new(BootstrapClient::new(
            &self,
            channel,
            self.message_publisher.clone(),
        ));
        self.connections_count.fetch_add(1, Ordering::SeqCst);
        self.network_info
            .write()
            .unwrap()
            .remove_attempt(&peer_addr);
        self.pool_connection(client, true, push_front);

        true
    }

    fn request_pull<'a>(
        &'a self,
        mut guard: MutexGuard<'a, BootstrapConnectionsData>,
    ) -> MutexGuard<'a, BootstrapConnectionsData> {
        drop(guard);
        let (connection_l, _should_stop) = self.connection(false);
        guard = self.mutex.lock().unwrap();
        if let Some(connection_l) = connection_l {
            if !guard.pulls.is_empty() {
                let mut attempt_l = None;
                let mut pull = PullInfo::default();
                // Search pulls with existing attempts
                while attempt_l.is_none() && !guard.pulls.is_empty() {
                    pull = guard.pulls.pop_front().unwrap();
                    attempt_l = self
                        .attempts
                        .lock()
                        .unwrap()
                        .find(pull.bootstrap_id as usize)
                        .cloned();
                    // Check if lazy pull is obsolete (head was processed or head is 0 for destinations requests)
                    if let Some(attempt) = &attempt_l {
                        if let BootstrapStrategy::Lazy(lazy) = &**attempt {
                            if !pull.head.is_zero() && lazy.lazy_processed_or_exists(&pull.head) {
                                attempt.pull_finished();
                                attempt_l = None;
                            }
                        }
                    }
                }

                if let Some(attempt_l) = attempt_l {
                    // The bulk_pull_client destructor attempt to requeue_pull which can cause a deadlock if this is the last reference
                    // Dispatch request in an external thread in case it needs to be destroyed
                    let self_l = Arc::clone(self);
                    let initiator = self_l
                        .bootstrap_initiator
                        .lock()
                        .unwrap()
                        .as_ref()
                        .cloned()
                        .expect("bootstrap initiator not set")
                        .upgrade();

                    let client_config = BulkPullClientConfig {
                        disable_legacy_bootstrap: self.config.disable_legacy_bootstrap,
                        retry_limit: self.config.lazy_retry_limit,
                        work_thresholds: self.config.work_thresholds.clone(),
                    };

                    if let Some(initiator) = initiator {
                        self.workers.push_task(Box::new(move || {
                            let client = Arc::new(BulkPullClient::new(
                                client_config,
                                Arc::clone(&self_l.stats),
                                Arc::clone(&self_l.block_processor),
                                connection_l,
                                attempt_l,
                                Arc::clone(&self_l.workers),
                                self_l.tokio.clone(),
                                self_l,
                                initiator,
                                pull,
                            ));
                            client.request();
                        }));
                    }
                }
            } else {
                // Reuse connection if pulls deque become empty
                drop(guard);
                self.pool_connection(connection_l, false, false);
                guard = self.mutex.lock().unwrap();
            }
        }

        guard
    }
}

#[derive(Default)]
pub struct BootstrapConnectionsData {
    pulls: VecDeque<PullInfo>,
    clients: VecDeque<Weak<BootstrapClient>>,
    idle: VecDeque<Arc<BootstrapClient>>,
}

struct OrderedByBlockRateDesc(Arc<BootstrapClient>);

impl Deref for OrderedByBlockRateDesc {
    type Target = Arc<BootstrapClient>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Ord for OrderedByBlockRateDesc {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        OrderedFloat(other.0.block_rate()).cmp(&OrderedFloat(self.0.block_rate()))
    }
}

impl PartialOrd for OrderedByBlockRateDesc {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(&other))
    }
}

impl PartialEq for OrderedByBlockRateDesc {
    fn eq(&self, other: &Self) -> bool {
        self.0.block_rate() == other.0.block_rate()
    }
}

impl Eq for OrderedByBlockRateDesc {}
