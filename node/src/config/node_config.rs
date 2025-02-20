use super::{
    websocket_config::WebsocketConfig, DiagnosticsConfig, NetworkParams, Networks,
    DEV_NETWORK_PARAMS,
};
use crate::{
    block_processing::{
        BacklogScanConfig, BlockProcessorConfig, BoundedBacklogConfig, LocalBlockBroadcasterConfig,
    },
    bootstrap::{BootstrapConfig, BootstrapResponderConfig},
    cementation::ConfirmingSetConfig,
    consensus::{
        ActiveElectionsConfig, HintedSchedulerConfig, OptimisticSchedulerConfig,
        PriorityBucketConfig, RequestAggregatorConfig, VoteCacheConfig, VoteProcessorConfig,
    },
    stats::StatsConfig,
    transport::MessageProcessorConfig,
};
use once_cell::sync::Lazy;
use rand::Rng;
use rsnano_core::{
    utils::{get_env_or_default_string, Peer},
    Account, Amount, PublicKey,
};
use rsnano_network::NetworkConfig;
use rsnano_nullable_http_client::Url;
use rsnano_store_lmdb::LmdbConfig;
use std::{cmp::max, net::Ipv6Addr, time::Duration};

#[derive(Clone, Debug, PartialEq)]
pub struct NodeConfig {
    pub default_peering_port: u16,
    pub optimistic_scheduler: OptimisticSchedulerConfig,
    pub hinted_scheduler: HintedSchedulerConfig,
    pub priority_bucket: PriorityBucketConfig,
    pub bootstrap_fraction_numerator: u32,
    pub receive_minimum: Amount,
    pub online_weight_minimum: Amount,
    /// The minimum vote weight that a representative must have for its vote to be counted.
    /// All representatives above this weight will be kept in memory!
    pub representative_vote_weight_minimum: Amount,
    pub password_fanout: u32,
    pub io_threads: usize,
    pub network_threads: u32,
    pub work_threads: u32,
    pub background_threads: u32,
    pub signature_checker_threads: u32,
    pub enable_voting: bool,
    pub enable_vote_processor: bool,
    pub enable_priority_scheduler: bool,
    pub enable_optimistic_scheduler: bool,
    pub enable_hinted_scheduler: bool,
    pub enable_monitor: bool,
    pub enable_bounded_backlog: bool,
    pub bootstrap_initiator_threads: u32,
    pub bootstrap_serving_threads: u32,
    pub block_processor_batch_max_time_ms: i64,
    pub allow_local_peers: bool,
    pub vote_minimum: Amount,
    pub vote_generator_delay: Duration,
    pub unchecked_cutoff_time_s: i64,
    pub pow_sleep_interval_ns: i64,
    pub external_address: String,
    pub external_port: u16,
    pub use_memory_pools: bool,
    pub bandwidth_limit: usize,
    pub bandwidth_limit_burst_ratio: f64,
    pub bootstrap: BootstrapConfig,
    pub bootstrap_responder: BootstrapResponderConfig,
    pub bootstrap_bandwidth_limit: usize,
    pub bootstrap_bandwidth_burst_ratio: f64,
    pub confirming_set_batch_time: Duration,
    pub backup_before_upgrade: bool,
    pub max_work_generate_multiplier: f64,
    pub max_queued_requests: u32,
    pub request_aggregator_threads: u32,
    pub max_unchecked_blocks: u32,
    pub rep_crawler_weight_minimum: Amount,
    pub work_peers: Vec<Peer>,
    pub secondary_work_peers: Vec<Peer>,
    pub preconfigured_peers: Vec<Peer>,
    pub preconfigured_representatives: Vec<PublicKey>,
    pub max_pruning_age_s: i64,
    pub max_pruning_depth: u64,
    pub callback_address: String,
    pub callback_port: u16,
    pub callback_target: String,
    pub websocket_config: WebsocketConfig,
    pub diagnostics_config: DiagnosticsConfig,
    pub stat_config: StatsConfig,
    pub lmdb_config: LmdbConfig,
    pub vote_cache: VoteCacheConfig,
    pub rep_crawler_query_timeout: Duration,
    pub block_processor: BlockProcessorConfig,
    pub active_elections: ActiveElectionsConfig,
    pub vote_processor: VoteProcessorConfig,
    pub tcp: TcpConfig,
    pub request_aggregator: RequestAggregatorConfig,
    pub message_processor: MessageProcessorConfig,
    pub local_block_broadcaster: LocalBlockBroadcasterConfig,
    pub confirming_set: ConfirmingSetConfig,
    pub monitor: MonitorConfig,
    pub backlog_scan: BacklogScanConfig,
    pub bounded_backlog: BoundedBacklogConfig,
    pub network_duplicate_filter_size: usize,
    pub network_duplicate_filter_cutoff: u64,
    pub max_ledger_notifications: usize,
    pub network: NetworkConfig,
}

static DEFAULT_LIVE_PEER_NETWORK: Lazy<String> =
    Lazy::new(|| get_env_or_default_string("NANO_DEFAULT_PEER", "peering.nano.org"));

static DEFAULT_BETA_PEER_NETWORK: Lazy<String> =
    Lazy::new(|| get_env_or_default_string("NANO_DEFAULT_PEER", "peering-beta.nano.org"));

static DEFAULT_TEST_PEER_NETWORK: Lazy<String> =
    Lazy::new(|| get_env_or_default_string("NANO_DEFAULT_PEER", "peering-test.nano.org"));

impl NodeConfig {
    pub fn default_for(network: Networks, parallelism: usize) -> Self {
        let net_params = NetworkParams::new(network);
        Self::new(
            Some(net_params.network.default_node_port),
            &net_params,
            parallelism,
        )
    }

    pub fn new(
        peering_port: Option<u16>,
        network_params: &NetworkParams,
        parallelism: usize,
    ) -> Self {
        if peering_port == Some(0) {
            // comment for posterity:
            // - we used to consider ports being 0 a sentinel that meant to use a default port for that specific purpose
            // - the actual default value was determined based on the active network (e.g. dev network peering port = 44000)
            // - now, the 0 value means something different instead: user wants to let the OS pick a random port
            // - for the specific case of the peering port, after it gets picked, it can be retrieved by client code via
            //   node.network.endpoint ().port ()
            // - the config value does not get back-propagated because it represents the choice of the user, and that was 0
        }

        let mut enable_voting = false;
        let mut preconfigured_peers = Vec::new();
        let mut preconfigured_representatives = Vec::new();
        let default_port = network_params.network.default_node_port;
        let network = network_params.network.current_network;
        match network {
            Networks::NanoDevNetwork => {
                enable_voting = true;
                preconfigured_representatives.push(network_params.ledger.genesis_account.into());
            }
            Networks::NanoBetaNetwork => {
                preconfigured_peers
                    .push(Peer::new(DEFAULT_BETA_PEER_NETWORK.clone(), default_port));
                preconfigured_representatives.push(
                    Account::decode_account(
                        "nano_1defau1t9off1ine9rep99999999999999999999999999999999wgmuzxxy",
                    )
                    .unwrap()
                    .into(),
                );
            }
            Networks::NanoLiveNetwork => {
                preconfigured_peers
                    .push(Peer::new(DEFAULT_LIVE_PEER_NETWORK.clone(), default_port));
                preconfigured_representatives.push(
                    PublicKey::decode_hex(
                        "A30E0A32ED41C8607AA9212843392E853FCBCB4E7CB194E35C94F07F91DE59EF",
                    )
                    .unwrap(),
                );
                preconfigured_representatives.push(
                    PublicKey::decode_hex(
                        "67556D31DDFC2A440BF6147501449B4CB9572278D034EE686A6BEE29851681DF",
                    )
                    .unwrap(),
                );
                preconfigured_representatives.push(
                    PublicKey::decode_hex(
                        "5C2FBB148E006A8E8BA7A75DD86C9FE00C83F5FFDBFD76EAA09531071436B6AF",
                    )
                    .unwrap(),
                );
                preconfigured_representatives.push(
                    PublicKey::decode_hex(
                        "AE7AC63990DAAAF2A69BF11C913B928844BF5012355456F2F164166464024B29",
                    )
                    .unwrap(),
                );
                preconfigured_representatives.push(
                    PublicKey::decode_hex(
                        "BD6267D6ECD8038327D2BCC0850BDF8F56EC0414912207E81BCF90DFAC8A4AAA",
                    )
                    .unwrap(),
                );
                preconfigured_representatives.push(
                    PublicKey::decode_hex(
                        "2399A083C600AA0572F5E36247D978FCFC840405F8D4B6D33161C0066A55F431",
                    )
                    .unwrap(),
                );
                preconfigured_representatives.push(
                    PublicKey::decode_hex(
                        "2298FAB7C61058E77EA554CB93EDEEDA0692CBFCC540AB213B2836B29029E23A",
                    )
                    .unwrap(),
                );
                preconfigured_representatives.push(
                    PublicKey::decode_hex(
                        "3FE80B4BC842E82C1C18ABFEEC47EA989E63953BC82AC411F304D13833D52A56",
                    )
                    .unwrap(),
                );
            }
            Networks::NanoTestNetwork => {
                preconfigured_peers
                    .push(Peer::new(DEFAULT_TEST_PEER_NETWORK.clone(), default_port));
                preconfigured_representatives.push(network_params.ledger.genesis_account.into());
            }
            Networks::Invalid => panic!("invalid network"),
        }

        let block_processor_cfg = BlockProcessorConfig::new(network_params.work.clone());

        Self {
            default_peering_port: network_params.network.default_node_port,
            bootstrap_fraction_numerator: 1,
            receive_minimum: Amount::micronano(1),
            online_weight_minimum: Amount::nano(60_000_000),
            representative_vote_weight_minimum: Amount::nano(10),
            password_fanout: 1024,
            io_threads: max(parallelism, 4),
            network_threads: max(parallelism, 4) as u32,
            work_threads: max(parallelism, 4) as u32,
            background_threads: max(parallelism, 4) as u32,
            /* Use half available threads on the system for signature checking. The calling thread does checks as well, so these are extra worker threads */
            signature_checker_threads: (parallelism / 2) as u32,
            enable_voting,
            enable_vote_processor: true,
            enable_priority_scheduler: true,
            enable_optimistic_scheduler: true,
            enable_hinted_scheduler: true,
            enable_monitor: true,
            enable_bounded_backlog: true,
            bootstrap_initiator_threads: 1,
            bootstrap_serving_threads: 1,
            block_processor_batch_max_time_ms: block_processor_cfg.batch_max_time.as_millis()
                as i64,
            allow_local_peers: !(network_params.network.is_live_network()
                || network_params.network.is_test_network()), // disable by default for live network
            vote_minimum: Amount::nano(1000),
            vote_generator_delay: Duration::from_millis(100),
            unchecked_cutoff_time_s: 4 * 60 * 60, // 4 hours
            pow_sleep_interval_ns: 0,
            external_address: Ipv6Addr::UNSPECIFIED.to_string(),
            external_port: 0,
            use_memory_pools: true,
            // Default outbound traffic shaping is 10MB/s
            bandwidth_limit: 10 * 1024 * 1024,
            // By default, allow bursts of 15MB/s (not sustainable)
            bandwidth_limit_burst_ratio: 3_f64,
            // Default bootstrap outbound traffic limit is 5MB/s
            bootstrap_bandwidth_limit: 5 * 1024 * 1024,
            // Bootstrap traffic does not need bursts
            bootstrap_bandwidth_burst_ratio: 1.,
            bootstrap: Default::default(),
            bootstrap_responder: Default::default(),
            confirming_set_batch_time: Duration::from_millis(250),
            backup_before_upgrade: false,
            max_work_generate_multiplier: 64_f64,
            max_queued_requests: 512,
            request_aggregator_threads: max(parallelism, 4) as u32,
            max_unchecked_blocks: 65536,
            rep_crawler_weight_minimum: Amount::decode_hex("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF")
                .unwrap(),
            work_peers: Vec::new(),
            secondary_work_peers: vec![Peer::new("127.0.0.1", 8076)],
            preconfigured_peers,
            preconfigured_representatives,
            max_pruning_age_s: if !network_params.network.is_beta_network() {
                24 * 60 * 60
            } else {
                5 * 60
            }, // 1 day; 5 minutes for beta network
            max_pruning_depth: 0,
            callback_address: String::new(),
            callback_port: 0,
            callback_target: String::new(),
            websocket_config: WebsocketConfig::new(&network_params.network),
            diagnostics_config: DiagnosticsConfig::new(),
            stat_config: StatsConfig::new(),
            lmdb_config: LmdbConfig::new(),
            optimistic_scheduler: OptimisticSchedulerConfig::new(),
            hinted_scheduler: if network_params.network.is_dev_network() {
                HintedSchedulerConfig::default_for_dev_network()
            } else {
                HintedSchedulerConfig::default()
            },
            priority_bucket: Default::default(),
            vote_cache: Default::default(),
            active_elections: Default::default(),
            rep_crawler_query_timeout: if network_params.network.is_dev_network() {
                Duration::from_secs(1)
            } else {
                Duration::from_secs(60)
            },
            block_processor: block_processor_cfg,
            vote_processor: VoteProcessorConfig::new(parallelism),
            tcp: if network_params.network.is_dev_network() {
                TcpConfig::for_dev_network()
            } else {
                Default::default()
            },
            request_aggregator: RequestAggregatorConfig::new(parallelism),
            message_processor: MessageProcessorConfig::new(parallelism),
            local_block_broadcaster: LocalBlockBroadcasterConfig::new(network),
            confirming_set: Default::default(),
            monitor: Default::default(),
            backlog_scan: Default::default(),
            bounded_backlog: Default::default(),
            network_duplicate_filter_size: 1024 * 1024,
            network_duplicate_filter_cutoff: 60,
            max_ledger_notifications: 8,
            network: NetworkConfig {
                listening_port: peering_port.unwrap_or_default(),
                ..NetworkConfig::default_for(network)
            },
        }
    }

    pub fn new_test_instance() -> Self {
        Self::new(None, &DEV_NETWORK_PARAMS, 1)
    }

    pub fn random_representative(&self) -> PublicKey {
        let i = rand::rng().random_range(0..self.preconfigured_representatives.len());
        return self.preconfigured_representatives[i];
    }

    pub fn rpc_callback_url(&self) -> Option<Url> {
        format!(
            "http://{}:{}{}",
            self.callback_address, self.callback_port, self.callback_target
        )
        .parse()
        .ok()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct MonitorConfig {
    pub interval: Duration,
}

impl Default for MonitorConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct TcpConfig {
    pub max_inbound_connections: usize,
    pub max_outbound_connections: usize,
    pub max_attempts: usize,
    pub max_attempts_per_ip: usize,
    pub connect_timeout: Duration,
}

impl TcpConfig {
    pub fn for_dev_network() -> Self {
        Self {
            max_inbound_connections: 128,
            max_outbound_connections: 128,
            max_attempts: 128,
            max_attempts_per_ip: 128,
            connect_timeout: Duration::from_secs(5),
        }
    }
}

impl Default for TcpConfig {
    fn default() -> Self {
        Self {
            max_inbound_connections: 2048,
            max_outbound_connections: 2048,
            max_attempts: 60,
            max_attempts_per_ip: 1,
            connect_timeout: Duration::from_secs(60),
        }
    }
}
