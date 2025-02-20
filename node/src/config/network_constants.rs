use crate::bootstrap::BootstrapConfig;
use anyhow::Result;
use rsnano_core::{utils::get_env_or_default, work::WorkThresholds, Networks, ACTIVE_NETWORK};
use rsnano_messages::ProtocolInfo;
use std::time::Duration;

#[derive(Clone, Debug, PartialEq)]
pub struct NetworkConstants {
    pub work: WorkThresholds,
    pub default_node_port: u16,
    pub default_rpc_port: u16,
    pub default_ipc_port: u16,
    pub default_websocket_port: u16,
    pub aec_loop_interval: Duration,
    pub cleanup_period: Duration,
    /** How often to send keepalive messages */
    pub keepalive_period: Duration,
    /** Default maximum idle time for a socket before it's automatically closed */
    pub idle_timeout: Duration,
    pub sync_cookie_cutoff: Duration,
    pub bootstrap_interval_s: i64,
    /** Maximum number of peers per IP. It is also the max number of connections per IP*/
    pub max_peers_per_ip: usize,
    /** Maximum number of peers per subnetwork */
    pub max_peers_per_subnetwork: usize,
    pub peer_dump_interval: Duration,

    pub current_network: Networks,
    /** Current protocol version */
    pub protocol_version: u8,
    /** Minimum accepted protocol version */
    pub protocol_version_min: u8,
    /** Minimum accepted protocol version used when bootstrapping */
    pub bootstrap_protocol_version_min: u8,
    pub ipv6_subnetwork_prefix_for_limiting: usize,
    pub silent_connection_tolerance_time_s: i64,
    /// Time to wait before vote rebroadcasts for active elections (milliseconds)
    pub vote_broadcast_interval: Duration,
    pub block_broadcast_interval: Duration,

    /** We do not reply to telemetry requests made within cooldown period */
    pub telemetry_request_cooldown: Duration,
    /** How often to request telemetry from peers */
    pub telemetry_request_interval_ms: i64,
    /** How often to broadcast telemetry to peers */
    pub telemetry_broadcast_interval_ms: i64,
    /** Telemetry data older than this value is considered stale */
    pub telemetry_cache_cutoff_ms: i64, // 2 * `telemetry_broadcast_interval` + some margin
    /// How much to delay activation of optimistic elections to avoid interfering with election scheduler
    pub optimistic_activation_delay: Duration,
    pub rep_crawler_normal_interval: Duration,
    pub rep_crawler_warmup_interval: Duration,
}

impl NetworkConstants {
    pub fn empty() -> Self {
        Self::new(WorkThresholds::publish_dev().clone(), Networks::Invalid)
    }

    pub fn new(work: WorkThresholds, network: Networks) -> Self {
        match network {
            Networks::NanoDevNetwork => Self::dev(work),
            Networks::NanoBetaNetwork => Self::beta(work),
            Networks::NanoLiveNetwork | Networks::Invalid => Self::live(work),
            Networks::NanoTestNetwork => Self::test(work),
        }
    }

    pub fn for_network(network: Networks) -> Self {
        Self::new(WorkThresholds::default_for(network), network)
    }

    pub fn protocol_info(&self) -> ProtocolInfo {
        ProtocolInfo {
            version_using: self.protocol_version,
            version_max: self.protocol_version,
            version_min: self.protocol_version_min,
            network: self.current_network,
        }
    }

    fn live(work: WorkThresholds) -> Self {
        let cleanup_period = Duration::from_secs(60);
        let protocol_info = ProtocolInfo::default();
        Self {
            work,
            current_network: Networks::NanoLiveNetwork,
            protocol_version: protocol_info.version_using,
            protocol_version_min: protocol_info.version_min,
            bootstrap_protocol_version_min: BootstrapConfig::default().min_protocol_version,
            default_node_port: 7075,
            default_rpc_port: 7076,
            default_ipc_port: 7077,
            default_websocket_port: 7078,
            aec_loop_interval: Duration::from_millis(300),
            cleanup_period,
            keepalive_period: Duration::from_secs(15),
            idle_timeout: cleanup_period * 2,
            sync_cookie_cutoff: Duration::from_secs(5),
            bootstrap_interval_s: 15 * 60,
            max_peers_per_ip: 4,
            max_peers_per_subnetwork: 16,
            peer_dump_interval: Duration::from_secs(5 * 60),
            ipv6_subnetwork_prefix_for_limiting: 64,
            silent_connection_tolerance_time_s: 120,
            vote_broadcast_interval: Duration::from_secs(15),
            block_broadcast_interval: Duration::from_secs(150),
            telemetry_request_cooldown: Duration::from_secs(15),
            telemetry_request_interval_ms: 1000 * 60,
            telemetry_broadcast_interval_ms: 1000 * 60,
            telemetry_cache_cutoff_ms: 1000 * 130, //  2 * `telemetry_broadcast_interval` + some margin
            optimistic_activation_delay: Duration::from_secs(30),
            rep_crawler_normal_interval: Duration::from_secs(7),
            rep_crawler_warmup_interval: Duration::from_secs(3),
        }
    }

    pub fn for_beta() -> Self {
        Self {
            current_network: Networks::NanoBetaNetwork,
            default_node_port: 54000,
            default_rpc_port: 55000,
            default_ipc_port: 56000,
            default_websocket_port: 57000,
            max_peers_per_ip: 256,
            max_peers_per_subnetwork: 256,
            ..Self::live(WorkThresholds::publish_beta().clone())
        }
    }

    fn beta(work: WorkThresholds) -> Self {
        Self {
            current_network: Networks::NanoBetaNetwork,
            default_node_port: 54000,
            default_rpc_port: 55000,
            default_ipc_port: 56000,
            default_websocket_port: 57000,
            max_peers_per_ip: 256,
            max_peers_per_subnetwork: 256,
            ..Self::live(work)
        }
    }

    fn test(work: WorkThresholds) -> Self {
        Self {
            current_network: Networks::NanoTestNetwork,
            default_node_port: test_node_port(),
            default_rpc_port: test_rpc_port(),
            default_ipc_port: test_ipc_port(),
            default_websocket_port: test_websocket_port(),
            ..Self::live(work)
        }
    }

    fn dev(work: WorkThresholds) -> Self {
        let cleanup_period = Duration::from_secs(1);
        Self {
            current_network: Networks::NanoDevNetwork,
            default_node_port: 44000,
            default_rpc_port: 45000,
            default_ipc_port: 46000,
            default_websocket_port: 47000,
            aec_loop_interval: Duration::from_millis(20),
            cleanup_period,
            keepalive_period: Duration::from_secs(1),
            idle_timeout: cleanup_period * 15,
            max_peers_per_ip: 256, // During tests, all peers are on localhost
            max_peers_per_subnetwork: 256,
            peer_dump_interval: Duration::from_secs(1),
            vote_broadcast_interval: Duration::from_millis(500),
            block_broadcast_interval: Duration::from_millis(500),
            telemetry_request_cooldown: Duration::from_millis(500),
            telemetry_cache_cutoff_ms: 2000,
            telemetry_request_interval_ms: 500,
            telemetry_broadcast_interval_ms: 500,
            optimistic_activation_delay: Duration::from_secs(2),
            rep_crawler_normal_interval: Duration::from_millis(500),
            rep_crawler_warmup_interval: Duration::from_millis(500),
            ..Self::live(work)
        }
    }

    pub fn is_live_network(&self) -> bool {
        self.current_network == Networks::NanoLiveNetwork
    }

    pub fn is_beta_network(&self) -> bool {
        self.current_network == Networks::NanoBetaNetwork
    }

    pub fn is_dev_network(&self) -> bool {
        self.current_network == Networks::NanoDevNetwork
    }

    pub fn is_test_network(&self) -> bool {
        self.current_network == Networks::NanoTestNetwork
    }

    /** Initial value is ACTIVE_NETWORK compile flag, but can be overridden by a CLI flag */
    pub fn active_network() -> Networks {
        *ACTIVE_NETWORK.lock().unwrap()
    }

    /**
     * Optionally called on startup to override the global active network.
     * If not called, the compile-time option will be used.
     * @param network The new active network
     */
    pub fn set_active_network(network: Networks) {
        *ACTIVE_NETWORK.lock().unwrap() = network;
    }

    /**
     * Optionally called on startup to override the global active network.
     * If not called, the compile-time option will be used.
     * @param network The new active network. Valid values are "live", "beta" and "dev"
     */
    pub fn set_active_network_from_str(network: impl AsRef<str>) -> Result<()> {
        let net = match network.as_ref() {
            "live" => Networks::NanoLiveNetwork,
            "beta" => Networks::NanoBetaNetwork,
            "dev" => Networks::NanoDevNetwork,
            "test" => Networks::NanoTestNetwork,
            _ => bail!("invalid network"),
        };
        Self::set_active_network(net);
        Ok(())
    }

    pub fn cleanup_cutoff(&self) -> Duration {
        self.cleanup_period * 5
    }

    pub fn get_current_network_as_string(&self) -> &str {
        match self.current_network {
            Networks::NanoDevNetwork => "dev",
            Networks::NanoBetaNetwork => "beta",
            Networks::NanoLiveNetwork => "live",
            Networks::NanoTestNetwork => "test",
            Networks::Invalid => panic!("invalid network"),
        }
    }

    pub fn default_for(network: Networks) -> Self {
        match network {
            Networks::Invalid => Self::empty(),
            Networks::NanoBetaNetwork => Self::new(
                WorkThresholds::publish_beta().clone(),
                Networks::NanoBetaNetwork,
            ),
            Networks::NanoDevNetwork => Self::new(
                WorkThresholds::publish_dev().clone(),
                Networks::NanoDevNetwork,
            ),
            Networks::NanoLiveNetwork => Self::new(
                WorkThresholds::publish_full().clone(),
                Networks::NanoLiveNetwork,
            ),
            Networks::NanoTestNetwork => Self::new(
                WorkThresholds::publish_test().clone(),
                Networks::NanoTestNetwork,
            ),
        }
    }
}

pub fn test_node_port() -> u16 {
    get_env_or_default("NANO_TEST_NODE_PORT", 17075)
}

fn test_rpc_port() -> u16 {
    get_env_or_default("NANO_TEST_RPC_PORT", 17076)
}

fn test_ipc_port() -> u16 {
    get_env_or_default("NANO_TEST_IPC_PORT", 17077)
}

fn test_websocket_port() -> u16 {
    get_env_or_default("NANO_TEST_WEBSOCKET_PORT", 17078)
}
