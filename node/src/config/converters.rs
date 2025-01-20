use super::GlobalConfig;
use crate::block_processing::{BacklogScanConfig, BlockProcessorConfig};
use rsnano_network::{bandwidth_limiter::BandwidthLimiterConfig, NetworkConfig};
use std::time::Duration;

impl From<&GlobalConfig> for BlockProcessorConfig {
    fn from(value: &GlobalConfig) -> Self {
        let config = &value.node_config.block_processor;
        Self {
            max_peer_queue: config.max_peer_queue,
            priority_local: config.priority_local,
            priority_bootstrap: config.priority_bootstrap,
            priority_live: config.priority_live,
            priority_system: config.priority_system,
            max_system_queue: config.max_system_queue,
            batch_max_time: Duration::from_millis(
                value.node_config.block_processor_batch_max_time_ms as u64,
            ),
            full_size: value.flags.block_processor_full_size,
            batch_size: 256,
            work_thresholds: value.network_params.work.clone(),
        }
    }
}

impl From<&GlobalConfig> for BacklogScanConfig {
    fn from(value: &GlobalConfig) -> Self {
        value.node_config.backlog_scan.clone()
    }
}

impl From<&GlobalConfig> for NetworkConfig {
    fn from(value: &GlobalConfig) -> Self {
        Self {
            max_inbound_connections: value.node_config.tcp.max_inbound_connections,
            max_outbound_connections: value.node_config.tcp.max_outbound_connections,
            max_peers_per_ip: value.node_config.max_peers_per_ip,
            max_peers_per_subnetwork: value.node_config.max_peers_per_subnetwork,
            max_attempts_per_ip: value.node_config.tcp.max_attempts_per_ip,
            allow_local_peers: value.node_config.allow_local_peers,
            disable_max_peers_per_ip: value.flags.disable_max_peers_per_ip,
            disable_max_peers_per_subnetwork: value.flags.disable_max_peers_per_subnetwork,
            disable_network: value.flags.disable_tcp_realtime,
            min_protocol_version: value.network_params.network.protocol_info().version_min,
            listening_port: value.node_config.peering_port.unwrap_or(0),
            limiter: value.into(),
        }
    }
}

impl From<&GlobalConfig> for BandwidthLimiterConfig {
    fn from(value: &GlobalConfig) -> Self {
        Self {
            generic_limit: value.node_config.bandwidth_limit,
            generic_burst_ratio: value.node_config.bandwidth_limit_burst_ratio,
            bootstrap_limit: value.node_config.bootstrap_bandwidth_limit,
            bootstrap_burst_ratio: value.node_config.bootstrap_bandwidth_burst_ratio,
        }
    }
}
