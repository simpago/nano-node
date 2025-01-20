use super::{NodeRpcToml, NodeToml, OpenclToml};
use crate::config::DaemonConfig;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
pub struct DaemonToml {
    pub node: Option<NodeToml>,
    pub opencl: Option<OpenclToml>,
    pub rpc: Option<NodeRpcToml>,
}

impl DaemonConfig {
    pub fn merge_toml(&mut self, toml: &DaemonToml) {
        if let Some(node_toml) = &toml.node {
            self.node.merge_toml(node_toml);
        }
        if let Some(opencl) = &toml.opencl {
            if let Some(enable) = opencl.enable {
                self.opencl_enable = enable;
            }
            self.opencl.merge_toml(opencl);
        }
        if let Some(rpc) = &toml.rpc {
            if let Some(enable) = rpc.enable {
                self.rpc_enable = enable;
            }
            self.rpc.merge_toml(rpc);
        }
    }
}

impl From<&DaemonConfig> for DaemonToml {
    fn from(config: &DaemonConfig) -> Self {
        Self {
            node: Some((&config.node).into()),
            rpc: Some(config.into()),
            opencl: Some(config.into()),
        }
    }
}

impl From<&DaemonConfig> for NodeRpcToml {
    fn from(config: &DaemonConfig) -> Self {
        Self {
            enable: Some(config.rpc_enable),
            enable_sign_hash: Some(config.rpc.enable_sign_hash),
            child_process: Some((&config.rpc.child_process).into()),
        }
    }
}

impl From<&DaemonConfig> for OpenclToml {
    fn from(config: &DaemonConfig) -> Self {
        Self {
            enable: Some(config.opencl_enable),
            platform: Some(config.opencl.platform),
            device: Some(config.opencl.device),
            threads: Some(config.opencl.threads),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{DaemonConfig, DaemonToml};
    use rsnano_core::Networks;
    use std::path::PathBuf;

    static CUSTOM_TOML_STR: &str = r#"[node]
        allow_local_peers = false
        backup_before_upgrade = true
        bandwidth_limit = 999
        bandwidth_limit_burst_ratio = 999.9
        bootstrap_bandwidth_limit = 999
        bootstrap_bandwidth_burst_ratio = 999.9
        block_processor_batch_max_time = 999
        bootstrap_connections = 999
        bootstrap_connections_max = 999
        bootstrap_initiator_threads = 999
        bootstrap_serving_threads = 999
        bootstrap_frontier_request_count = 9999
        bootstrap_fraction_numerator = 999
        confirming_set_batch_time = 999
        enable_voting = true
        external_address = "0:0:0:0:0:ffff:7f01:101"
        external_port = 999
        io_threads = 999
        max_queued_requests = 999
        network_threads = 999
        background_threads = 999
        online_weight_minimum = "999"
        representative_vote_weight_minimum = "999"
        rep_crawler_weight_minimum = "999"
        password_fanout = 999
        peering_port = 999
        pow_sleep_interval = 999
        preconfigured_peers = ["dev.org"]
        preconfigured_representatives = ["nano_3arg3asgtigae3xckabaaewkx3bzsh7nwz7jkmjos79ihyaxwphhm6qgjps4"]
        receive_minimum = "999"
        signature_checker_threads = 999
        tcp_io_timeout = 999
        unchecked_cutoff_time = 999
        use_memory_pools = false
        vote_generator_delay = 999
        vote_minimum = "999"
        work_peers = ["dev.org:999"]
        work_threads = 999
        max_work_generate_multiplier = 999
        request_aggregator_threads = 999
        max_unchecked_blocks = 999
        max_backlog = 999
        frontiers_confirmation = "always"

        [node.backlog_scan]
        enable = false
        batch_size = 999
        rate_limit = 999

        [node.bounded_backlog]
        enable = false
        batch_size = 999
        max_queued_notifications = 999
        scan_rate = 999

        [node.block_processor]
        max_peer_queue = 999
        max_system_queue = 999
        priority_live = 999
        priority_bootstrap = 999
        priority_local = 999

        [node.active_elections]
        size = 999
        hinted_limit_percentage = 90
        optimistic_limit_percentage = 90
        confirmation_history_size = 999
        confirmation_cache = 999

        [node.diagnostics.txn_tracking]
        enable = true
        ignore_writes_below_block_processor_max_time = false
        min_read_txn_time = 999
        min_write_txn_time = 999

        [node.httpcallback]
        address = "dev.org"
        port = 999
        target = "/dev"

        [node.priority_bucket]
        max_blocks = 999
        max_elections = 999
        reserved_elections = 999

        [node.rep_crawler]
        query_timeout = 999

        [node.monitor]
        enable = false
        interval = 999

        [node.statistics]
        max_samples = 999

        [node.statistics.log]
        filename_counters = "devcounters.stat"
        filename_samples = "devsamples.stat"
        headers = false
        interval_counters = 999
        interval_samples = 999
        rotation_count = 999

        [node.websocket]
        address = "0:0:0:0:0:ffff:7f01:101"
        enable = true
        port = 999

        [node.lmdb]
        sync = "nosync_safe"
        max_databases = 999
        map_size = 999

        [node.optimistic_scheduler]
        enable = false
        gap_threshold = 999
        max_size = 999

        [node.hinted_scheduler]
        enable = false
        hinting_threshold = 99
        check_interval = 999
        block_cooldown = 999
        vacancy_threshold = 99

        [node.experimental]
        secondary_work_peers = ["dev.org:998"]
        max_pruning_age = 999
        max_pruning_depth = 999

        [node.vote_cache]
        age_cutoff = 999
        max_size = 999
        max_voters = 999

        [node.vote_processor]
        max_pr_queue = 999
        max_non_pr_queue = 999
        pr_priority = 999
        threads = 999
        batch_size = 999

        [node.bootstrap]
        enable = false
        enable_database_scan = false
        enable_dependency_walker = false
        block_processor_threshold = 999
        database_rate_limit = 999
        max_pull_count = 999
        channel_limit = 999
        rate_limit = 999
        throttle_coefficient = 999
        throttle_wait = 999
        request_timeout = 999
        max_requests = 999

        [node.bootstrap.account_sets]
        blocking_max = 999
        consideration_count = 999
        cooldown = 999
        priorities_max = 999

        [node.bootstrap_server]
        max_queue = 999
        threads = 999
        batch_size = 999

        [node.request_aggregator]
        max_queue = 999
        threads = 999
        batch_size = 999

        [node.message_processor]
        threads = 999
        max_queue = 999

        [node.tcp]
        max_inbound_connections = 999
	    max_outbound_connections = 999
	    max_attempts = 999
	    max_attempts_per_ip = 999
	    connect_timeout = 999
	    handshake_timeout = 999
	    io_timeout = 999

        [opencl]
        device = 999
        enable = true
        platform = 999
        threads = 999

        [rpc]
        enable = true
        enable_sign_hash = true

        [rpc.child_process]
        enable = true
        rpc_path = "/dev/nano_rpc""#;

    #[test]
    fn deserialize_no_defaults() {
        let daemon_toml: DaemonToml =
            toml::from_str(CUSTOM_TOML_STR).expect("Failed to deserialize TOML");

        let mut deserialized = create_default_daemon_config();
        deserialized.merge_toml(&daemon_toml);

        let default_cfg = create_default_daemon_config();

        // Node section
        assert_ne!(
            deserialized.node.allow_local_peers,
            default_cfg.node.allow_local_peers
        );
        assert_ne!(
            deserialized.node.backup_before_upgrade,
            default_cfg.node.backup_before_upgrade
        );
        assert_ne!(
            deserialized.node.bandwidth_limit,
            default_cfg.node.bandwidth_limit
        );
        assert_ne!(
            deserialized.node.bandwidth_limit_burst_ratio,
            default_cfg.node.bandwidth_limit_burst_ratio
        );
        assert_ne!(
            deserialized.node.bootstrap_bandwidth_limit,
            default_cfg.node.bootstrap_bandwidth_limit
        );
        assert_ne!(
            deserialized.node.bootstrap_bandwidth_burst_ratio,
            default_cfg.node.bootstrap_bandwidth_burst_ratio
        );
        assert_ne!(
            deserialized.node.block_processor_batch_max_time_ms,
            default_cfg.node.block_processor_batch_max_time_ms
        );
        assert_ne!(
            deserialized.node.bootstrap_initiator_threads,
            default_cfg.node.bootstrap_initiator_threads
        );
        assert_ne!(
            deserialized.node.bootstrap_serving_threads,
            default_cfg.node.bootstrap_serving_threads
        );
        assert_ne!(
            deserialized.node.bootstrap_fraction_numerator,
            default_cfg.node.bootstrap_fraction_numerator
        );
        assert_ne!(
            deserialized.node.confirming_set_batch_time,
            default_cfg.node.confirming_set_batch_time
        );
        assert_ne!(
            deserialized.node.enable_voting,
            default_cfg.node.enable_voting
        );
        assert_ne!(
            deserialized.node.external_address,
            default_cfg.node.external_address
        );
        assert_ne!(
            deserialized.node.external_port,
            default_cfg.node.external_port
        );
        assert_ne!(deserialized.node.io_threads, default_cfg.node.io_threads);
        assert_ne!(
            deserialized.node.max_queued_requests,
            default_cfg.node.max_queued_requests
        );
        assert_ne!(
            deserialized.node.network_threads,
            default_cfg.node.network_threads
        );
        assert_ne!(
            deserialized.node.background_threads,
            default_cfg.node.background_threads
        );
        assert_ne!(
            deserialized.node.online_weight_minimum,
            default_cfg.node.online_weight_minimum
        );
        assert_ne!(
            deserialized.node.representative_vote_weight_minimum,
            default_cfg.node.representative_vote_weight_minimum
        );
        assert_ne!(
            deserialized.node.rep_crawler_weight_minimum,
            default_cfg.node.rep_crawler_weight_minimum
        );
        assert_ne!(
            deserialized.node.password_fanout,
            default_cfg.node.password_fanout
        );
        assert_ne!(
            deserialized.node.peering_port,
            default_cfg.node.peering_port
        );
        assert_ne!(
            deserialized.node.pow_sleep_interval_ns,
            default_cfg.node.pow_sleep_interval_ns
        );
        assert_ne!(
            deserialized.node.preconfigured_peers,
            default_cfg.node.preconfigured_peers
        );
        assert_ne!(
            deserialized.node.preconfigured_representatives,
            default_cfg.node.preconfigured_representatives
        );
        assert_ne!(
            deserialized.node.receive_minimum,
            default_cfg.node.receive_minimum
        );
        assert_ne!(
            deserialized.node.signature_checker_threads,
            default_cfg.node.signature_checker_threads
        );
        assert_ne!(
            deserialized.node.unchecked_cutoff_time_s,
            default_cfg.node.unchecked_cutoff_time_s
        );
        assert_ne!(
            deserialized.node.use_memory_pools,
            default_cfg.node.use_memory_pools
        );
        assert_ne!(
            deserialized.node.vote_generator_delay_ms,
            default_cfg.node.vote_generator_delay_ms
        );
        assert_ne!(
            deserialized.node.vote_minimum,
            default_cfg.node.vote_minimum
        );
        assert_ne!(deserialized.node.work_peers, default_cfg.node.work_peers);
        assert_ne!(
            deserialized.node.work_threads,
            default_cfg.node.work_threads
        );
        assert_ne!(
            deserialized.node.max_work_generate_multiplier,
            default_cfg.node.max_work_generate_multiplier
        );
        assert_ne!(
            deserialized.node.request_aggregator_threads,
            default_cfg.node.request_aggregator_threads
        );
        assert_ne!(
            deserialized.node.max_unchecked_blocks,
            default_cfg.node.max_unchecked_blocks
        );
        assert_ne!(
            deserialized.node.backlog_scan.enabled,
            default_cfg.node.backlog_scan.enabled
        );
        assert_ne!(
            deserialized.node.backlog_scan.rate_limit,
            default_cfg.node.backlog_scan.rate_limit
        );

        // Block Processor section
        assert_ne!(
            deserialized.node.block_processor.max_peer_queue,
            default_cfg.node.block_processor.max_peer_queue
        );
        assert_ne!(
            deserialized.node.block_processor.max_system_queue,
            default_cfg.node.block_processor.max_system_queue
        );
        assert_ne!(
            deserialized.node.block_processor.priority_live,
            default_cfg.node.block_processor.priority_live
        );
        assert_ne!(
            deserialized.node.block_processor.priority_bootstrap,
            default_cfg.node.block_processor.priority_bootstrap
        );
        assert_ne!(
            deserialized.node.block_processor.priority_local,
            default_cfg.node.block_processor.priority_local
        );

        // Active Elections section
        assert_ne!(
            deserialized.node.active_elections.size,
            default_cfg.node.active_elections.size
        );
        assert_ne!(
            deserialized.node.active_elections.hinted_limit_percentage,
            default_cfg.node.active_elections.hinted_limit_percentage
        );
        assert_ne!(
            deserialized
                .node
                .active_elections
                .optimistic_limit_percentage,
            default_cfg
                .node
                .active_elections
                .optimistic_limit_percentage
        );
        assert_ne!(
            deserialized.node.active_elections.confirmation_history_size,
            default_cfg.node.active_elections.confirmation_history_size
        );
        assert_ne!(
            deserialized.node.active_elections.confirmation_cache,
            default_cfg.node.active_elections.confirmation_cache
        );

        // Diagnostics section
        assert_ne!(
            deserialized.node.diagnostics_config.txn_tracking.enable,
            default_cfg.node.diagnostics_config.txn_tracking.enable
        );
        assert_ne!(
            deserialized
                .node
                .diagnostics_config
                .txn_tracking
                .ignore_writes_below_block_processor_max_time,
            default_cfg
                .node
                .diagnostics_config
                .txn_tracking
                .ignore_writes_below_block_processor_max_time
        );
        assert_ne!(
            deserialized
                .node
                .diagnostics_config
                .txn_tracking
                .min_read_txn_time_ms,
            default_cfg
                .node
                .diagnostics_config
                .txn_tracking
                .min_read_txn_time_ms
        );
        assert_ne!(
            deserialized
                .node
                .diagnostics_config
                .txn_tracking
                .min_write_txn_time_ms,
            default_cfg
                .node
                .diagnostics_config
                .txn_tracking
                .min_write_txn_time_ms
        );

        // HTTP Callback section
        assert_ne!(
            deserialized.node.callback_address,
            default_cfg.node.callback_address
        );
        assert_ne!(
            deserialized.node.callback_port,
            default_cfg.node.callback_port
        );
        assert_ne!(
            deserialized.node.callback_target,
            default_cfg.node.callback_target
        );

        // Priority Bucket section
        assert_ne!(
            deserialized.node.priority_bucket.max_blocks,
            default_cfg.node.priority_bucket.max_blocks
        );
        assert_ne!(
            deserialized.node.priority_bucket.max_elections,
            default_cfg.node.priority_bucket.max_elections
        );
        assert_ne!(
            deserialized.node.priority_bucket.reserved_elections,
            default_cfg.node.priority_bucket.reserved_elections
        );

        // Rep Crawler section
        assert_ne!(
            deserialized.node.rep_crawler_query_timeout,
            default_cfg.node.rep_crawler_query_timeout
        );

        // Monitor section
        assert_ne!(
            deserialized.node.enable_monitor,
            default_cfg.node.enable_monitor
        );
        assert_ne!(
            deserialized.node.monitor.interval,
            default_cfg.node.monitor.interval
        );

        // Statistics section
        assert_ne!(
            deserialized.node.stat_config.max_samples,
            default_cfg.node.stat_config.max_samples
        );

        // Statistics Log section
        assert_ne!(
            deserialized.node.stat_config.log_counters_filename,
            default_cfg.node.stat_config.log_counters_filename
        );
        assert_ne!(
            deserialized.node.stat_config.log_samples_filename,
            default_cfg.node.stat_config.log_samples_filename
        );
        assert_ne!(
            deserialized.node.stat_config.log_headers,
            default_cfg.node.stat_config.log_headers
        );
        assert_ne!(
            deserialized.node.stat_config.log_counters_interval,
            default_cfg.node.stat_config.log_counters_interval
        );
        assert_ne!(
            deserialized.node.stat_config.log_samples_interval,
            default_cfg.node.stat_config.log_samples_interval
        );
        assert_ne!(
            deserialized.node.stat_config.log_rotation_count,
            default_cfg.node.stat_config.log_rotation_count
        );

        // WebSocket section
        assert_ne!(
            deserialized.node.websocket_config.address,
            default_cfg.node.websocket_config.address
        );
        assert_ne!(
            deserialized.node.websocket_config.enabled,
            default_cfg.node.websocket_config.enabled
        );
        assert_ne!(
            deserialized.node.websocket_config.port,
            default_cfg.node.websocket_config.port
        );

        // LMDB section
        assert_ne!(
            deserialized.node.lmdb_config.sync,
            default_cfg.node.lmdb_config.sync
        );
        assert_ne!(
            deserialized.node.lmdb_config.max_databases,
            default_cfg.node.lmdb_config.max_databases
        );
        assert_ne!(
            deserialized.node.lmdb_config.map_size,
            default_cfg.node.lmdb_config.map_size
        );

        // Optimistic Scheduler section
        assert_ne!(
            deserialized.node.enable_optimistic_scheduler,
            default_cfg.node.enable_optimistic_scheduler
        );
        assert_ne!(
            deserialized.node.optimistic_scheduler.gap_threshold,
            default_cfg.node.optimistic_scheduler.gap_threshold
        );
        assert_ne!(
            deserialized.node.optimistic_scheduler.max_size,
            default_cfg.node.optimistic_scheduler.max_size
        );

        // Hinted Scheduler section
        assert_ne!(
            deserialized.node.enable_hinted_scheduler,
            default_cfg.node.enable_hinted_scheduler
        );
        assert_ne!(
            deserialized.node.hinted_scheduler.hinting_threshold_percent,
            default_cfg.node.hinted_scheduler.hinting_threshold_percent
        );
        assert_ne!(
            deserialized.node.hinted_scheduler.check_interval,
            default_cfg.node.hinted_scheduler.check_interval
        );
        assert_ne!(
            deserialized.node.hinted_scheduler.block_cooldown,
            default_cfg.node.hinted_scheduler.block_cooldown
        );
        assert_ne!(
            deserialized.node.hinted_scheduler.vacancy_threshold_percent,
            default_cfg.node.hinted_scheduler.vacancy_threshold_percent
        );

        // Vote Cache section
        assert_ne!(
            deserialized.node.vote_cache.age_cutoff,
            default_cfg.node.vote_cache.age_cutoff
        );
        assert_ne!(
            deserialized.node.vote_cache.max_size,
            default_cfg.node.vote_cache.max_size
        );
        assert_ne!(
            deserialized.node.vote_cache.max_voters,
            default_cfg.node.vote_cache.max_voters
        );

        // Vote Processor section
        assert_ne!(
            deserialized.node.vote_processor.max_pr_queue,
            default_cfg.node.vote_processor.max_pr_queue
        );
        assert_ne!(
            deserialized.node.vote_processor.max_non_pr_queue,
            default_cfg.node.vote_processor.max_non_pr_queue
        );
        assert_ne!(
            deserialized.node.vote_processor.pr_priority,
            default_cfg.node.vote_processor.pr_priority
        );
        assert_ne!(
            deserialized.node.vote_processor.threads,
            default_cfg.node.vote_processor.threads
        );
        assert_ne!(
            deserialized.node.vote_processor.batch_size,
            default_cfg.node.vote_processor.batch_size
        );

        // Bootstrap Ascending section
        assert_ne!(
            deserialized.node.bootstrap.block_processor_theshold,
            default_cfg.node.bootstrap.block_processor_theshold
        );
        assert_ne!(
            deserialized.node.bootstrap.database_rate_limit,
            default_cfg.node.bootstrap.database_rate_limit
        );
        assert_ne!(
            deserialized.node.bootstrap.max_pull_count,
            default_cfg.node.bootstrap.max_pull_count
        );
        assert_ne!(
            deserialized.node.bootstrap.channel_limit,
            default_cfg.node.bootstrap.channel_limit
        );
        assert_ne!(
            deserialized.node.bootstrap.rate_limit,
            default_cfg.node.bootstrap.rate_limit
        );
        assert_ne!(
            deserialized.node.bootstrap.throttle_coefficient,
            default_cfg.node.bootstrap.throttle_coefficient
        );
        assert_ne!(
            deserialized.node.bootstrap.throttle_wait,
            default_cfg.node.bootstrap.throttle_wait
        );
        assert_ne!(
            deserialized.node.bootstrap.request_timeout,
            default_cfg.node.bootstrap.request_timeout
        );

        // Bootstrap Ascending Account Sets section
        assert_ne!(
            deserialized.node.bootstrap.account_sets.blocking_max,
            default_cfg.node.bootstrap.account_sets.blocking_max
        );
        assert_ne!(
            deserialized.node.bootstrap.account_sets.consideration_count,
            default_cfg.node.bootstrap.account_sets.consideration_count
        );
        assert_ne!(
            deserialized.node.bootstrap.account_sets.cooldown,
            default_cfg.node.bootstrap.account_sets.cooldown
        );
        assert_ne!(
            deserialized.node.bootstrap.account_sets.priorities_max,
            default_cfg.node.bootstrap.account_sets.priorities_max
        );

        // Bootstrap Server section
        assert_ne!(
            deserialized.node.bootstrap_server.max_queue,
            default_cfg.node.bootstrap_server.max_queue
        );
        assert_ne!(
            deserialized.node.bootstrap_server.threads,
            default_cfg.node.bootstrap_server.threads
        );
        assert_ne!(
            deserialized.node.bootstrap_server.batch_size,
            default_cfg.node.bootstrap_server.batch_size
        );

        // Request Aggregator section
        assert_ne!(
            deserialized.node.request_aggregator.max_queue,
            default_cfg.node.request_aggregator.max_queue
        );
        assert_ne!(
            deserialized.node.request_aggregator.threads,
            default_cfg.node.request_aggregator.threads
        );
        assert_ne!(
            deserialized.node.request_aggregator.batch_size,
            default_cfg.node.request_aggregator.batch_size
        );

        // Message Processor section
        assert_ne!(
            deserialized.node.message_processor.threads,
            default_cfg.node.message_processor.threads
        );
        assert_ne!(
            deserialized.node.message_processor.max_queue,
            default_cfg.node.message_processor.max_queue
        );

        // OpenCL section
        assert_ne!(deserialized.opencl.device, default_cfg.opencl.device);
        assert_ne!(deserialized.opencl_enable, default_cfg.opencl_enable);
        assert_ne!(deserialized.opencl.platform, default_cfg.opencl.platform);
        assert_ne!(deserialized.opencl.threads, default_cfg.opencl.threads);

        // RPC section
        assert_ne!(deserialized.rpc_enable, default_cfg.rpc_enable);
        assert_ne!(
            deserialized.rpc.enable_sign_hash,
            default_cfg.rpc.enable_sign_hash
        );

        // RPC Child Process section
        assert_ne!(
            deserialized.rpc.child_process.enable,
            default_cfg.rpc.child_process.enable
        );
        assert_ne!(
            deserialized.rpc.child_process.rpc_path,
            default_cfg.rpc.child_process.rpc_path
        );
        assert_ne!(
            deserialized.node.bounded_backlog.max_backlog,
            default_cfg.node.bounded_backlog.max_backlog
        );
        assert_ne!(
            deserialized.node.enable_bounded_backlog,
            default_cfg.node.enable_bounded_backlog
        );
        assert_ne!(
            deserialized.node.bounded_backlog.batch_size,
            default_cfg.node.bounded_backlog.batch_size
        );
        assert_ne!(
            deserialized.node.bounded_backlog.scan_rate,
            default_cfg.node.bounded_backlog.scan_rate
        );

        // TCP
        assert_ne!(
            deserialized.node.tcp.max_inbound_connections,
            default_cfg.node.tcp.max_inbound_connections
        );
        assert_ne!(
            deserialized.node.tcp.max_outbound_connections,
            default_cfg.node.tcp.max_outbound_connections
        );
        assert_ne!(
            deserialized.node.tcp.max_attempts,
            default_cfg.node.tcp.max_attempts
        );
        assert_ne!(
            deserialized.node.tcp.max_attempts_per_ip,
            default_cfg.node.tcp.max_attempts_per_ip
        );
        assert_ne!(
            deserialized.node.tcp.connect_timeout,
            default_cfg.node.tcp.connect_timeout
        );
    }

    #[test]
    fn deserialize_empty() {
        let toml_str = "";
        let daemon_toml: DaemonToml = toml::from_str(toml_str).expect("Failed to deserialize TOML");

        let mut deserialized_daemon_config = create_default_daemon_config();
        deserialized_daemon_config.merge_toml(&daemon_toml);
        let default_daemon_config = create_default_daemon_config();

        assert_eq!(&deserialized_daemon_config, &default_daemon_config);
    }

    fn create_default_daemon_config() -> DaemonConfig {
        let mut config = DaemonConfig::new2(Networks::NanoBetaNetwork, 8);
        config.rpc.child_process.rpc_path = PathBuf::from("/home/foo/nano_rpc");
        config
    }
}
