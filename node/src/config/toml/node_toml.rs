use super::*;
use crate::config::NodeConfig;
use bounded_backlog_toml::BoundedBacklogToml;
use rsnano_core::{utils::Peer, Account, Amount};
use serde::{Deserialize, Serialize};
use std::{str::FromStr, time::Duration};
use tcp_toml::TcpToml;

#[derive(Serialize, Deserialize, Default)]
pub struct NodeToml {
    pub allow_local_peers: Option<bool>,
    pub background_threads: Option<u32>,
    pub backup_before_upgrade: Option<bool>,
    pub bandwidth_limit: Option<usize>,
    pub bandwidth_limit_burst_ratio: Option<f64>,
    pub max_peers_per_ip: Option<u16>,
    pub max_peers_per_subnetwork: Option<u16>,
    pub block_processor_batch_max_time: Option<i64>,
    pub bootstrap_bandwidth_burst_ratio: Option<f64>,
    pub bootstrap_bandwidth_limit: Option<usize>,
    pub bootstrap_fraction_numerator: Option<u32>,
    pub bootstrap_initiator_threads: Option<u32>,
    pub bootstrap_serving_threads: Option<u32>,
    pub confirming_set_batch_time: Option<u64>,
    pub enable_voting: Option<bool>,
    pub external_address: Option<String>,
    pub external_port: Option<u16>,
    pub io_threads: Option<u32>,
    pub max_queued_requests: Option<u32>,
    pub max_unchecked_blocks: Option<u32>,
    pub max_backlog: Option<usize>,
    pub max_work_generate_multiplier: Option<f64>,
    pub network_threads: Option<u32>,
    pub online_weight_minimum: Option<String>,
    pub password_fanout: Option<u32>,
    pub peering_port: Option<u16>,
    pub pow_sleep_interval: Option<i64>,
    pub preconfigured_peers: Option<Vec<String>>,
    pub preconfigured_representatives: Option<Vec<String>>,
    pub receive_minimum: Option<String>,
    pub rep_crawler_weight_minimum: Option<String>,
    pub representative_vote_weight_minimum: Option<String>,
    pub request_aggregator_threads: Option<u32>,
    pub signature_checker_threads: Option<u32>,
    pub unchecked_cutoff_time: Option<i64>,
    pub use_memory_pools: Option<bool>,
    pub vote_generator_delay: Option<u64>,
    pub vote_minimum: Option<String>,
    pub work_peers: Option<Vec<String>>,
    pub work_threads: Option<u32>,
    pub active_elections: Option<ActiveElectionsToml>,
    pub block_processor: Option<BlockProcessorToml>,
    pub bootstrap: Option<BootstrapToml>,
    pub bootstrap_server: Option<BootstrapServerToml>,
    pub diagnostics: Option<DiagnosticsToml>,
    pub experimental: Option<ExperimentalToml>,
    pub httpcallback: Option<HttpcallbackToml>,
    pub lmdb: Option<LmdbToml>,
    pub message_processor: Option<MessageProcessorToml>,
    pub monitor: Option<MonitorToml>,
    pub optimistic_scheduler: Option<OptimisticSchedulerToml>,
    pub hinted_scheduler: Option<HintedSchedulerToml>,
    pub priority_bucket: Option<PriorityBucketToml>,
    pub rep_crawler: Option<RepCrawlerToml>,
    pub request_aggregator: Option<RequestAggregatorToml>,
    pub statistics: Option<StatsToml>,
    pub vote_cache: Option<VoteCacheToml>,
    pub vote_processor: Option<VoteProcessorToml>,
    pub websocket: Option<WebsocketToml>,
    pub backlog_scan: Option<BacklogScanToml>,
    pub bounded_backlog: Option<BoundedBacklogToml>,
    pub tcp: Option<TcpToml>,
    pub max_ledger_notifications: Option<usize>,
}

impl NodeConfig {
    pub fn merge_toml(&mut self, toml: &NodeToml) {
        if let Some(allow_local_peers) = toml.allow_local_peers {
            self.allow_local_peers = allow_local_peers;
        }
        if let Some(background_threads) = toml.background_threads {
            self.background_threads = background_threads;
        }
        if let Some(backup_before_upgrade) = toml.backup_before_upgrade {
            self.backup_before_upgrade = backup_before_upgrade;
        }
        if let Some(bandwidth_limit) = toml.bandwidth_limit {
            self.bandwidth_limit = bandwidth_limit;
        }
        if let Some(max) = toml.max_peers_per_ip {
            self.max_peers_per_ip = max;
        }
        if let Some(max) = toml.max_peers_per_subnetwork {
            self.max_peers_per_subnetwork = max;
        }
        if let Some(bandwidth_limit_burst_ratio) = toml.bandwidth_limit_burst_ratio {
            self.bandwidth_limit_burst_ratio = bandwidth_limit_burst_ratio;
        }
        if let Some(block_processor_batch_max_time_ms) = toml.block_processor_batch_max_time {
            self.block_processor_batch_max_time_ms = block_processor_batch_max_time_ms;
        }
        if let Some(bootstrap_bandwidth_burst_ratio) = toml.bootstrap_bandwidth_burst_ratio {
            self.bootstrap_bandwidth_burst_ratio = bootstrap_bandwidth_burst_ratio;
        }
        if let Some(bootstrap_bandwidth_limit) = toml.bootstrap_bandwidth_limit {
            self.bootstrap_bandwidth_limit = bootstrap_bandwidth_limit;
        }
        if let Some(bootstrap_fraction_numerator) = toml.bootstrap_fraction_numerator {
            self.bootstrap_fraction_numerator = bootstrap_fraction_numerator;
        }
        if let Some(bootstrap_initiator_threads) = toml.bootstrap_initiator_threads {
            self.bootstrap_initiator_threads = bootstrap_initiator_threads;
        }
        if let Some(bootstrap_serving_threads) = toml.bootstrap_serving_threads {
            self.bootstrap_serving_threads = bootstrap_serving_threads;
        }
        if let Some(confirming_set_batch_time) = &toml.confirming_set_batch_time {
            self.confirming_set_batch_time = Duration::from_millis(*confirming_set_batch_time);
        }
        if let Some(enable_voting) = toml.enable_voting {
            self.enable_voting = enable_voting;
        }
        if let Some(opt) = &toml.optimistic_scheduler {
            if let Some(enable) = opt.enable {
                self.enable_optimistic_scheduler = enable;
            }
        }
        if let Some(external_address) = &toml.external_address {
            self.external_address = external_address.clone();
        }
        if let Some(external_port) = toml.external_port {
            self.external_port = external_port;
        }
        if let Some(io_threads) = toml.io_threads {
            self.io_threads = io_threads;
        }
        if let Some(max_queued_requests) = toml.max_queued_requests {
            self.max_queued_requests = max_queued_requests;
        }
        if let Some(max_unchecked_blocks) = toml.max_unchecked_blocks {
            self.max_unchecked_blocks = max_unchecked_blocks;
        }
        if let Some(max) = toml.max_backlog {
            self.bounded_backlog.max_backlog = max;
        }
        if let Some(max_work_generate_multiplier) = toml.max_work_generate_multiplier {
            self.max_work_generate_multiplier = max_work_generate_multiplier;
        }
        if let Some(network_threads) = toml.network_threads {
            self.network_threads = network_threads;
        }
        if let Some(online_weight_minimum) = &toml.online_weight_minimum {
            self.online_weight_minimum =
                Amount::decode_dec(&online_weight_minimum).expect("Invalid online weight minimum");
        }
        if let Some(password_fanout) = toml.password_fanout {
            self.password_fanout = password_fanout;
        }
        if let Some(peering_port) = toml.peering_port {
            self.peering_port = Some(peering_port);
        }
        if let Some(pow_sleep_interval_ns) = toml.pow_sleep_interval {
            self.pow_sleep_interval_ns = pow_sleep_interval_ns;
        }
        if let Some(preconfigured_peers) = &toml.preconfigured_peers {
            self.preconfigured_peers =
                Peer::parse_list(preconfigured_peers, self.default_peering_port);
        }
        if let Some(preconfigured_representatives) = &toml.preconfigured_representatives {
            self.preconfigured_representatives = preconfigured_representatives
                .iter()
                .map(|string| {
                    Account::decode_account(&string)
                        .expect("Invalid representative")
                        .into()
                })
                .collect();
        }
        if let Some(receive_minimum) = &toml.receive_minimum {
            self.receive_minimum =
                Amount::decode_dec(&receive_minimum).expect("Invalid receive minimum");
        }
        if let Some(rep_crawler) = &toml.rep_crawler {
            if let Some(query_timeout) = rep_crawler.query_timeout {
                self.rep_crawler_query_timeout = Duration::from_millis(query_timeout);
            }
        }
        if let Some(representative_vote_weight_minimum) = &toml.representative_vote_weight_minimum {
            self.representative_vote_weight_minimum =
                Amount::decode_dec(&representative_vote_weight_minimum)
                    .expect("Invalid representative vote weight minimum");
        }
        if let Some(request_aggregator_threads) = toml.request_aggregator_threads {
            self.request_aggregator_threads = request_aggregator_threads;
        }
        if let Some(signature_checker_threads) = toml.signature_checker_threads {
            self.signature_checker_threads = signature_checker_threads;
        }
        if let Some(unchecked_cutoff_time_s) = toml.unchecked_cutoff_time {
            self.unchecked_cutoff_time_s = unchecked_cutoff_time_s;
        }
        if let Some(use_memory_pools) = toml.use_memory_pools {
            self.use_memory_pools = use_memory_pools;
        }
        if let Some(delay) = toml.vote_generator_delay {
            self.vote_generator_delay = Duration::from_millis(delay);
        }
        if let Some(vote_minimum) = &toml.vote_minimum {
            self.vote_minimum = Amount::decode_dec(&vote_minimum).expect("Invalid vote minimum");
        }
        if let Some(work_peers) = &toml.work_peers {
            self.work_peers = work_peers
                .iter()
                .map(|string| Peer::from_str(&string).expect("Invalid work peer"))
                .collect();
        }
        if let Some(work_threads) = toml.work_threads {
            self.work_threads = work_threads;
        }
        if let Some(cfg) = &toml.optimistic_scheduler {
            if let Some(enable) = cfg.enable {
                self.enable_optimistic_scheduler = enable;
            }
            self.optimistic_scheduler.merge_toml(cfg);
        }
        if let Some(cfg) = &toml.hinted_scheduler {
            if let Some(enable) = cfg.enable {
                self.enable_hinted_scheduler = enable;
            }
            self.hinted_scheduler.merge_toml(&cfg);
        }
        if let Some(priority_bucket_toml) = &toml.priority_bucket {
            self.priority_bucket = priority_bucket_toml.into();
        }
        if let Some(ascending_toml) = &toml.bootstrap {
            let config = &mut self.bootstrap;
            if let Some(enable) = &ascending_toml.enable {
                config.enable = *enable;
            }
            if let Some(enable) = &ascending_toml.enable_dependency_walker {
                config.enable_dependency_walker = *enable;
            }
            if let Some(enable) = &ascending_toml.enable_databaser_scan {
                config.enable_database_scan = *enable;
            }
            if let Some(enable) = &ascending_toml.enable_frontier_scan {
                config.enable_frontier_scan = *enable;
            }
            if let Some(account_sets) = &ascending_toml.account_sets {
                config.account_sets = account_sets.into();
            }
            if let Some(block_wait_count) = ascending_toml.block_processor_threshold {
                config.block_processor_theshold = block_wait_count;
            }
            if let Some(database_rate_limit) = ascending_toml.database_rate_limit {
                config.database_rate_limit = database_rate_limit;
            }
            if let Some(database_warmup_ratio) = ascending_toml.database_warmup_ratio {
                config.database_warmup_ratio = database_warmup_ratio;
            }
            if let Some(pull_count) = ascending_toml.max_pull_count {
                config.max_pull_count = pull_count;
            }
            if let Some(limit) = ascending_toml.channel_limit {
                config.channel_limit = limit;
            }
            if let Some(limit) = ascending_toml.rate_limit {
                config.rate_limit = limit;
            }
            if let Some(timeout) = &ascending_toml.request_timeout {
                config.request_timeout = Duration::from_millis(*timeout);
            }
            if let Some(throttle_wait) = &ascending_toml.throttle_wait {
                config.throttle_wait = Duration::from_millis(*throttle_wait);
            }
            if let Some(throttle_coefficient) = ascending_toml.throttle_coefficient {
                config.throttle_coefficient = throttle_coefficient;
            }
            if let Some(max) = ascending_toml.max_requests {
                config.max_requests = max;
            }
            if let Some(percent) = ascending_toml.optimistic_request_percentage {
                config.optimistic_request_percentage = percent;
            }
        }
        if let Some(bootstrap_server_toml) = &toml.bootstrap_server {
            self.bootstrap_responder = bootstrap_server_toml.into();
        }
        if let Some(websocket_config_toml) = &toml.websocket {
            self.websocket_config.merge_toml(&websocket_config_toml);
        }
        if let Some(diagnostics_config_toml) = &toml.diagnostics {
            self.diagnostics_config = diagnostics_config_toml.into();
        }
        if let Some(stat_config_toml) = &toml.statistics {
            self.stat_config = stat_config_toml.into();
        }
        if let Some(lmdb_config_toml) = &toml.lmdb {
            self.lmdb_config = lmdb_config_toml.into();
        }
        if let Some(vote_cache_toml) = &toml.vote_cache {
            self.vote_cache = vote_cache_toml.into();
        }
        if let Some(block_processor_toml) = &toml.block_processor {
            self.block_processor.merge_toml(block_processor_toml);
        }
        if let Some(active_elections_toml) = &toml.active_elections {
            self.active_elections = active_elections_toml.into();
        }
        if let Some(vote_processor_toml) = &toml.vote_processor {
            self.vote_processor.merge_toml(&vote_processor_toml);
        }
        if let Some(request_aggregator_toml) = &toml.request_aggregator {
            self.request_aggregator.merge_toml(request_aggregator_toml);
        }
        if let Some(message_processor_toml) = &toml.message_processor {
            self.message_processor.merge_toml(message_processor_toml);
        }
        if let Some(cfg) = &toml.monitor {
            if let Some(enable) = cfg.enable {
                self.enable_monitor = enable;
            }
            if let Some(secs) = cfg.interval {
                self.monitor.interval = Duration::from_secs(secs);
            }
        }
        if let Some(rep_crawler_weight_minimum) = &toml.rep_crawler_weight_minimum {
            self.rep_crawler_weight_minimum = Amount::decode_dec(&rep_crawler_weight_minimum)
                .expect("Invalid rep crawler weight minimum");
        }
        if let Some(httpcallback) = &toml.httpcallback {
            if let Some(address) = &httpcallback.address {
                self.callback_address = address.clone();
            }
            if let Some(port) = &httpcallback.port {
                self.callback_port = port.clone();
            }
            if let Some(target) = &httpcallback.target {
                self.callback_target = target.clone();
            }
        }
        if let Some(toml) = &toml.backlog_scan {
            self.backlog_scan.merge_toml(toml);
        }
        self.bounded_backlog.merge_toml(toml);
        if let Some(toml) = &toml.bounded_backlog {
            if let Some(enable) = toml.enable {
                self.enable_bounded_backlog = enable;
            }
        }

        if let Some(toml) = &toml.tcp {
            self.tcp.merge_toml(toml);
        }

        if let Some(i) = toml.max_ledger_notifications {
            self.max_ledger_notifications = i;
        }
    }
}

impl From<&NodeConfig> for NodeToml {
    fn from(config: &NodeConfig) -> Self {
        Self {
            allow_local_peers: Some(config.allow_local_peers),
            background_threads: Some(config.background_threads),
            backup_before_upgrade: Some(config.backup_before_upgrade),
            bandwidth_limit: Some(config.bandwidth_limit),
            bandwidth_limit_burst_ratio: Some(config.bandwidth_limit_burst_ratio),
            max_peers_per_ip: Some(config.max_peers_per_ip),
            max_peers_per_subnetwork: Some(config.max_peers_per_subnetwork),
            block_processor_batch_max_time: Some(config.block_processor_batch_max_time_ms),
            bootstrap_bandwidth_burst_ratio: Some(config.bootstrap_bandwidth_burst_ratio),
            bootstrap_bandwidth_limit: Some(config.bootstrap_bandwidth_limit),
            bootstrap_fraction_numerator: Some(config.bootstrap_fraction_numerator),
            bootstrap_initiator_threads: Some(config.bootstrap_initiator_threads),
            bootstrap_serving_threads: Some(config.bootstrap_serving_threads),
            confirming_set_batch_time: Some(config.confirming_set_batch_time.as_millis() as u64),
            enable_voting: Some(config.enable_voting),
            external_address: Some(config.external_address.clone()),
            external_port: Some(config.external_port),
            io_threads: Some(config.io_threads),
            max_queued_requests: Some(config.max_queued_requests),
            max_unchecked_blocks: Some(config.max_unchecked_blocks),
            max_backlog: Some(config.bounded_backlog.max_backlog),
            max_work_generate_multiplier: Some(config.max_work_generate_multiplier),
            network_threads: Some(config.network_threads),
            online_weight_minimum: Some(config.online_weight_minimum.to_string_dec()),
            password_fanout: Some(config.password_fanout),
            peering_port: config.peering_port,
            pow_sleep_interval: Some(config.pow_sleep_interval_ns),
            preconfigured_peers: Some(
                config
                    .preconfigured_peers
                    .iter()
                    .map(|p| p.to_string())
                    .collect(),
            ),
            preconfigured_representatives: Some(
                config
                    .preconfigured_representatives
                    .iter()
                    .map(|pk| Account::from(pk).encode_account())
                    .collect(),
            ),
            receive_minimum: Some(config.receive_minimum.to_string_dec()),
            rep_crawler_weight_minimum: Some(config.rep_crawler_weight_minimum.to_string_dec()),
            representative_vote_weight_minimum: Some(
                config.representative_vote_weight_minimum.to_string_dec(),
            ),
            request_aggregator_threads: Some(config.request_aggregator_threads),
            signature_checker_threads: Some(config.signature_checker_threads),
            unchecked_cutoff_time: Some(config.unchecked_cutoff_time_s),
            use_memory_pools: Some(config.use_memory_pools),
            vote_generator_delay: Some(config.vote_generator_delay.as_millis() as u64),
            vote_minimum: Some(config.vote_minimum.to_string_dec()),
            work_peers: Some(
                config
                    .work_peers
                    .iter()
                    .map(|peer| peer.to_string())
                    .collect(),
            ),
            work_threads: Some(config.work_threads),
            optimistic_scheduler: Some(OptimisticSchedulerToml {
                enable: Some(config.enable_optimistic_scheduler),
                gap_threshold: Some(config.optimistic_scheduler.gap_threshold),
                max_size: Some(config.optimistic_scheduler.max_size),
            }),
            hinted_scheduler: Some(HintedSchedulerToml {
                enable: Some(config.enable_hinted_scheduler),
                hinting_threshold: Some(config.hinted_scheduler.hinting_threshold_percent),
                check_interval: Some(config.hinted_scheduler.check_interval.as_millis() as u64),
                block_cooldown: Some(config.hinted_scheduler.block_cooldown.as_millis() as u64),
                vacancy_threshold: Some(config.hinted_scheduler.vacancy_threshold_percent),
            }),
            priority_bucket: Some((&config.priority_bucket).into()),
            bootstrap: Some((&config.bootstrap).into()),
            bootstrap_server: Some((&config.bootstrap_responder).into()),
            websocket: Some((&config.websocket_config).into()),
            diagnostics: Some((&config.diagnostics_config).into()),
            statistics: Some((&config.stat_config).into()),
            lmdb: Some((&config.lmdb_config).into()),
            vote_cache: Some((&config.vote_cache).into()),
            block_processor: Some((&config.block_processor).into()),
            active_elections: Some((&config.active_elections).into()),
            vote_processor: Some((&config.vote_processor).into()),
            request_aggregator: Some((&config.request_aggregator).into()),
            message_processor: Some((&config.message_processor).into()),
            monitor: Some(MonitorToml {
                enable: Some(config.enable_monitor),
                interval: Some(config.monitor.interval.as_secs()),
            }),
            httpcallback: Some(config.into()),
            rep_crawler: Some(config.into()),
            experimental: Some(config.into()),
            backlog_scan: Some((&config.backlog_scan).into()),
            bounded_backlog: Some(config.into()),
            tcp: Some((&config.tcp).into()),
            max_ledger_notifications: Some(config.max_ledger_notifications),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::toml::AccountSetsToml;

    #[test]
    fn merge_bootstrap_ascending_toml() {
        let sets_toml = AccountSetsToml {
            blocking_max: Some(200),
            consideration_count: Some(201),
            cooldown: Some(203),
            priorities_max: Some(204),
        };

        let ascending_toml = BootstrapToml {
            enable: Some(false),
            enable_databaser_scan: Some(false),
            enable_dependency_walker: Some(false),
            enable_frontier_scan: Some(false),
            block_processor_threshold: Some(100),
            database_rate_limit: Some(101),
            max_pull_count: Some(102),
            channel_limit: Some(103),
            rate_limit: Some(200),
            throttle_coefficient: Some(104),
            throttle_wait: Some(105),
            request_timeout: Some(106),
            max_requests: Some(107),
            optimistic_request_percentage: Some(42),
            database_warmup_ratio: Some(108),
            account_sets: Some(sets_toml),
        };

        let toml = NodeToml {
            bootstrap: Some(ascending_toml),
            ..Default::default()
        };

        let mut cfg = NodeConfig::new_test_instance();
        cfg.merge_toml(&toml);

        let ascending = &cfg.bootstrap;
        assert_eq!(ascending.enable, false);
        assert_eq!(ascending.enable_database_scan, false);
        assert_eq!(ascending.enable_dependency_walker, false);
        assert_eq!(ascending.enable_frontier_scan, false);
        assert_eq!(ascending.block_processor_theshold, 100);
        assert_eq!(ascending.database_rate_limit, 101);
        assert_eq!(ascending.max_pull_count, 102);
        assert_eq!(ascending.channel_limit, 103);
        assert_eq!(ascending.rate_limit, 200);
        assert_eq!(ascending.throttle_coefficient, 104);
        assert_eq!(ascending.throttle_wait, Duration::from_millis(105));
        assert_eq!(ascending.request_timeout, Duration::from_millis(106));
        assert_eq!(ascending.max_requests, 107);
        assert_eq!(ascending.optimistic_request_percentage, 42);
        assert_eq!(ascending.database_warmup_ratio, 108);

        let sets = &cfg.bootstrap.account_sets;
        assert_eq!(sets.blocking_max, 200);
        assert_eq!(sets.consideration_count, 201);
        assert_eq!(sets.cooldown, Duration::from_millis(203));
        assert_eq!(sets.priorities_max, 204);
    }

    #[test]
    fn create_bootstrap_ascending_toml() {
        let cfg = NodeConfig::new_test_instance();
        let toml = NodeToml::from(&cfg);
        let ascending_toml = toml.bootstrap.as_ref().unwrap();
        assert_eq!(ascending_toml.enable, Some(true));
        assert_eq!(ascending_toml.enable_databaser_scan, Some(false));
        assert_eq!(ascending_toml.enable_frontier_scan, Some(true));
        assert_eq!(ascending_toml.enable_dependency_walker, Some(true));
        assert_eq!(ascending_toml.block_processor_threshold, Some(1000));
        assert_eq!(ascending_toml.database_rate_limit, Some(256));
        assert_eq!(ascending_toml.database_warmup_ratio, Some(10));
        assert_eq!(ascending_toml.max_pull_count, Some(128));
        assert_eq!(ascending_toml.channel_limit, Some(16));
        assert_eq!(ascending_toml.throttle_coefficient, Some(1024 * 8));
        assert_eq!(ascending_toml.throttle_wait, Some(100));
        assert_eq!(ascending_toml.request_timeout, Some(15000));
        assert_eq!(ascending_toml.max_requests, Some(1024));
        assert_eq!(ascending_toml.optimistic_request_percentage, Some(75));

        let sets_toml = ascending_toml.account_sets.as_ref().unwrap();
        assert_eq!(sets_toml.consideration_count, Some(4));
        assert_eq!(sets_toml.priorities_max, Some(1024 * 256));
        assert_eq!(sets_toml.blocking_max, Some(1024 * 256));
        assert_eq!(sets_toml.cooldown, Some(3000));
    }
}
