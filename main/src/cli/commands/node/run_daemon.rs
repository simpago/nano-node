use anyhow::{anyhow, Result};
use clap::Parser;
use rsnano_core::Networks;
use rsnano_daemon::DaemonBuilder;
use rsnano_node::config::NodeFlags;
use std::{path::PathBuf, str::FromStr};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
pub(crate) struct RunDaemonArgs {
    /// Uses the supplied path as the data directory
    #[arg(long)]
    data_path: Option<String>,
    /// Uses the supplied network (live, test, beta or dev)
    #[arg(long)]
    network: Option<String>,
    /// Disables activate_successors in active_elections
    #[arg(long)]
    disable_activate_successors: bool,
    /// Turn off automatic wallet backup process
    #[arg(long)]
    disable_backup: bool,
    /// Turn off use of lazy bootstrap
    #[arg(long)]
    disable_lazy_bootstrap: bool,
    /// Turn off use of legacy bootstrap
    #[arg(long)]
    disable_legacy_bootstrap: bool,
    /// Turn off use of wallet-based bootstrap
    #[arg(long)]
    disable_wallet_bootstrap: bool,
    /// Turn off listener on the bootstrap network so incoming TCP (bootstrap) connections are rejected
    /// Note: this does not impact TCP traffic for the live network.
    #[arg(long, verbatim_doc_comment)]
    disable_bootstrap_listener: bool,
    /// Disables the legacy bulk pull server for bootstrap operations
    #[arg(long)]
    disable_bootstrap_bulk_pull_server: bool,
    /// Disables the legacy bulk push client for bootstrap operations
    #[arg(long)]
    disable_bootstrap_bulk_push_client: bool,
    /// Turn off the ability for ongoing bootstraps to occur
    #[arg(long)]
    disable_ongoing_bootstrap: bool,
    /// Turn off the request loop
    #[arg(long)]
    disable_request_loop: bool,
    /// Turn off the rep crawler process
    #[arg(long)]
    disable_rep_crawler: bool,
    /// Turn off use of TCP live network (TCP for bootstrap will remain available)
    #[arg(long)]
    disable_tcp_realtime: bool,
    /// Do not provide any telemetry data to nodes requesting it. Responses are still made to requests, but they will have an empty payload.
    #[arg(long)]
    disable_providing_telemetry_metrics: bool,
    /// Disable deletion of unchecked blocks after processing.
    #[arg(long)]
    disable_block_processor_unchecked_deletion: bool,
    /// Disables block republishing by disabling the local_block_broadcaster component
    #[arg(long)]
    disable_block_processor_republishing: bool,
    /// Allow multiple connections to the same peer in bootstrap attempts
    #[arg(long)]
    allow_bootstrap_peers_duplicates: bool,
    /// Enable experimental ledger pruning
    #[arg(long)]
    enable_pruning: bool,
    /// Increase bootstrap processor limits to allow more blocks before hitting full state and verify/write more per database call. Also disable deletion of processed unchecked blocks.
    #[arg(long)]
    fast_bootstrap: bool,
    /// Increase block processor transaction batch write size, default 0 (limited by config block_processor_batch_max_time), 256k for fast_bootstrap
    #[arg(long)]
    block_processor_batch_size: Option<usize>,
    /// Increase block processor allowed blocks queue size before dropping live network packets and holding bootstrap download, default 65536, 1 million for fast_bootstrap
    #[arg(long)]
    block_processor_full_size: Option<usize>,
    /// Increase batch signature verification size in block processor, default 0 (limited by config signature_checker_threads), unlimited for fast_bootstrap
    #[arg(long)]
    block_processor_verification_size: Option<usize>,
    /// Vote processor queue size before dropping votes, default 144k
    #[arg(long)]
    vote_processor_capacity: Option<usize>,
}

impl RunDaemonArgs {
    pub(crate) fn run_daemon(&self) -> Result<()> {
        init_tracing();
        let network = self.get_network()?;
        let flags = self.get_flags();
        let mut daemon = DaemonBuilder::new(network).flags(flags);
        if let Some(path) = self.specified_data_path() {
            daemon = daemon.data_path(path);
        }
        daemon.run(shutdown_signal())
    }

    pub fn specified_data_path(&self) -> Option<PathBuf> {
        self.data_path
            .as_ref()
            .map(|p| PathBuf::from_str(p).unwrap())
    }

    pub fn get_network(&self) -> anyhow::Result<Networks> {
        self.network
            .as_ref()
            .map(|s| Networks::from_str(s).map_err(|e| anyhow!(e)))
            .transpose()
            .map(|n| n.unwrap_or(Networks::NanoLiveNetwork))
    }

    pub(crate) fn get_flags(&self) -> NodeFlags {
        let mut flags = NodeFlags::new();
        flags.disable_activate_successors = self.disable_activate_successors;
        flags.disable_backup = self.disable_backup;
        flags.disable_lazy_bootstrap = self.disable_lazy_bootstrap;
        flags.disable_legacy_bootstrap = self.disable_legacy_bootstrap;
        flags.disable_wallet_bootstrap = self.disable_wallet_bootstrap;
        flags.disable_bootstrap_listener = self.disable_bootstrap_listener;
        flags.disable_bootstrap_bulk_pull_server = self.disable_bootstrap_bulk_pull_server;
        flags.disable_bootstrap_bulk_push_client = self.disable_bootstrap_bulk_push_client;
        flags.disable_ongoing_bootstrap = self.disable_ongoing_bootstrap;
        flags.disable_rep_crawler = self.disable_rep_crawler;
        flags.disable_request_loop = self.disable_request_loop;
        flags.disable_tcp_realtime = self.disable_tcp_realtime;
        flags.disable_providing_telemetry_metrics = self.disable_providing_telemetry_metrics;
        flags.disable_block_processor_unchecked_deletion =
            self.disable_block_processor_unchecked_deletion;
        flags.disable_block_processor_republishing = self.disable_block_processor_republishing;
        flags.allow_bootstrap_peers_duplicates = self.allow_bootstrap_peers_duplicates;
        flags.enable_pruning = self.enable_pruning;
        flags.fast_bootstrap = self.fast_bootstrap;
        if let Some(block_processor_batch_size) = self.block_processor_batch_size {
            flags.block_processor_batch_size = block_processor_batch_size;
        }
        if let Some(block_processor_full_size) = self.block_processor_full_size {
            flags.block_processor_full_size = block_processor_full_size;
        }
        if let Some(block_processor_verification_size) = self.block_processor_verification_size {
            flags.block_processor_verification_size = block_processor_verification_size;
        }
        if let Some(vote_processor_capacity) = self.vote_processor_capacity {
            flags.vote_processor_capacity = vote_processor_capacity;
        }

        flags
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}

fn init_tracing() {
    let dirs = std::env::var(EnvFilter::DEFAULT_ENV).unwrap_or(String::from("info"));
    let filter = EnvFilter::builder().parse_lossy(dirs);
    let value = std::env::var("NANO_LOG");
    let log_style = value.as_ref().map(|i| i.as_str()).unwrap_or_default();
    match log_style {
        "json" => {
            tracing_subscriber::fmt::fmt()
                .json()
                .with_env_filter(filter)
                .init();
        }
        "noansi" => {
            tracing_subscriber::fmt::fmt()
                .with_env_filter(filter)
                .with_ansi(false)
                .init();
        }
        _ => {
            tracing_subscriber::fmt::fmt()
                .with_env_filter(filter)
                .with_ansi(true)
                .init();
        }
    }
    tracing::debug!(log_style, ?value, "init tracing");
}
