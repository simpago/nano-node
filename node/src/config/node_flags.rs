use crate::block_processing::BlockProcessorConfig;
use rsnano_ledger::GenerateCacheFlags;

#[derive(Clone)]
pub struct NodeFlags {
    pub config_overrides: Vec<String>,
    pub rpc_config_overrides: Vec<String>,
    pub disable_activate_successors: bool,
    pub disable_backup: bool,
    pub disable_lazy_bootstrap: bool,
    pub disable_legacy_bootstrap: bool,
    pub disable_wallet_bootstrap: bool,
    pub disable_bootstrap_bulk_pull_server: bool,
    pub disable_bootstrap_bulk_push_client: bool,
    pub disable_ongoing_bootstrap: bool, // For testing only
    pub disable_rep_crawler: bool,
    pub disable_request_loop: bool, // For testing only
    pub disable_providing_telemetry_metrics: bool,
    pub disable_block_processor_unchecked_deletion: bool,
    pub disable_block_processor_republishing: bool,
    pub allow_bootstrap_peers_duplicates: bool,
    pub disable_search_pending: bool, // For testing only
    pub enable_pruning: bool,
    pub fast_bootstrap: bool,
    pub read_only: bool,
    pub disable_connection_cleanup: bool,
    pub generate_cache: GenerateCacheFlags,
    pub inactive_node: bool,
    pub block_processor_batch_size: usize,
    pub block_processor_full_size: usize,
    pub block_processor_verification_size: usize,
    pub vote_processor_capacity: usize,
    pub bootstrap_interval: usize, // For testing only
}

impl NodeFlags {
    pub fn new() -> Self {
        Self {
            config_overrides: Vec::new(),
            rpc_config_overrides: Vec::new(),
            disable_activate_successors: false,
            disable_backup: false,
            disable_lazy_bootstrap: false,
            disable_legacy_bootstrap: false,
            disable_wallet_bootstrap: false,
            disable_bootstrap_bulk_pull_server: false,
            disable_bootstrap_bulk_push_client: false,
            disable_ongoing_bootstrap: false,
            disable_rep_crawler: false,
            disable_request_loop: false,
            disable_providing_telemetry_metrics: false,
            disable_block_processor_unchecked_deletion: false,
            disable_block_processor_republishing: false,
            allow_bootstrap_peers_duplicates: false,
            disable_search_pending: false,
            enable_pruning: false,
            fast_bootstrap: false,
            read_only: false,
            disable_connection_cleanup: false,
            generate_cache: GenerateCacheFlags::new(),
            inactive_node: false,
            block_processor_batch_size: BlockProcessorConfig::DEFAULT_BATCH_SIZE,
            block_processor_full_size: BlockProcessorConfig::DEFAULT_FULL_SIZE,
            block_processor_verification_size: 0,
            vote_processor_capacity: 144 * 1024,
            bootstrap_interval: 0,
        }
    }
}

impl Default for NodeFlags {
    fn default() -> Self {
        Self::new()
    }
}
