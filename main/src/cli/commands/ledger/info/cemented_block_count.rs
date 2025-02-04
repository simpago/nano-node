use crate::cli::get_path;
use anyhow::Result;
use clap::{ArgGroup, Parser};
use rsnano_core::Amount;
use rsnano_ledger::{Ledger, RepWeightCache};
use rsnano_node::config::{NetworkConstants, NetworkParams};
use rsnano_store_lmdb::LmdbStore;
use std::sync::Arc;

#[derive(Parser)]
#[command(group = ArgGroup::new("input")
    .args(&["data_path", "network"]))]
pub(crate) struct CementedBlockCountArgs {
    /// Uses the supplied path as the data directory
    #[arg(long, group = "input")]
    data_path: Option<String>,
    /// Uses the supplied network (live, test, beta or dev)
    #[arg(long, group = "input")]
    network: Option<String>,
}

impl CementedBlockCountArgs {
    pub(crate) fn cemented_block_count(&self) -> Result<()> {
        let path = get_path(&self.data_path, &self.network).join("data.ldb");

        let network_params = NetworkParams::new(NetworkConstants::active_network());

        let ledger = Ledger::new(
            Arc::new(LmdbStore::open(&path).build()?),
            network_params.ledger,
            Amount::zero(),
            Arc::new(RepWeightCache::new()),
        )?;

        println!("Total cemented block count: {}", ledger.cemented_count());

        Ok(())
    }
}
