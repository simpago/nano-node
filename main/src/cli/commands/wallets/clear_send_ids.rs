use crate::cli::build_node;
use anyhow::Result;
use clap::{ArgGroup, Parser};

#[derive(Parser)]
#[command(group = ArgGroup::new("input")
    .args(&["data_path", "network"]))]
pub(crate) struct ClearSendIdsArgs {
    /// Uses the supplied path as the data directory
    #[arg(long, group = "input")]
    data_path: Option<String>,
    /// Uses the supplied network (live, test, beta or dev)
    #[arg(long, group = "input")]
    network: Option<String>,
}

impl ClearSendIdsArgs {
    pub(crate) fn clear_send_ids(&self) -> Result<()> {
        let node = build_node(&self.data_path, &self.network)?;
        node.wallets.clear_send_ids();
        println!("{}", "Send IDs deleted");
        Ok(())
    }
}
