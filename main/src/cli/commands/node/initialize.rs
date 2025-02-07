use crate::cli::build_node;
use anyhow::Result;
use clap::{ArgGroup, Parser};

#[derive(Parser)]
#[command(group = ArgGroup::new("input")
    .args(&["data_path", "network"]))]
pub(crate) struct InitializeArgs {
    /// Uses the supplied path as the data directory
    #[arg(long, group = "input")]
    data_path: Option<String>,
    /// Uses the supplied network (live, test, beta or dev)
    #[arg(long, group = "input")]
    network: Option<String>,
}

impl InitializeArgs {
    pub(crate) fn initialize(&self) -> Result<()> {
        build_node(&self.data_path, &self.network)?;
        Ok(())
    }
}
