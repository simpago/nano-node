use crate::cli::build_node;
use anyhow::{anyhow, Result};
use clap::{ArgGroup, Parser};
use rsnano_core::Account;

#[derive(Parser)]
#[command(group = ArgGroup::new("input")
    .args(&["data_path", "network"]))]
pub(crate) struct ListWalletsArgs {
    /// Uses the supplied path as the data directory
    #[arg(long, group = "input")]
    data_path: Option<String>,
    /// Uses the supplied network (live, test, beta or dev)
    #[arg(long, group = "input")]
    network: Option<String>,
}

impl ListWalletsArgs {
    pub(crate) fn list_wallets(&self) -> Result<()> {
        let node = build_node(&self.data_path, &self.network)?;

        let wallet_ids = node.wallets.get_wallet_ids();

        for wallet_id in wallet_ids {
            println!("{:?}", wallet_id);
            let accounts = node
                .wallets
                .get_accounts_of_wallet(&wallet_id)
                .map_err(|e| anyhow!("Failed to get accounts of wallets: {:?}", e))?;
            if !accounts.is_empty() {
                for account in accounts {
                    println!("{:?}", Account::encode_account(&account));
                }
            }
        }

        Ok(())
    }
}
