use crate::cli::build_node;
use anyhow::{anyhow, Result};
use clap::{ArgGroup, Parser};
use rsnano_core::{Account, WalletId};
use rsnano_node::wallets::WalletsExt;

#[derive(Parser)]
#[command(group = ArgGroup::new("input")
    .args(&["data_path", "network"]))]
pub(crate) struct CreateAccountArgs {
    /// Creates an account in the supplied <wallet>
    #[arg(long)]
    wallet: String,
    /// Optional password to unlock the wallet
    #[arg(long)]
    password: Option<String>,
    /// Uses the supplied path as the data directory
    #[arg(long, group = "input")]
    data_path: Option<String>,
    /// Uses the supplied network (live, test, beta or dev)
    #[arg(long, group = "input")]
    network: Option<String>,
}

impl CreateAccountArgs {
    pub(crate) fn create_account(&self) -> Result<()> {
        let node = build_node(&self.data_path, &self.network)?;
        let wallet = WalletId::decode_hex(&self.wallet)?;
        let password = self.password.clone().unwrap_or_default();

        node.wallets.ensure_wallet_is_unlocked(wallet, &password);

        let public_key = node
            .wallets
            .deterministic_insert2(&wallet, false)
            .map_err(|e| anyhow!("Failed to insert wallet: {:?}", e))?;

        println!("Account: {:?}", Account::from(public_key).encode_account());

        Ok(())
    }
}
