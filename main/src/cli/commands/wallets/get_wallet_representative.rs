use crate::cli::build_node;
use anyhow::{anyhow, Result};
use clap::{ArgGroup, Parser};
use rsnano_core::{Account, WalletId};
use rsnano_node::wallets::WalletsExt;

#[derive(Parser)]
#[command(group = ArgGroup::new("input")
    .args(&["data_path", "network"]))]
pub(crate) struct GetWalletRepresentativeArgs {
    /// Gets the representative of the supplied <wallet>
    #[arg(long)]
    wallet: String,
    /// Optional password to unlock the wallet
    #[arg(long)]
    password: Option<String>,
    #[arg(long)]
    /// Uses the supplied path as the data directory
    #[arg(long, group = "input")]
    data_path: Option<String>,
    /// Uses the supplied network (live, test, beta or dev)
    #[arg(long, group = "input")]
    network: Option<String>,
}

impl GetWalletRepresentativeArgs {
    pub(crate) fn get_wallet_representative(&self) -> Result<()> {
        let node = build_node(&self.data_path, &self.network)?;
        let wallet_id = WalletId::decode_hex(&self.wallet)?;
        let password = self.password.clone().unwrap_or_default();

        node.wallets.ensure_wallet_is_unlocked(wallet_id, &password);

        let representative = node
            .wallets
            .get_representative(wallet_id)
            .map_err(|e| anyhow!("Failed to get wallet representative: {:?}", e))?;

        println!(
            "Representative: {:?}",
            Account::from(representative).encode_account()
        );

        Ok(())
    }
}
