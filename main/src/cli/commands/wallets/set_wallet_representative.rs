use crate::cli::build_node;
use anyhow::{anyhow, Result};
use clap::{ArgGroup, Parser};
use rsnano_core::{Account, WalletId};
use rsnano_node::wallets::WalletsExt;

#[derive(Parser)]
#[command(group = ArgGroup::new("input")
    .args(&["data_path", "network"]))]
pub(crate) struct SetWalletRepresentativeArgs {
    /// Sets the representative for the supplied <wallet>
    #[arg(long)]
    wallet: String,
    /// Sets the supplied account as the wallet representative
    #[arg(long)]
    account: String,
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

impl SetWalletRepresentativeArgs {
    pub(crate) fn set_representative_wallet(&self) -> Result<()> {
        let node = build_node(&self.data_path, &self.network)?;
        let wallet_id = WalletId::decode_hex(&self.wallet)?;
        let representative = Account::decode_account(&self.account)?.into();
        let password = self.password.clone().unwrap_or_default();

        node.wallets.ensure_wallet_is_unlocked(wallet_id, &password);

        node.wallets
            .set_representative(wallet_id, representative, false)
            .map_err(|e| anyhow!("Failed to set wallet representative: {:?}", e))?;

        Ok(())
    }
}
