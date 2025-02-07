use crate::cli::build_node;
use anyhow::Result;
use clap::{ArgGroup, Parser};
use rsnano_core::WalletId;
use rsnano_node::wallets::WalletsExt;

#[derive(Parser)]
#[command(group = ArgGroup::new("input")
    .args(&["data_path", "network"]))]
pub(crate) struct DestroyWalletArgs {
    /// The wallet to be destroyed
    #[arg(long)]
    wallet: String,
    /// Optional password to unlock the wallet
    #[arg(long)]
    password: Option<String>,
    /// Uses sthe supplied path as the data directory
    #[arg(long, group = "input")]
    data_path: Option<String>,
    /// Uses the supplied network (live, test, beta or dev)
    #[arg(long, group = "input")]
    network: Option<String>,
}

impl DestroyWalletArgs {
    pub(crate) fn destroy_wallet(&self) -> Result<()> {
        let node = build_node(&self.data_path, &self.network)?;

        let wallet_id = WalletId::decode_hex(&self.wallet)?;
        let password = self.password.clone().unwrap_or_default();
        node.wallets.ensure_wallet_is_unlocked(wallet_id, &password);
        node.wallets.destroy(&wallet_id);
        Ok(())
    }
}
