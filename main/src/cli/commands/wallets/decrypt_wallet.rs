use crate::cli::build_node;
use anyhow::{anyhow, Result};
use clap::Parser;
use rsnano_core::WalletId;
use rsnano_node::wallets::WalletsExt;

#[derive(Parser)]
#[command(group = clap::ArgGroup::new("input")
    .args(&["data_path", "network"]))]
pub(crate) struct DecryptWalletArgs {
    /// The wallet to be decrypted
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

impl DecryptWalletArgs {
    pub(crate) fn decrypt_wallet(&self) -> Result<()> {
        let node = build_node(&self.data_path, &self.network)?;
        let wallet_id = WalletId::decode_hex(&self.wallet)?;
        let password = self.password.clone().unwrap_or_default();

        node.wallets.ensure_wallet_is_unlocked(wallet_id, &password);

        let seed = node
            .wallets
            .get_seed(wallet_id)
            .map_err(|e| anyhow!("Failed to get wallet seed: {:?}", e))?;

        println!("Seed: {:?}", seed);
        Ok(())
    }
}
