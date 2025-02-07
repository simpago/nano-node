use crate::cli::build_node;
use anyhow::{anyhow, Result};
use clap::{ArgGroup, Parser};
use rsnano_core::{RawKey, WalletId};
use rsnano_node::wallets::WalletsExt;

#[derive(Parser)]
#[command(group = ArgGroup::new("input")
    .args(&["data_path", "network"]))]
pub(crate) struct ChangeWalletSeedArgs {
    /// Changes the seed of the supplied wallet
    #[arg(long)]
    wallet: String,
    /// The new <seed> of the wallet
    #[arg(long)]
    seed: String,
    /// Optional <password> to unlock the wallet
    #[arg(long)]
    password: Option<String>,
    /// Uses the supplied path as the data directory
    #[arg(long, group = "input")]
    data_path: Option<String>,
    /// Uses the supplied network (live, test, beta or dev)
    #[arg(long, group = "input")]
    network: Option<String>,
}

impl ChangeWalletSeedArgs {
    pub(crate) fn change_wallet_seed(&self) -> Result<()> {
        let node = build_node(&self.data_path, &self.network)?;
        let wallet_id = WalletId::decode_hex(&self.wallet)?;
        let seed = RawKey::decode_hex(&self.seed)?;
        let password = self.password.clone().unwrap_or_default();

        node.wallets.ensure_wallet_is_unlocked(wallet_id, &password);

        node.wallets
            .change_seed(wallet_id, &seed, 0)
            .map_err(|e| anyhow!("Failed to change wallet seed: {:?}", e))?;

        Ok(())
    }
}
