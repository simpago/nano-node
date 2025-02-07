use crate::cli::build_node;
use anyhow::{anyhow, Result};
use clap::{ArgGroup, Parser};
use rsnano_core::{RawKey, WalletId};
use rsnano_node::wallets::WalletsExt;

#[derive(Parser)]
#[command(group = ArgGroup::new("input")
    .args(&["data_path", "network"]))]
pub(crate) struct AddPrivateKeyArgs {
    /// Adds the key to the supplied wallet
    #[arg(long)]
    wallet: String,
    /// Adds the supplied <private_key> to the wallet
    #[arg(long)]
    private_key: String,
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

impl AddPrivateKeyArgs {
    pub(crate) fn add_key(&self) -> Result<()> {
        let node = build_node(&self.data_path, &self.network)?;
        let wallet_id = WalletId::decode_hex(&self.wallet)?;
        let public_key = RawKey::decode_hex(&self.private_key)?;
        let password = self.password.clone().unwrap_or_default();

        node.wallets.ensure_wallet_is_unlocked(wallet_id, &password);

        node.wallets
            .insert_adhoc2(&wallet_id, &public_key, false)
            .map_err(|e| anyhow!("Failed to insert key: {:?}", e))?;

        Ok(())
    }
}
