use crate::cli::build_node;
use anyhow::{anyhow, Result};
use clap::{ArgGroup, Parser};
use rand::Rng;
use rsnano_core::{RawKey, WalletId};
use rsnano_node::wallets::WalletsExt;

#[derive(Parser)]
#[command(group = ArgGroup::new("input")
    .args(&["data_path", "network"]))]
pub(crate) struct CreateWalletArgs {
    /// Optional seed of the new wallet
    #[arg(long)]
    seed: Option<String>,
    /// Optional password of the new wallet
    #[arg(long)]
    password: Option<String>,
    /// Uses the supplied path as the data directory
    #[arg(long, group = "input")]
    data_path: Option<String>,
    /// Uses the supplied network (live, test, beta or dev)
    #[arg(long, group = "input")]
    network: Option<String>,
}

impl CreateWalletArgs {
    pub(crate) fn create_wallet(&self) -> Result<()> {
        let node = build_node(&self.data_path, &self.network)?;
        let wallet_id = WalletId::from_bytes(rand::rng().random());

        node.wallets.create(wallet_id);
        println!("{:?}", wallet_id);

        let password = self.password.clone().unwrap_or_default();

        node.wallets
            .rekey(&wallet_id, &password)
            .map_err(|e| anyhow!("Failed to set wallet password: {:?}", e))?;

        node.wallets.ensure_wallet_is_unlocked(wallet_id, &password);

        if let Some(seed) = &self.seed {
            let key = RawKey::decode_hex(seed)?;

            node.wallets
                .change_seed(wallet_id, &key, 0)
                .map_err(|e| anyhow!("Failed to set wallet seed: {:?}", e))?;
        }

        Ok(())
    }
}
