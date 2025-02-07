use crate::cli::build_node;
use anyhow::Context;
use anyhow::{anyhow, Result};
use clap::{ArgGroup, Parser};
use rsnano_core::WalletId;
use rsnano_node::wallets::WalletsExt;
use std::{fs::File, io::Read, path::PathBuf};

#[derive(Parser)]
#[command(group = ArgGroup::new("input")
    .args(&["data_path", "network"]))]
pub(crate) struct ImportKeysArgs {
    /// The path of the file that contains the keys
    #[arg(long)]
    file: String,
    #[arg(long)]
    /// Optional password to unlock the wallet
    password: Option<String>,
    #[arg(long)]
    /// Forces the command if the wallet is locked
    force: bool,
    /// The wallet importing the keys
    #[arg(long)]
    wallet: String,
    /// Uses the supplied path as the data directory
    #[arg(long, group = "input")]
    data_path: Option<String>,
    /// Uses the supplied network (live, test, beta or dev)
    #[arg(long, group = "input")]
    network: Option<String>,
}

impl ImportKeysArgs {
    pub(crate) fn import_keys(&self) -> Result<()> {
        let mut file = File::open(PathBuf::from(&self.file))?;
        let mut contents = String::new();

        file.read_to_string(&mut contents)
            .context("Unable to read <file> contents")?;

        let node = build_node(&self.data_path, &self.network)?;
        let wallet_id = WalletId::decode_hex(&self.wallet)?;
        let password = self.password.clone().unwrap_or_default();

        node.wallets.ensure_wallet_is_unlocked(wallet_id, &password);

        if node.wallets.mutex.lock().unwrap().contains_key(&wallet_id) {
            let valid = node.wallets.ensure_wallet_is_unlocked(wallet_id, &password);
            if valid {
                node.wallets
                    .import_replace(wallet_id, &contents, &password)?
            } else {
                eprintln!("Invalid password for wallet {}. New wallet should have empty (default) password or passwords for new wallet & json file should match", wallet_id);
                return Err(anyhow!("Invalid arguments"));
            }
        } else {
            if !self.force {
                eprintln!("Wallet doesn't exist");
                return Err(anyhow!("Invalid arguments"));
            } else {
                node.wallets.import(wallet_id, &contents)?
            }
        }

        Ok(())
    }
}
