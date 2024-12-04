use add_private_key::AddPrivateKeyArgs;
use anyhow::Result;
use change_wallet_seed::ChangeWalletSeedArgs;
use clap::{CommandFactory, Parser, Subcommand};
use clear_send_ids::ClearSendIdsArgs;
use create_account::CreateAccountArgs;
use create_wallet::CreateWalletArgs;
use decrypt_wallet::DecryptWalletArgs;
use destroy_wallet::DestroyWalletArgs;
use get_wallet_representative::GetWalletRepresentativeArgs;
use import_keys::ImportKeysArgs;
use list_wallets::ListWalletsArgs;
use remove_account::RemoveAccountArgs;
use set_wallet_representative::SetWalletRepresentativeArgs;

pub(crate) mod add_private_key;
pub(crate) mod change_wallet_seed;
pub(crate) mod clear_send_ids;
pub(crate) mod create_account;
pub(crate) mod create_wallet;
pub(crate) mod decrypt_wallet;
pub(crate) mod destroy_wallet;
pub(crate) mod get_wallet_representative;
pub(crate) mod import_keys;
pub(crate) mod list_wallets;
pub(crate) mod remove_account;
pub(crate) mod set_wallet_representative;

#[derive(Subcommand)]
pub(crate) enum WalletSubcommands {
    /// Creates a new account in a wallet
    CreateAccount(CreateAccountArgs),
    /// Creates a new wallet
    CreateWallet(CreateWalletArgs),
    /// Destroys a wallet
    DestroyWallet(DestroyWalletArgs),
    /// Imports keys from a file to a wallet
    ImportKeys(ImportKeysArgs),
    /// Adds a private_key to a wallet
    AddPrivateKey(AddPrivateKeyArgs),
    /// Changes the seed of a wallet
    ChangeWalletSeed(ChangeWalletSeedArgs),
    /// Prints the representative of a wallet
    GetWalletRepresentative(GetWalletRepresentativeArgs),
    /// Sets the representative of a wallet
    SetWalletRepresentative(SetWalletRepresentativeArgs),
    /// Removes an account from a wallet
    RemoveAccount(RemoveAccountArgs),
    /// Decrypts a wallet (WARNING: THIS WILL PRINT YOUR PRIVATE KEY TO STDOUT!)
    DecryptWallet(DecryptWalletArgs),
    /// List all wallets and their public keys
    ListWallets(ListWalletsArgs),
    /// Removes all send IDs from the wallets (dangerous: not intended for production use)
    ClearSendIds(ClearSendIdsArgs),
}

#[derive(Parser)]
pub(crate) struct WalletsCommand {
    #[command(subcommand)]
    pub subcommand: Option<WalletSubcommands>,
}

impl WalletsCommand {
    pub(crate) async fn run(&self) -> Result<()> {
        match &self.subcommand {
            Some(WalletSubcommands::CreateAccount(args)) => args.create_account().await?,
            Some(WalletSubcommands::ListWallets(args)) => args.list_wallets().await?,
            Some(WalletSubcommands::CreateWallet(args)) => args.create_wallet().await?,
            Some(WalletSubcommands::DestroyWallet(args)) => args.destroy_wallet().await?,
            Some(WalletSubcommands::AddPrivateKey(args)) => args.add_key().await?,
            Some(WalletSubcommands::ChangeWalletSeed(args)) => args.change_wallet_seed().await?,
            Some(WalletSubcommands::ImportKeys(args)) => args.import_keys().await?,
            Some(WalletSubcommands::RemoveAccount(args)) => args.remove_account().await?,
            Some(WalletSubcommands::DecryptWallet(args)) => args.decrypt_wallet().await?,
            Some(WalletSubcommands::GetWalletRepresentative(args)) => {
                args.get_wallet_representative().await?
            }
            Some(WalletSubcommands::SetWalletRepresentative(args)) => {
                args.set_representative_wallet().await?
            }
            Some(WalletSubcommands::ClearSendIds(args)) => args.clear_send_ids().await?,
            None => WalletsCommand::command().print_long_help()?,
        }

        Ok(())
    }
}
