use account_count::AccountCountArgs;
use anyhow::Result;
use block_count::BlockCountArgs;
use blocks::Blocks;
use cemented_block_count::CementedBlockCountArgs;
use clap::{CommandFactory, Parser, Subcommand};
use peers::PeersArgs;

pub(crate) mod account_count;
pub(crate) mod block_count;
pub(crate) mod blocks;
pub(crate) mod cemented_block_count;
pub(crate) mod peers;

#[derive(Subcommand)]
pub(crate) enum InfoSubcommands {
    /// Displays the number of accounts
    AccountCount(AccountCountArgs),
    /// Displays the number of blocks
    BlockCount(BlockCountArgs),
    /// Displays all the blocks in the ledger in text format
    Blocks(Blocks),
    /// Displays peer IPv6:port connections
    Peers(PeersArgs),
    /// Displays the number of cemented (confirmed) blocks
    CementedBlockCount(CementedBlockCountArgs),
}

#[derive(Parser)]
pub(crate) struct InfoCommand {
    #[command(subcommand)]
    pub subcommand: Option<InfoSubcommands>,
}

impl InfoCommand {
    pub(crate) fn run(&self) -> Result<()> {
        match &self.subcommand {
            Some(InfoSubcommands::AccountCount(args)) => args.account_count()?,
            Some(InfoSubcommands::BlockCount(args)) => args.block_count()?,
            Some(InfoSubcommands::Blocks(args)) => args.blocks()?,
            Some(InfoSubcommands::CementedBlockCount(args)) => args.cemented_block_count()?,
            Some(InfoSubcommands::Peers(args)) => args.peers()?,
            None => InfoCommand::command().print_long_help()?,
        }

        Ok(())
    }
}
