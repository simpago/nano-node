use anyhow::Result;
use clap::{CommandFactory, Parser, Subcommand};
use commands::{
    config::ConfigCommand, ledger::LedgerCommand, node::NodeCommand, utils::UtilsCommand,
    wallets::WalletsCommand,
};
use rsnano_core::{Networks, PrivateKeyFactory};
use rsnano_node::{config::NetworkConstants, working_path_for, Node, NodeBuilder};
use rsnano_nullable_console::Console;
use std::{path::PathBuf, str::FromStr};

mod commands;

#[derive(Parser)]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,
}

impl Cli {
    pub(crate) async fn run(&self, infra: &mut CliInfrastructure) -> Result<()> {
        match &self.command {
            Some(Commands::Wallets(command)) => command.run()?,
            Some(Commands::Utils(command)) => command.run(infra)?,
            Some(Commands::Node(command)) => command.run().await?,
            Some(Commands::Ledger(command)) => command.run()?,
            Some(Commands::Config(command)) => command.run()?,
            None => Cli::command().print_long_help()?,
        }
        Ok(())
    }
}

#[derive(Subcommand)]
pub(crate) enum Commands {
    /// Commands related to configs
    Config(ConfigCommand),
    /// Commands related to the ledger
    Ledger(LedgerCommand),
    /// Commands related to running the node
    Node(NodeCommand),
    /// Utils related to keys and accounts
    Utils(UtilsCommand),
    /// Commands to manage wallets
    Wallets(WalletsCommand),
}

pub(crate) fn get_path(path_str: &Option<String>, network_str: &Option<String>) -> PathBuf {
    if let Some(path) = path_str {
        return PathBuf::from_str(path).unwrap();
    }
    if let Some(network) = network_str {
        let network = Networks::from_str(&network).unwrap();
        NetworkConstants::set_active_network(network);
    }
    let network = NetworkConstants::active_network();
    working_path_for(network).unwrap()
}

pub(crate) fn get_network(network_str: &Option<String>) -> Networks {
    match network_str {
        None => Networks::NanoLiveNetwork,
        Some(s) => Networks::from_str(s).expect("invalid network"),
    }
}

pub(crate) fn build_node(
    data_path: &Option<String>,
    network: &Option<String>,
) -> anyhow::Result<Node> {
    let network = get_network(network);
    let mut node_builder = NodeBuilder::new(network);
    if let Some(path) = data_path {
        node_builder = node_builder.data_path(path);
    }
    node_builder.finish()
}

#[derive(Default)]
pub(crate) struct CliInfrastructure {
    pub key_factory: PrivateKeyFactory,
    pub console: Console,
}

impl CliInfrastructure {
    pub fn new_null() -> Self {
        Self {
            key_factory: PrivateKeyFactory::new_null(),
            console: Console::new_null(),
        }
    }
}
