use anyhow::Result;
use clap::{CommandFactory, Parser, Subcommand};
use generate_config::GenerateConfigArgs;
use initialize::InitializeArgs;
use rsnano_node::{BUILD_INFO, VERSION_STRING};
use run_daemon::RunDaemonArgs;

pub(crate) mod generate_config;
pub(crate) mod initialize;
pub(crate) mod run_daemon;

#[derive(Subcommand)]
pub(crate) enum NodeSubcommands {
    /// Start node daemon.
    Run(RunDaemonArgs),
    /// Initialize the data folder, if it is not already initialised.
    ///
    /// This command is meant to be run when the data folder is empty, to populate it with the genesis block.
    Initialize(InitializeArgs),
    /// Prints out version.
    Version,
    /// Writes node or rpc configuration to stdout, populated with defaults suitable for this system.
    ///
    /// Pass the configuration type node or rpc.
    /// See also use_defaults.
    GenerateConfig(GenerateConfigArgs),
}

#[derive(Parser)]
pub(crate) struct NodeCommand {
    #[command(subcommand)]
    pub subcommand: Option<NodeSubcommands>,
}

impl NodeCommand {
    pub(crate) fn run(&self) -> Result<()> {
        match &self.subcommand {
            Some(NodeSubcommands::Run(args)) => args.run_daemon()?,
            Some(NodeSubcommands::Initialize(args)) => args.initialize()?,
            Some(NodeSubcommands::GenerateConfig(args)) => args.generate_config()?,
            Some(NodeSubcommands::Version) => Self::version(),
            None => NodeCommand::command().print_long_help()?,
        }

        Ok(())
    }

    fn version() {
        println!("Version {}", VERSION_STRING);
        println!("Build Info {}", BUILD_INFO);
    }
}
