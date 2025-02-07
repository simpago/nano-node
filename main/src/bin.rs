mod cli;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, CliInfrastructure};

fn main() -> Result<()> {
    let cli = Cli::parse();
    let mut infra = CliInfrastructure::default();
    cli.run(&mut infra)?;
    Ok(())
}
