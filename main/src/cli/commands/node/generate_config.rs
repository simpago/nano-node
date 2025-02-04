use anyhow::Result;
use clap::{ArgGroup, Parser};
use rsnano_core::{utils::get_cpu_count, Networks};
use rsnano_node::config::{DaemonConfig, DaemonToml, NetworkParams};
use rsnano_rpc_server::{RpcServerConfig, RpcServerToml};
use std::io::BufRead;

#[derive(Parser)]
#[command(group = ArgGroup::new("input1")
    .args(&["node", "rpc"])
    .required(true))]
pub(crate) struct GenerateConfigArgs {
    /// Generates the node config
    #[arg(long, group = "input1")]
    node: bool,
    /// Generates the rpc config
    #[arg(long, group = "input1")]
    rpc: bool,
    /// Uncomments the entries of the config
    #[arg(long)]
    use_defaults: bool,
}

impl GenerateConfigArgs {
    pub(crate) fn generate_config(&self) -> Result<()> {
        let (toml_str, config_type) = if self.node {
            let daemon_toml: DaemonToml = (&DaemonConfig::new(
                &NetworkParams::new(Networks::NanoBetaNetwork),
                get_cpu_count(),
            ))
                .into();
            (toml::to_string(&daemon_toml)?, "node")
        } else {
            let rpc_server_toml: RpcServerToml =
                (&RpcServerConfig::default_for(Networks::NanoBetaNetwork, get_cpu_count())).into();
            (toml::to_string(&rpc_server_toml)?, "rpc")
        };

        println!("# This is an example configuration file for Nano. Visit https://docs.nano.org/running-a-node/configuration/ for more information.");
        println!("# Fields may need to be defined in the context of a [category] above them.");
        println!("# The desired configuration changes should be placed in config-{}.toml in the node data path.", config_type);
        println!(
            "# To change a value from its default, uncomment (erasing #) the corresponding field."
        );
        println!("# It is not recommended to uncomment every field, as the default value for important fields may change in the future. Only change what you need.");
        println!("# Additional information for notable configuration options is available in https://docs.nano.org/running-a-node/configuration/#notable-configuration-options\n");

        if self.use_defaults {
            println!("{}", with_comments(&toml_str, false));
        } else {
            println!("{}", with_comments(&toml_str, true));
        }

        Ok(())
    }
}

fn with_comments(toml_string: &String, comment_values: bool) -> String {
    let mut ss_processed = String::new();

    let reader = std::io::BufReader::new(toml_string.as_bytes());

    for line in reader.lines() {
        let mut line = line.unwrap();
        if !line.is_empty() && !line.starts_with('[') {
            if line.starts_with('#') {
                line = format!("\t{}", line);
            } else {
                line = if comment_values {
                    format!("\t# {}", line)
                } else {
                    format!("\t{}", line)
                };
            }
        }
        ss_processed.push_str(&line);
        ss_processed.push('\n');
    }

    ss_processed
}
