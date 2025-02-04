use crate::cli::get_path;
use anyhow::Result;
use clap::{ArgGroup, Parser};
use rsnano_core::{utils::get_cpu_count, Networks};
use rsnano_node::config::{
    get_node_toml_config_path, get_rpc_toml_config_path, DaemonConfig, DaemonToml,
    NetworkConstants, NetworkParams,
};
use rsnano_rpc_server::{RpcServerConfig, RpcServerToml};
use std::fs::read_to_string;
use toml::{from_str, to_string};

#[derive(Parser)]
#[command(group = ArgGroup::new("input1")
    .args(&["node", "rpc"])
    .required(true))]
#[command(group = ArgGroup::new("input2")
    .args(&["data_path", "network"]))]
pub(crate) struct CurrentArgs {
    /// Prints the current node config
    #[arg(long, group = "input1")]
    node: bool,
    /// Prints the current rpc config
    #[arg(long, group = "input1")]
    rpc: bool,
    /// Uses the supplied path as the data directory
    #[arg(long, group = "input2")]
    data_path: Option<String>,
    /// Uses the supplied network (live, test, beta or dev)
    #[arg(long, group = "input2")]
    network: Option<String>,
}

impl CurrentArgs {
    pub(crate) fn current(&self) -> Result<()> {
        let path = get_path(&self.data_path, &self.network);
        let network = Networks::NanoBetaNetwork;
        let network_params = NetworkParams::new(network);
        let parallelism = get_cpu_count();

        if self.node {
            let node_toml_config_path = get_node_toml_config_path(path);

            if node_toml_config_path.exists() {
                let daemon_toml_str = read_to_string(&node_toml_config_path)?;

                let current_daemon_toml: DaemonToml = from_str(&daemon_toml_str)?;

                let mut default_daemon_config = DaemonConfig::new(&network_params, parallelism);

                default_daemon_config.merge_toml(&current_daemon_toml);

                let merged_daemon_toml: DaemonToml = (&default_daemon_config).into();

                println!("{}", to_string(&merged_daemon_toml).unwrap());
            }
        } else {
            let rpc_toml_config_path = get_rpc_toml_config_path(path);

            if rpc_toml_config_path.exists() {
                let rpc_toml_str = read_to_string(&rpc_toml_config_path)?;

                let current_rpc_toml: RpcServerToml = from_str(&rpc_toml_str)?;

                let mut default_rpc_config =
                    RpcServerConfig::new(&NetworkConstants::for_beta(), parallelism);

                default_rpc_config.merge_toml(&current_rpc_toml);

                let merged_rpc_toml: RpcServerToml = (&default_rpc_config).into();

                println!("{}", to_string(&merged_rpc_toml).unwrap());
            }
        }

        Ok(())
    }
}
