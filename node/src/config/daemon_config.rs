use super::{
    get_node_toml_config_path, read_toml_file, DaemonToml, NetworkParams, NodeConfig,
    NodeRpcConfig, OpenclConfig,
};
use rsnano_core::Networks;
use std::path::Path;

#[derive(Debug, PartialEq)]
pub struct DaemonConfig {
    pub rpc_enable: bool,
    pub rpc: NodeRpcConfig,
    pub node: NodeConfig,
    pub opencl: OpenclConfig,
    pub opencl_enable: bool,
}

impl DaemonConfig {
    pub fn new(network_params: &NetworkParams, parallelism: usize) -> Self {
        Self {
            rpc_enable: false,
            node: NodeConfig::new(
                Some(network_params.network.default_node_port),
                network_params,
                parallelism,
            ),
            opencl: OpenclConfig::new(),
            opencl_enable: false,
            rpc: NodeRpcConfig::new(),
        }
    }

    pub fn new2(network: Networks, parallelism: usize) -> Self {
        Self {
            rpc_enable: false,
            node: NodeConfig::default_for(network, parallelism),
            opencl: OpenclConfig::new(),
            opencl_enable: false,
            rpc: NodeRpcConfig::new(),
        }
    }

    pub fn load_from_data_path(
        network: Networks,
        parallelism: usize,
        data_path: impl AsRef<Path>,
    ) -> anyhow::Result<Self> {
        let file_path = get_node_toml_config_path(data_path.as_ref());
        let mut result = Self::new2(network, parallelism);
        if file_path.exists() {
            let toml: DaemonToml = read_toml_file(file_path)?;
            result.merge_toml(&toml);
        }
        Ok(result)
    }
}
