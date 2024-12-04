use rsnano_core::Networks;
use rsnano_node::config::{get_rpc_toml_config_path, read_toml_file, NetworkConstants};
use std::{
    net::{AddrParseError, IpAddr, Ipv6Addr, SocketAddr},
    path::Path,
    str::FromStr,
};

use crate::RpcServerToml;

#[derive(Debug, PartialEq, Clone)]
pub struct RpcServerConfig {
    pub address: String,
    pub port: u16,
    pub enable_control: bool,
    pub max_json_depth: u8,
    pub max_request_size: u64,
    pub rpc_logging: RpcServerLoggingConfig,
    pub rpc_process: RpcServerProcessConfig,
}

impl RpcServerConfig {
    pub fn new(network_constants: &NetworkConstants, parallelism: usize) -> Self {
        Self::new2(
            network_constants,
            parallelism,
            network_constants.default_rpc_port,
            false,
        )
    }

    pub fn new2(
        network_constants: &NetworkConstants,
        parallelism: usize,
        port: u16,
        enable_control: bool,
    ) -> Self {
        Self {
            address: Ipv6Addr::LOCALHOST.to_string(),
            port,
            enable_control,
            max_json_depth: 20,
            max_request_size: 32 * 1024 * 1024,
            rpc_logging: RpcServerLoggingConfig::default(),
            rpc_process: RpcServerProcessConfig::new(network_constants, parallelism),
        }
    }

    pub fn default_for(network: Networks, parallelism: usize) -> Self {
        Self::new(&NetworkConstants::for_network(network), parallelism)
    }

    pub fn load_from_data_path(
        network: Networks,
        parallelism: usize,
        data_path: impl AsRef<Path>,
    ) -> anyhow::Result<Self> {
        let file_path = get_rpc_toml_config_path(data_path.as_ref());
        let mut result = Self::default_for(network, parallelism);
        if file_path.exists() {
            let toml: RpcServerToml = read_toml_file(file_path)?;
            result.merge_toml(&toml);
        }
        Ok(result)
    }

    pub fn listening_addr(&self) -> Result<SocketAddr, AddrParseError> {
        let ip_addr = IpAddr::from_str(&self.address)?;
        Ok(SocketAddr::new(ip_addr, self.port))
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct RpcServerLoggingConfig {
    pub log_rpc: bool,
}

impl RpcServerLoggingConfig {
    pub fn new(log_rpc: bool) -> Self {
        Self { log_rpc }
    }
}

impl Default for RpcServerLoggingConfig {
    fn default() -> Self {
        Self::new(true)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct RpcServerProcessConfig {
    pub io_threads: u32,
    pub ipc_address: String,
    pub ipc_port: u16,
    pub num_ipc_connections: u32,
}

impl RpcServerProcessConfig {
    pub fn new(network_constants: &NetworkConstants, parallelism: usize) -> Self {
        Self {
            io_threads: if parallelism > 4 {
                parallelism as u32
            } else {
                4
            },
            ipc_address: Ipv6Addr::LOCALHOST.to_string(),
            ipc_port: network_constants.default_ipc_port,
            num_ipc_connections: if network_constants.is_live_network()
                || network_constants.is_test_network()
            {
                8
            } else if network_constants.is_beta_network() {
                4
            } else {
                1
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use rsnano_node::config::get_default_rpc_filepath_from;
    use std::path::Path;

    #[test]
    fn default_rpc_filepath() {
        assert_eq!(
            get_default_rpc_filepath_from(Path::new("/path/to/nano_node")),
            Path::new("/path/to/nano_rpc")
        );

        assert_eq!(
            get_default_rpc_filepath_from(Path::new("/nano_node")),
            Path::new("/nano_rpc")
        );

        assert_eq!(
            get_default_rpc_filepath_from(Path::new("/bin/nano_node.exe")),
            Path::new("/bin/nano_rpc.exe")
        );
    }
}
