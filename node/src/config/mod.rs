mod converters;
mod daemon_config;
mod diagnostics_config;
mod network_constants;
mod network_params;
mod node_config;
mod node_flags;
mod node_rpc_config;
mod opencl_config;
mod toml;
mod websocket_config;

pub use daemon_config::*;
pub use diagnostics_config::*;
pub use network_constants::*;
pub use network_params::*;
pub use node_config::*;
pub use node_flags::*;
pub use node_rpc_config::*;
pub use opencl_config::*;
pub use rsnano_core::Networks;
use serde::de::DeserializeOwned;
use std::path::{Path, PathBuf};
pub use toml::DaemonToml;
pub use websocket_config::WebsocketConfig;

pub fn get_node_toml_config_path(data_path: impl Into<PathBuf>) -> PathBuf {
    let mut node_toml = data_path.into();
    node_toml.push("config-node.toml");
    node_toml
}

pub fn get_rpc_toml_config_path(data_path: impl Into<PathBuf>) -> PathBuf {
    let mut rpc_toml = data_path.into();
    rpc_toml.push("config-rpc.toml");
    rpc_toml
}

pub fn get_default_rpc_filepath() -> PathBuf {
    get_default_rpc_filepath_from(std::env::current_exe().unwrap_or_default().as_path())
}

pub fn get_default_rpc_filepath_from(node_exe_path: &Path) -> PathBuf {
    let mut result = node_exe_path.to_path_buf();
    result.pop();
    result.push("nano_rpc");
    if let Some(ext) = node_exe_path.extension() {
        result.set_extension(ext);
    }
    result
}

pub fn force_nano_dev_network() {
    NetworkConstants::set_active_network(Networks::NanoDevNetwork);
}

pub struct GlobalConfig {
    pub node_config: NodeConfig,
    pub flags: NodeFlags,
    pub network_params: NetworkParams,
}

pub fn read_toml_file<T: DeserializeOwned>(path: impl AsRef<Path>) -> anyhow::Result<T> {
    let toml_str = std::fs::read_to_string(path)?;
    ::toml::from_str(&toml_str).map_err(|e| e.into())
}
