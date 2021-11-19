use std::path::PathBuf;
use anyhow::Result;

use super::get_default_rpc_filepath;

pub struct RpcChildProcessConfig{
    pub enable:bool,
    pub rpc_path: PathBuf,
}

impl RpcChildProcessConfig{
    pub fn new() -> Result<Self>{
        Ok(Self{
            enable: false,
            rpc_path: get_default_rpc_filepath()?,
        })
    }
}

pub struct NodeRpcConfig{
    pub enable_sign_hash: bool,
    pub child_process: RpcChildProcessConfig,
}

impl NodeRpcConfig{
    pub fn new() -> Result<Self> {
        Ok(Self {
            enable_sign_hash: false,
            child_process: RpcChildProcessConfig::new()?,
        })
    }
}