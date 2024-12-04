use crate::{common::HashesArgs, RpcCommand};
use rsnano_core::{BlockHash, JsonBlock};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

impl RpcCommand {
    pub fn blocks(hashes: Vec<BlockHash>) -> Self {
        Self::Blocks(HashesArgs::new(hashes))
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct BlocksResponse {
    pub blocks: HashMap<BlockHash, JsonBlock>,
}

impl BlocksResponse {
    pub fn new(blocks: HashMap<BlockHash, JsonBlock>) -> Self {
        Self { blocks }
    }
}
