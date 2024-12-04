use crate::{common::WorkVersionDto, RpcBool, RpcCommand, RpcF64, RpcU64};
use rsnano_core::{Account, BlockHash, JsonBlock, WorkNonce};
use serde::{Deserialize, Serialize};

impl RpcCommand {
    pub fn work_generate(work_generate_args: WorkGenerateArgs) -> Self {
        Self::WorkGenerate(work_generate_args)
    }
}

impl From<BlockHash> for WorkGenerateArgs {
    fn from(value: BlockHash) -> Self {
        Self::build(value).build()
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct WorkGenerateArgs {
    pub hash: BlockHash,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_peers: Option<RpcBool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub difficulty: Option<RpcU64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub multiplier: Option<RpcU64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account: Option<Account>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<WorkVersionDto>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub block: Option<JsonBlock>,
}

impl WorkGenerateArgs {
    pub fn build(hash: BlockHash) -> WorkGenerateArgsBuilder {
        WorkGenerateArgsBuilder::new(hash)
    }
}

pub struct WorkGenerateArgsBuilder {
    args: WorkGenerateArgs,
}

impl WorkGenerateArgsBuilder {
    pub fn new(hash: BlockHash) -> Self {
        WorkGenerateArgsBuilder {
            args: WorkGenerateArgs {
                hash,
                use_peers: None,
                difficulty: None,
                multiplier: None,
                account: None,
                version: None,
                block: None,
            },
        }
    }

    pub fn use_peers(mut self) -> Self {
        self.args.use_peers = Some(true.into());
        self
    }

    pub fn difficulty(mut self, difficulty: u64) -> Self {
        self.args.difficulty = Some(difficulty.into());
        self
    }

    pub fn multiplier(mut self, multiplier: u64) -> Self {
        self.args.multiplier = Some(multiplier.into());
        self
    }

    pub fn account(mut self, account: Account) -> Self {
        self.args.account = Some(account);
        self
    }

    pub fn version(mut self, version: WorkVersionDto) -> Self {
        self.args.version = Some(version);
        self
    }

    pub fn block(mut self, block: JsonBlock) -> Self {
        self.args.block = Some(block);
        self
    }

    pub fn build(self) -> WorkGenerateArgs {
        self.args
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct WorkGenerateDto {
    pub work: WorkNonce,
    pub difficulty: RpcU64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub multiplier: Option<RpcF64>,
    pub hash: BlockHash,
}

impl WorkGenerateDto {
    pub fn new(work: WorkNonce, difficulty: u64, multiplier: Option<f64>, hash: BlockHash) -> Self {
        Self {
            work,
            difficulty: difficulty.into(),
            multiplier: multiplier.map(|i| i.into()),
            hash,
        }
    }
}
