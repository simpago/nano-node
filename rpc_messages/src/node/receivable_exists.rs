use crate::{RpcBool, RpcCommand};
use rsnano_core::BlockHash;
use serde::{Deserialize, Serialize};

impl RpcCommand {
    pub fn receivable_exists(args: impl Into<ReceivableExistsArgs>) -> Self {
        Self::ReceivableExists(args.into())
    }
}

impl From<BlockHash> for ReceivableExistsArgs {
    fn from(value: BlockHash) -> Self {
        Self::build(value).finish()
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ReceivableExistsArgs {
    pub hash: BlockHash,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_active: Option<RpcBool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_only_confirmed: Option<RpcBool>,
}

impl ReceivableExistsArgs {
    pub fn build(hash: BlockHash) -> ReceivableExistsArgsBuilder {
        ReceivableExistsArgsBuilder {
            args: ReceivableExistsArgs {
                hash,
                include_active: None,
                include_only_confirmed: None,
            },
        }
    }
}

pub struct ReceivableExistsArgsBuilder {
    args: ReceivableExistsArgs,
}

impl ReceivableExistsArgsBuilder {
    pub fn include_active(mut self) -> Self {
        self.args.include_active = Some(true.into());
        self
    }

    pub fn include_unconfirmed_blocks(mut self) -> Self {
        self.args.include_only_confirmed = Some(false.into());
        self
    }

    pub fn finish(self) -> ReceivableExistsArgs {
        self.args
    }
}

#[cfg(test)]
mod tests {
    use super::{ReceivableExistsArgs, RpcCommand};
    use rsnano_core::BlockHash;
    use serde_json::to_string_pretty;

    #[test]
    fn serialize_receivable_exists_command_basic() {
        let hash = BlockHash::zero();
        assert_eq!(
            to_string_pretty(&RpcCommand::receivable_exists(hash)).unwrap(),
            r#"{
  "action": "receivable_exists",
  "hash": "0000000000000000000000000000000000000000000000000000000000000000"
}"#
        )
    }

    #[test]
    fn serialize_receivable_exists_command_with_options() {
        let hash = BlockHash::zero();
        let args = ReceivableExistsArgs::build(hash)
            .include_active()
            .include_unconfirmed_blocks()
            .finish();
        assert_eq!(
            to_string_pretty(&RpcCommand::receivable_exists(args)).unwrap(),
            r#"{
  "action": "receivable_exists",
  "hash": "0000000000000000000000000000000000000000000000000000000000000000",
  "include_active": "true",
  "include_only_confirmed": "false"
}"#
        )
    }

    #[test]
    fn deserialize_receivable_exists_command_basic() {
        let hash = BlockHash::zero();
        let cmd = RpcCommand::receivable_exists(hash);
        let serialized = serde_json::to_string_pretty(&cmd).unwrap();
        let deserialized: RpcCommand = serde_json::from_str(&serialized).unwrap();
        assert_eq!(cmd, deserialized)
    }

    #[test]
    fn deserialize_receivable_exists_command_with_options() {
        let hash = BlockHash::zero();
        let args = ReceivableExistsArgs::build(hash)
            .include_active()
            .include_unconfirmed_blocks()
            .finish();
        let cmd = RpcCommand::receivable_exists(args);
        let serialized = serde_json::to_string_pretty(&cmd).unwrap();
        let deserialized: RpcCommand = serde_json::from_str(&serialized).unwrap();
        assert_eq!(cmd, deserialized)
    }
}
