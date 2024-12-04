use crate::{RpcBool, RpcCommand, RpcU64};
use rsnano_core::{Account, Amount, BlockHash};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

impl RpcCommand {
    pub fn ledger(ledger_args: LedgerArgs) -> Self {
        Self::Ledger(ledger_args)
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Default)]
pub struct LedgerArgs {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account: Option<Account>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub count: Option<RpcU64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub representative: Option<RpcBool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub weight: Option<RpcBool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receivable: Option<RpcBool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modified_since: Option<RpcU64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sorting: Option<RpcBool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threshold: Option<Amount>,
}

impl LedgerArgs {
    pub fn builder() -> LedgerArgsBuilder {
        LedgerArgsBuilder {
            args: LedgerArgs::default(),
        }
    }
}

pub struct LedgerArgsBuilder {
    args: LedgerArgs,
}

impl LedgerArgsBuilder {
    pub fn from_account(mut self, account: Account) -> Self {
        self.args.account = Some(account);
        self
    }

    pub fn count(mut self, count: u64) -> Self {
        self.args.count = Some(count.into());
        self
    }

    pub fn include_representative(mut self) -> Self {
        self.args.representative = Some(true.into());
        self
    }

    pub fn include_weight(mut self) -> Self {
        self.args.weight = Some(true.into());
        self
    }

    pub fn include_receivables(mut self) -> Self {
        self.args.receivable = Some(true.into());
        self
    }

    pub fn modified_since(mut self, modified_since: u64) -> Self {
        self.args.modified_since = Some(modified_since.into());
        self
    }

    pub fn sorted(mut self) -> Self {
        self.args.sorting = Some(true.into());
        self
    }

    pub fn with_minimum_balance(mut self, threshold: Amount) -> Self {
        self.args.threshold = Some(threshold);
        self
    }

    pub fn build(self) -> LedgerArgs {
        self.args
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct LedgerResponse {
    pub accounts: HashMap<Account, LedgerAccountInfo>,
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct LedgerAccountInfo {
    pub frontier: BlockHash,
    pub open_block: BlockHash,
    pub representative_block: BlockHash,
    pub balance: Amount,
    pub modified_timestamp: RpcU64,
    pub block_count: RpcU64,
    pub representative: Option<Account>,
    pub weight: Option<Amount>,
    pub pending: Option<Amount>,
    pub receivable: Option<Amount>,
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::{
        ledger::{LedgerAccountInfo, LedgerArgs, LedgerResponse},
        RpcCommand,
    };
    use rsnano_core::{Account, Amount, BlockHash};
    use serde_json::json;

    #[test]
    fn test_ledger_rpc_command_serialization() {
        let account = Account::decode_account(
            "nano_1ipx847tk8o46pwxt5qjdbncjqcbwcc1rrmqnkztrfjy5k7z4imsrata9est",
        )
        .unwrap();
        let ledger_args = LedgerArgs::builder()
            .from_account(account)
            .count(1000)
            .include_representative()
            .include_weight()
            .include_receivables()
            .modified_since(1234567890)
            .sorted()
            .with_minimum_balance(Amount::raw(1000000000000000000000000000000u128))
            .build();

        let rpc_command = RpcCommand::Ledger(ledger_args);

        let serialized = serde_json::to_value(&rpc_command).unwrap();

        let expected = json!({
            "action": "ledger",
            "account": "nano_1ipx847tk8o46pwxt5qjdbncjqcbwcc1rrmqnkztrfjy5k7z4imsrata9est",
            "count": "1000",
            "representative": "true",
            "weight": "true",
            "receivable": "true",
            "modified_since": "1234567890",
            "sorting": "true",
            "threshold": "1000000000000000000000000000000"
        });

        assert_eq!(serialized, expected);
    }

    #[test]
    fn test_ledger_rpc_command_deserialization() {
        let json_str = r#"{
            "action": "ledger",
            "account": "nano_1ipx847tk8o46pwxt5qjdbncjqcbwcc1rrmqnkztrfjy5k7z4imsrata9est",
            "count": "1000",
            "representative": "true",
            "weight": "true",
            "pending": "true",
            "receivable": "true",
            "modified_since": "1234567890",
            "sorting": "true",
            "threshold": "1000000000000000000000000000000"
        }"#;

        let deserialized: RpcCommand = serde_json::from_str(json_str).unwrap();

        match deserialized {
            RpcCommand::Ledger(args) => {
                assert_eq!(
                    args.account,
                    Some(
                        Account::decode_account(
                            "nano_1ipx847tk8o46pwxt5qjdbncjqcbwcc1rrmqnkztrfjy5k7z4imsrata9est"
                        )
                        .unwrap()
                    )
                );
                assert_eq!(args.count, Some(1000.into()));
                assert_eq!(args.representative, Some(true.into()));
                assert_eq!(args.weight, Some(true.into()));
                assert_eq!(args.receivable, Some(true.into()));
                assert_eq!(args.modified_since, Some(1234567890.into()));
                assert_eq!(args.sorting, Some(true.into()));
                assert_eq!(
                    args.threshold,
                    Some(Amount::raw(1000000000000000000000000000000u128))
                );
            }
            _ => panic!("Deserialized to wrong variant"),
        }
    }

    #[test]
    fn test_ledger_dto_serialization() {
        let mut accounts = HashMap::new();
        accounts.insert(
            Account::decode_account(
                "nano_1ipx847tk8o46pwxt5qjdbncjqcbwcc1rrmqnkztrfjy5k7z4imsrata9est",
            )
            .unwrap(),
            LedgerAccountInfo {
                frontier: BlockHash::decode_hex(
                    "000D1BAEC8EC208142C99059B393051BAC8380F9B5A2E6B2489A277D81789F3F",
                )
                .unwrap(),
                open_block: BlockHash::decode_hex(
                    "991CF190094C00F0B68E2E5F75F6BEE95A2E0BD93CEAA4A6734DB9F19C34F1ED",
                )
                .unwrap(),
                representative_block: BlockHash::decode_hex(
                    "991CF190094C00F0B68E2E5F75F6BEE95A2E0BD93CEAA4A6734DB9F19C34F1ED",
                )
                .unwrap(),
                balance: Amount::raw(10000000000000000000000000000000u128),
                modified_timestamp: 1553174994.into(),
                block_count: 50.into(),
                representative: Some(
                    Account::decode_account(
                        "nano_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3",
                    )
                    .unwrap(),
                ),
                weight: Some(Amount::raw(10000000000000000000000000000000u128)),
                pending: Some(Amount::raw(10000000000000000000000000000u128)),
                receivable: Some(Amount::raw(10000000000000000000000000000u128)),
            },
        );

        let ledger_dto = LedgerResponse { accounts };

        let serialized = serde_json::to_value(&ledger_dto).unwrap();

        let expected = json!({
            "accounts": {
                "nano_1ipx847tk8o46pwxt5qjdbncjqcbwcc1rrmqnkztrfjy5k7z4imsrata9est": {
                    "frontier": "000D1BAEC8EC208142C99059B393051BAC8380F9B5A2E6B2489A277D81789F3F",
                    "open_block": "991CF190094C00F0B68E2E5F75F6BEE95A2E0BD93CEAA4A6734DB9F19C34F1ED",
                    "representative_block": "991CF190094C00F0B68E2E5F75F6BEE95A2E0BD93CEAA4A6734DB9F19C34F1ED",
                    "balance": "10000000000000000000000000000000",
                    "modified_timestamp": "1553174994",
                    "block_count": "50",
                    "representative": "nano_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3",
                    "weight": "10000000000000000000000000000000",
                    "pending": "10000000000000000000000000000",
                    "receivable": "10000000000000000000000000000"
                }
            }
        });

        assert_eq!(serialized, expected);
    }

    #[test]
    fn test_ledger_dto_deserialization() {
        let json_str = r#"{
            "accounts": {
                "nano_1ipx847tk8o46pwxt5qjdbncjqcbwcc1rrmqnkztrfjy5k7z4imsrata9est": {
                    "frontier": "000D1BAEC8EC208142C99059B393051BAC8380F9B5A2E6B2489A277D81789F3F",
                    "open_block": "991CF190094C00F0B68E2E5F75F6BEE95A2E0BD93CEAA4A6734DB9F19C34F1ED",
                    "representative_block": "991CF190094C00F0B68E2E5F75F6BEE95A2E0BD93CEAA4A6734DB9F19C34F1ED",
                    "balance": "10000000000000000000000000000000",
                    "modified_timestamp": "1553174994",
                    "block_count": "50",
                    "representative": "nano_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3",
                    "weight": "10000000000000000000000000000000",
                    "pending": "10000000000000000000000000000",
                    "receivable": "10000000000000000000000000000"
                }
            }
        }"#;

        let deserialized: LedgerResponse = serde_json::from_str(json_str).unwrap();

        assert_eq!(deserialized.accounts.len(), 1);

        let account = Account::decode_account(
            "nano_1ipx847tk8o46pwxt5qjdbncjqcbwcc1rrmqnkztrfjy5k7z4imsrata9est",
        )
        .unwrap();
        let account_info = deserialized.accounts.get(&account).unwrap();

        assert_eq!(
            account_info.frontier,
            BlockHash::decode_hex(
                "000D1BAEC8EC208142C99059B393051BAC8380F9B5A2E6B2489A277D81789F3F"
            )
            .unwrap()
        );
        assert_eq!(
            account_info.open_block,
            BlockHash::decode_hex(
                "991CF190094C00F0B68E2E5F75F6BEE95A2E0BD93CEAA4A6734DB9F19C34F1ED"
            )
            .unwrap()
        );
        assert_eq!(
            account_info.representative_block,
            BlockHash::decode_hex(
                "991CF190094C00F0B68E2E5F75F6BEE95A2E0BD93CEAA4A6734DB9F19C34F1ED"
            )
            .unwrap()
        );
        assert_eq!(
            account_info.balance,
            Amount::raw(10000000000000000000000000000000u128)
        );
        assert_eq!(account_info.modified_timestamp, 1553174994.into());
        assert_eq!(account_info.block_count, 50.into());
        assert_eq!(
            account_info.representative,
            Some(
                Account::decode_account(
                    "nano_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3"
                )
                .unwrap()
            )
        );
        assert_eq!(
            account_info.weight,
            Some(Amount::raw(10000000000000000000000000000000u128))
        );
        assert_eq!(
            account_info.pending,
            Some(Amount::raw(10000000000000000000000000000u128))
        );
        assert_eq!(
            account_info.receivable,
            Some(Amount::raw(10000000000000000000000000000u128))
        );
    }
}
