use crate::{RpcBool, RpcCommand, RpcU16, RpcU64};
use rsnano_core::{Account, Amount, BlockHash};
use serde::{Deserialize, Serialize};

impl RpcCommand {
    pub fn account_info(account_info_args: AccountInfoArgs) -> Self {
        Self::AccountInfo(account_info_args)
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct AccountInfoArgs {
    pub account: Account,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub representative: Option<RpcBool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub weight: Option<RpcBool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receivable: Option<RpcBool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub include_confirmed: Option<RpcBool>,
}

impl AccountInfoArgs {
    pub fn new(account: Account) -> AccountInfoArgs {
        AccountInfoArgs {
            account,
            representative: None,
            weight: None,
            receivable: None,
            include_confirmed: None,
        }
    }

    pub fn build(account: Account) -> AccountInfoArgsBuilder {
        AccountInfoArgsBuilder::new(account)
    }
}

impl From<Account> for AccountInfoArgs {
    fn from(account: Account) -> Self {
        Self {
            account,
            representative: None,
            weight: None,
            receivable: None,
            include_confirmed: None,
        }
    }
}

pub struct AccountInfoArgsBuilder {
    args: AccountInfoArgs,
}

impl AccountInfoArgsBuilder {
    fn new(account: Account) -> Self {
        Self {
            args: AccountInfoArgs {
                account,
                representative: None,
                weight: None,
                receivable: None,
                include_confirmed: None,
            },
        }
    }

    pub fn include_representative(mut self) -> Self {
        self.args.representative = Some(true.into());
        self
    }

    pub fn include_weight(mut self) -> Self {
        self.args.weight = Some(true.into());
        self
    }

    pub fn include_pending(mut self) -> Self {
        self.args.receivable = Some(true.into());
        self
    }

    pub fn include_receivable(mut self) -> Self {
        self.args.receivable = Some(true.into());
        self
    }

    pub fn include_confirmed(mut self) -> Self {
        self.args.include_confirmed = Some(true.into());
        self
    }

    pub fn finish(self) -> AccountInfoArgs {
        self.args
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct AccountInfoResponse {
    pub frontier: BlockHash,
    pub open_block: BlockHash,
    pub representative_block: BlockHash,
    pub balance: Amount,
    pub modified_timestamp: RpcU64,
    pub block_count: RpcU64,
    pub account_version: RpcU16,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confirmation_height: Option<RpcU64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confirmed_height: Option<RpcU64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confirmed_frontier: Option<BlockHash>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confirmation_height_frontier: Option<BlockHash>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub representative: Option<Account>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub weight: Option<Amount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pending: Option<Amount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receivable: Option<Amount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confirmed_balance: Option<Amount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confirmed_pending: Option<Amount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confirmed_receivable: Option<Amount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub confirmed_representative: Option<Account>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{from_str, to_string_pretty};

    #[test]
    fn test_account_info_args_builder() {
        let account = Account::from(123);
        let args = AccountInfoArgs::build(account)
            .include_weight()
            .include_pending()
            .include_confirmed()
            .finish();

        assert_eq!(args.account, account);
        assert_eq!(args.representative, None);
        assert_eq!(args.weight, Some(true.into()));
        assert_eq!(args.receivable, Some(true.into()));
        assert_eq!(args.include_confirmed, Some(true.into()));
    }

    #[test]
    fn serialize_account_info_command_with_optionals() {
        let account = Account::from(123);
        let args = AccountInfoArgs::build(account)
            .include_representative()
            .include_weight()
            .include_pending()
            .include_receivable()
            .include_confirmed()
            .finish();

        let serialized = to_string_pretty(&RpcCommand::account_info(args)).unwrap();

        assert!(serialized.contains(r#""action": "account_info""#));
        assert!(serialized.contains(
            r#""account": "nano_111111111111111111111111111111111111111111111111115uwdgas549""#
        ));
        assert!(serialized.contains(r#""representative": "true""#));
        assert!(serialized.contains(r#""weight": "true""#));
        assert!(serialized.contains(r#""receivable": "true""#));
        assert!(serialized.contains(r#""include_confirmed": "true""#));
    }

    #[test]
    fn deserialize_account_info_command_with_optionals() {
        let json = r#"{
            "action": "account_info",
            "account": "nano_111111111111111111111111111111111111111111111111115uwdgas549",
            "representative": "true",
            "weight": "true",
            "receivable": "true",
            "include_confirmed": "true"
        }"#;

        let deserialized: RpcCommand = from_str(json).unwrap();

        if let RpcCommand::AccountInfo(args) = deserialized {
            assert_eq!(args.account, Account::from(123));
            assert_eq!(args.representative, Some(true.into()));
            assert_eq!(args.weight, Some(true.into()));
            assert_eq!(args.receivable, Some(true.into()));
            assert_eq!(args.include_confirmed, Some(true.into()));
        } else {
            panic!("Deserialized to wrong RpcCommand variant");
        }
    }

    #[test]
    fn serialize_account_info_command_without_optionals() {
        let account = Account::from(123);
        let args = AccountInfoArgs::build(account).finish();

        assert_eq!(
            serde_json::to_string_pretty(&RpcCommand::account_info(args)).unwrap(),
            r#"{
  "action": "account_info",
  "account": "nano_111111111111111111111111111111111111111111111111115uwdgas549"
}"#
        )
    }

    #[test]
    fn derialize_account_info_command_without_optionals() {
        let account = Account::from(123);
        let args = AccountInfoArgs::build(account).finish();
        let cmd = RpcCommand::account_info(args);
        let serialized = to_string_pretty(&cmd).unwrap();
        let deserialized: RpcCommand = from_str(&serialized).unwrap();

        assert_eq!(cmd, deserialized)
    }

    #[test]
    fn serialize_account_info_args() {
        let args = AccountInfoArgs::build(Account::zero())
            .include_representative()
            .include_weight()
            .include_receivable()
            .finish();

        let serialized = to_string_pretty(&args).unwrap();
        let deserialized: AccountInfoArgs = from_str(&serialized).unwrap();

        assert!(serialized.contains("account"));
        assert!(serialized.contains("representative"));
        assert!(serialized.contains("weight"));
        assert!(serialized.contains("receivable"));
        assert!(!serialized.contains("pending"));
        assert!(!serialized.contains("include_confirmed"));
        assert_eq!(args, deserialized);
    }

    #[test]
    fn serialize_account_info_command_with_some_args() {
        let args = AccountInfoArgs::build(Account::zero())
            .include_representative()
            .include_weight()
            .include_receivable()
            .finish();

        let command = RpcCommand::account_info(args);
        let serialized = to_string_pretty(&command).unwrap();

        assert!(serialized.contains("account"));
        assert!(serialized.contains("representative"));
        assert!(serialized.contains("weight"));
        assert!(serialized.contains("receivable"));
        assert!(!serialized.contains("include_confirmed"));
    }

    #[test]
    fn serialize_account_info_dto_with_none_values() {
        let account_info = AccountInfoResponse {
            frontier: BlockHash::zero(),
            open_block: BlockHash::zero(),
            representative_block: BlockHash::zero(),
            balance: Amount::raw(1000),
            modified_timestamp: 1234567890.into(),
            block_count: 100.into(),
            account_version: 1.into(),
            confirmed_height: Some(99.into()),
            confirmation_height_frontier: Some(BlockHash::zero()),
            representative: Some(Account::zero()),
            weight: Some(Amount::raw(2000)),
            pending: Some(Amount::raw(300)),
            receivable: Some(Amount::raw(400)),
            confirmed_balance: None,
            confirmed_pending: None,
            confirmed_receivable: None,
            confirmed_representative: None,
            confirmed_frontier: None,
            confirmation_height: None,
        };

        let serialized = to_string_pretty(&account_info).unwrap();
        let deserialized: AccountInfoResponse = from_str(&serialized).unwrap();

        assert_eq!(account_info, deserialized);
    }

    #[test]
    fn deserialize_account_info_dto_with_none_values() {
        let account_info = AccountInfoResponse {
            frontier: BlockHash::zero(),
            open_block: BlockHash::zero(),
            representative_block: BlockHash::zero(),
            balance: Amount::raw(1000),
            modified_timestamp: 1234567890.into(),
            block_count: 100.into(),
            account_version: 1.into(),
            confirmed_height: None,
            confirmation_height_frontier: None,
            representative: None,
            weight: None,
            pending: None,
            receivable: None,
            confirmed_balance: None,
            confirmed_pending: None,
            confirmed_receivable: None,
            confirmed_representative: None,
            confirmed_frontier: None,
            confirmation_height: None,
        };

        let serialized = to_string_pretty(&account_info).unwrap();
        let deserialized: AccountInfoResponse = from_str(&serialized).unwrap();

        assert_eq!(account_info, deserialized);
        assert!(!serialized.contains("weight"));
        assert!(!serialized.contains("pending"));
        assert!(!serialized.contains("receivable"));
        assert!(!serialized.contains("confirmed_balance"));
        assert!(!serialized.contains("confirmed_pending"));
        assert!(!serialized.contains("confirmed_receivable"));
        assert!(!serialized.contains("confirmed_representative"));
    }

    fn create_account_info_dto_with_some_values() -> AccountInfoResponse {
        AccountInfoResponse {
            frontier: BlockHash::zero(),
            open_block: BlockHash::zero(),
            representative_block: BlockHash::zero(),
            balance: Amount::from(1000),
            modified_timestamp: 1234567890.into(),
            block_count: 100.into(),
            account_version: 1.into(),
            confirmed_height: Some(99.into()),
            confirmation_height_frontier: Some(BlockHash::zero()),
            representative: Some(Account::zero()),
            weight: Some(Amount::from(2000)),
            pending: Some(Amount::from(300)),
            receivable: Some(Amount::from(400)),
            confirmed_balance: Some(Amount::from(950)),
            confirmed_pending: Some(Amount::from(250)),
            confirmed_receivable: Some(Amount::from(350)),
            confirmed_representative: Some(Account::zero()),
            confirmed_frontier: None,
            confirmation_height: None,
        }
    }

    #[test]
    fn serialize_account_info_dto_with_some_values() {
        let account_info = create_account_info_dto_with_some_values();
        let serialized = to_string_pretty(&account_info).unwrap();

        assert!(serialized.contains("frontier"));
        assert!(serialized.contains("representative"));
        assert!(serialized.contains("weight"));
        assert!(serialized.contains("pending"));
        assert!(serialized.contains("receivable"));
        assert!(serialized.contains("confirmed_balance"));
        assert!(serialized.contains("confirmed_pending"));
        assert!(serialized.contains("confirmed_receivable"));
        assert!(serialized.contains("confirmed_representative"));
    }

    #[test]
    fn deserialize_account_info_dto_with_some_values() {
        let account_info = create_account_info_dto_with_some_values();
        let serialized = to_string_pretty(&account_info).unwrap();
        let deserialized: AccountInfoResponse = from_str(&serialized).unwrap();

        assert_eq!(account_info, deserialized);
    }
}
