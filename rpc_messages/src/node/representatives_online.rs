use crate::RpcBool;
use rsnano_core::{Account, Amount};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RepresentativesOnlineArgs {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub weight: Option<RpcBool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub accounts: Option<Vec<Account>>,
}

impl RepresentativesOnlineArgs {
    pub fn builder() -> RepresentativesOnlineArgsBuilder {
        RepresentativesOnlineArgsBuilder {
            args: RepresentativesOnlineArgs::default(),
        }
    }
}

pub struct RepresentativesOnlineArgsBuilder {
    args: RepresentativesOnlineArgs,
}

impl RepresentativesOnlineArgsBuilder {
    pub fn weight(mut self) -> Self {
        self.args.weight = Some(true.into());
        self
    }

    pub fn accounts(mut self, accounts: Vec<Account>) -> Self {
        self.args.accounts = Some(accounts);
        self
    }

    pub fn build(self) -> RepresentativesOnlineArgs {
        self.args
    }
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RepresentativesOnlineResponse {
    Simple(SimpleRepresentativesOnline),
    Detailed(DetailedRepresentativesOnline),
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimpleRepresentativesOnline {
    pub representatives: Vec<Account>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct DetailedRepresentativesOnline {
    pub representatives: HashMap<Account, RepWeightDto>,
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepWeightDto {
    pub weight: Amount,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RpcCommand;
    use serde_json::json;

    #[test]
    fn serialize_representatives_online_command_options_none() {
        let command = RpcCommand::RepresentativesOnline(RepresentativesOnlineArgs::default());
        let serialized = serde_json::to_value(command).unwrap();
        let expected = json!({"action": "representatives_online"});
        assert_eq!(serialized, expected);
    }

    #[test]
    fn deserialize_representatives_online_command_options_none() {
        let json = r#"{"action": "representatives_online"}"#;
        let deserialized: RpcCommand = serde_json::from_str(json).unwrap();
        let command = RpcCommand::RepresentativesOnline(RepresentativesOnlineArgs::default());
        assert_eq!(deserialized, command);
    }

    #[test]
    fn serialize_representatives_online_command_options_some() {
        let accounts = vec![Account::decode_account(
            "nano_1jg8zygjg3pp5w644emqcbmjqpnzmubfni3kfe1s8pooeuxsw49fdq1mco9j",
        )
        .unwrap()];
        let args = RepresentativesOnlineArgs::builder()
            .weight()
            .accounts(accounts.clone())
            .build();
        let command = RpcCommand::RepresentativesOnline(args);
        let serialized = serde_json::to_value(command).unwrap();
        let expected = json!({
            "action": "representatives_online",
            "weight": "true",
            "accounts": ["nano_1jg8zygjg3pp5w644emqcbmjqpnzmubfni3kfe1s8pooeuxsw49fdq1mco9j"]
        });
        assert_eq!(serialized, expected);
    }

    #[test]
    fn deserialize_representatives_online_command_options_some() {
        let json = r#"{
            "action": "representatives_online",
            "weight": "true",
            "accounts": ["nano_1jg8zygjg3pp5w644emqcbmjqpnzmubfni3kfe1s8pooeuxsw49fdq1mco9j"]
        }"#;
        let deserialized: RpcCommand = serde_json::from_str(json).unwrap();
        if let RpcCommand::RepresentativesOnline(args) = deserialized {
            assert_eq!(args.weight, Some(true.into()));
            assert_eq!(
                args.accounts,
                Some(vec![Account::decode_account(
                    "nano_1jg8zygjg3pp5w644emqcbmjqpnzmubfni3kfe1s8pooeuxsw49fdq1mco9j"
                )
                .unwrap()])
            );
        } else {
            panic!("Deserialized to wrong variant");
        }
    }

    #[test]
    fn serialize_representatives_online_dto_with_weight() {
        let mut detailed = DetailedRepresentativesOnline::default();
        detailed.representatives.insert(
            Account::decode_account(
                "nano_114nk4rwjctu6n6tr6g6ps61g1w3hdpjxfas4xj1tq6i8jyomc5d858xr1xi",
            )
            .unwrap(),
            RepWeightDto {
                weight: Amount::raw(150462654614686936429917024683496890),
            },
        );
        let dto = RepresentativesOnlineResponse::Detailed(detailed);
        let serialized = serde_json::to_string(&dto).unwrap();
        let expected = r#"{"representatives":{"nano_114nk4rwjctu6n6tr6g6ps61g1w3hdpjxfas4xj1tq6i8jyomc5d858xr1xi":{"weight":"150462654614686936429917024683496890"}}}"#;
        assert_eq!(serialized, expected);
    }

    #[test]
    fn serialize_representatives_online_dto_without_weight() {
        let mut representatives = Vec::new();
        representatives.push(
            Account::decode_account(
                "nano_114nk4rwjctu6n6tr6g6ps61g1w3hdpjxfas4xj1tq6i8jyomc5d858xr1xi",
            )
            .unwrap(),
        );
        let dto =
            RepresentativesOnlineResponse::Simple(SimpleRepresentativesOnline { representatives });
        let serialized = serde_json::to_string(&dto).unwrap();
        let expected = r#"{"representatives":["nano_114nk4rwjctu6n6tr6g6ps61g1w3hdpjxfas4xj1tq6i8jyomc5d858xr1xi"]}"#;
        assert_eq!(serialized, expected);
    }

    #[test]
    fn deserialize_representatives_online_dto() {
        let json = r#"{"representatives":{"nano_114nk4rwjctu6n6tr6g6ps61g1w3hdpjxfas4xj1tq6i8jyomc5d858xr1xi":{"weight":"150462654614686936429917024683496890"}}}"#;
        let deserialized: DetailedRepresentativesOnline = serde_json::from_str(json).unwrap();

        assert_eq!(deserialized.representatives.len(), 1);
        let account = Account::decode_account(
            "nano_114nk4rwjctu6n6tr6g6ps61g1w3hdpjxfas4xj1tq6i8jyomc5d858xr1xi",
        )
        .unwrap();
        assert_eq!(
            deserialized.representatives[&account].weight,
            Amount::raw(150462654614686936429917024683496890)
        );
    }

    #[test]
    fn deserialize_representatives_online_dto_without_weight() {
        let json = r#"{"representatives":["nano_114nk4rwjctu6n6tr6g6ps61g1w3hdpjxfas4xj1tq6i8jyomc5d858xr1xi"]}"#;
        let deserialized: SimpleRepresentativesOnline = serde_json::from_str(json).unwrap();

        assert_eq!(deserialized.representatives.len(), 1);
        let account = Account::decode_account(
            "nano_114nk4rwjctu6n6tr6g6ps61g1w3hdpjxfas4xj1tq6i8jyomc5d858xr1xi",
        )
        .unwrap();
        assert!(deserialized.representatives.contains(&account));
    }
}
