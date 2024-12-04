use crate::RpcCommand;
use rsnano_core::{Account, Amount, WalletId, WorkNonce};
use serde::{Deserialize, Serialize};

impl RpcCommand {
    pub fn send(args: SendArgs) -> Self {
        Self::Send(args)
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Default)]
pub struct SendArgs {
    pub wallet: WalletId,
    pub source: Account,
    pub destination: Account,
    pub amount: Amount,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub work: Option<WorkNonce>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn serialize_send_command() {
        let wallet = WalletId::decode_hex(
            "000D1BAEC8EC208142C99059B393051BAC8380F9B5A2E6B2489A277D81789F3F",
        )
        .unwrap();
        let source = Account::decode_account(
            "nano_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3",
        )
        .unwrap();
        let destination = Account::decode_account(
            "nano_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3",
        )
        .unwrap();
        let amount = Amount::raw(1000000);

        let send_command = RpcCommand::send(SendArgs {
            wallet,
            source,
            destination,
            amount,
            ..Default::default()
        });

        let serialized = serde_json::to_value(&send_command).unwrap();
        let expected = json!({
            "action": "send",
            "wallet": "000D1BAEC8EC208142C99059B393051BAC8380F9B5A2E6B2489A277D81789F3F",
            "source": "nano_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3",
            "destination": "nano_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3",
            "amount": "1000000"
        });

        assert_eq!(serialized, expected);
    }

    #[test]
    fn deserialize_send_command() {
        let json_str = r#"{
            "action": "send",
            "wallet": "000D1BAEC8EC208142C99059B393051BAC8380F9B5A2E6B2489A277D81789F3F",
            "source": "nano_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3",
            "destination": "nano_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3",
            "amount": "1000000"
        }"#;

        let deserialized: RpcCommand = serde_json::from_str(json_str).unwrap();

        let wallet = WalletId::decode_hex(
            "000D1BAEC8EC208142C99059B393051BAC8380F9B5A2E6B2489A277D81789F3F",
        )
        .unwrap();
        let source = Account::decode_account(
            "nano_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3",
        )
        .unwrap();
        let destination = Account::decode_account(
            "nano_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3",
        )
        .unwrap();
        let amount = Amount::raw(1000000);

        assert_eq!(
            deserialized,
            RpcCommand::send(SendArgs {
                wallet,
                source,
                destination,
                amount,
                ..Default::default()
            })
        );
    }

    #[test]
    fn serialize_send_args() {
        let wallet = WalletId::decode_hex(
            "000D1BAEC8EC208142C99059B393051BAC8380F9B5A2E6B2489A277D81789F3F",
        )
        .unwrap();
        let source = Account::decode_account(
            "nano_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3",
        )
        .unwrap();
        let destination = Account::decode_account(
            "nano_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3",
        )
        .unwrap();
        let amount = Amount::raw(1000000);

        let send_command = SendArgs {
            wallet,
            source,
            destination,
            amount,
            ..Default::default()
        };

        let serialized = serde_json::to_value(&send_command).unwrap();
        let expected = json!({
            "wallet": "000D1BAEC8EC208142C99059B393051BAC8380F9B5A2E6B2489A277D81789F3F",
            "source": "nano_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3",
            "destination": "nano_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3",
            "amount": "1000000"
        });

        assert_eq!(serialized, expected);
    }

    #[test]
    fn deserialize_send_args() {
        let json_str = r#"{
            "wallet": "000D1BAEC8EC208142C99059B393051BAC8380F9B5A2E6B2489A277D81789F3F",
            "source": "nano_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3",
            "destination": "nano_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3",
            "amount": "1000000"
        }"#;

        let deserialized: SendArgs = serde_json::from_str(json_str).unwrap();

        assert_eq!(
            deserialized.wallet,
            WalletId::decode_hex(
                "000D1BAEC8EC208142C99059B393051BAC8380F9B5A2E6B2489A277D81789F3F"
            )
            .unwrap()
        );
        assert_eq!(
            deserialized.source,
            Account::decode_account(
                "nano_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3"
            )
            .unwrap()
        );
        assert_eq!(
            deserialized.destination,
            Account::decode_account(
                "nano_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3"
            )
            .unwrap()
        );
        assert_eq!(deserialized.amount, Amount::raw(1000000));
    }

    #[test]
    fn test_send_args_builder() {
        let wallet = WalletId::decode_hex(
            "000D1BAEC8EC208142C99059B393051BAC8380F9B5A2E6B2489A277D81789F3F",
        )
        .unwrap();
        let source = Account::decode_account(
            "nano_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3",
        )
        .unwrap();
        let destination = Account::decode_account(
            "nano_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3",
        )
        .unwrap();
        let amount = Amount::raw(1000000);

        let send_args = SendArgs {
            wallet,
            source,
            destination,
            amount,
            work: Some(1.into()),
            id: Some("test_id".to_string()),
        };

        let serialized = serde_json::to_value(&send_args).unwrap();
        let expected = json!({
            "wallet": "000D1BAEC8EC208142C99059B393051BAC8380F9B5A2E6B2489A277D81789F3F",
            "source": "nano_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3",
            "destination": "nano_3t6k35gi95xu6tergt6p69ck76ogmitsa8mnijtpxm9fkcm736xtoncuohr3",
            "amount": "1000000",
            "work": "0000000000000001",
            "id": "test_id"
        });

        assert_eq!(serialized, expected);
    }
}
