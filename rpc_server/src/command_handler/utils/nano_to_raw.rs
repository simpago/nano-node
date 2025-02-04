use anyhow::anyhow;
use rsnano_core::Amount;
use rsnano_rpc_messages::AmountRpcMessage;

pub fn nano_to_raw(args: AmountRpcMessage) -> anyhow::Result<AmountRpcMessage> {
    if let Some(raw) = args.amount.number().checked_mul(Amount::nano(1).number()) {
        Ok(AmountRpcMessage::new(Amount::raw(raw)))
    } else {
        Err(anyhow!("Invalid amount number"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nano_to_raw() {
        assert_eq!(
            nano_to_raw(AmountRpcMessage::new(Amount::raw(42)))
                .unwrap()
                .amount,
            Amount::nano(42)
        );
    }

    #[test]
    fn invalid_amount() {
        let err = nano_to_raw(AmountRpcMessage::new(Amount::nano(300_000_000))).unwrap_err();
        assert_eq!("Invalid amount number", err.to_string());
    }
}
