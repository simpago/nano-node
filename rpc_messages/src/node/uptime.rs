use crate::{RpcCommand, RpcU64};
use serde::{Deserialize, Serialize};

impl RpcCommand {
    pub fn uptime() -> Self {
        Self::Uptime
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct UptimeResponse {
    pub seconds: RpcU64,
}

impl UptimeResponse {
    pub fn new(seconds: u64) -> Self {
        Self {
            seconds: seconds.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::RpcCommand;
    use serde_json::{from_str, to_string_pretty};

    #[test]
    fn serialize_uptime_command() {
        assert_eq!(
            to_string_pretty(&RpcCommand::Uptime).unwrap(),
            r#"{
  "action": "uptime"
}"#
        );
    }

    #[test]
    fn deserialize_uptime_command() {
        let cmd = RpcCommand::Uptime;
        let serialized = to_string_pretty(&cmd).unwrap();
        let deserialized: RpcCommand = from_str(&serialized).unwrap();
        assert_eq!(cmd, deserialized);
    }
}
