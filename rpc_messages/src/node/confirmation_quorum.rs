use crate::{RpcBool, RpcCommand, RpcU8};
use rsnano_core::{Account, Amount};
use serde::{Deserialize, Serialize};
use std::net::SocketAddrV6;

impl RpcCommand {
    pub fn confirmation_quorum() -> Self {
        Self::ConfirmationQuorum(ConfirmationQuorumArgs { peer_details: None })
    }

    pub fn confirmation_quorum_with_details() -> Self {
        Self::ConfirmationQuorum(ConfirmationQuorumArgs {
            peer_details: Some(true.into()),
        })
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ConfirmationQuorumArgs {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer_details: Option<RpcBool>,
}

impl ConfirmationQuorumArgs {
    pub fn include_peer_details(&self) -> bool {
        self.peer_details.unwrap_or_default().inner()
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ConfirmationQuorumResponse {
    pub quorum_delta: Amount,
    pub online_weight_quorum_percent: RpcU8,
    pub online_weight_minimum: Amount,
    pub online_stake_total: Amount,
    pub peers_stake_total: Amount,
    pub trended_stake_total: Amount,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peers: Option<Vec<PeerDetailsDto>>,
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct PeerDetailsDto {
    pub account: Account,
    pub ip: SocketAddrV6,
    pub weight: Amount,
}
