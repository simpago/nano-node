use crate::{RpcBool, RpcCommand, RpcU8};
use rsnano_core::{Account, Amount};
use serde::{Deserialize, Serialize};
use std::net::SocketAddrV6;

impl RpcCommand {
    pub fn confirmation_quorum(peer_details: Option<bool>) -> Self {
        Self::ConfirmationQuorum(ConfirmationQuorumArgs::new(peer_details))
    }
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct ConfirmationQuorumArgs {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub peer_details: Option<RpcBool>,
}

impl ConfirmationQuorumArgs {
    pub fn new(peer_details: Option<bool>) -> Self {
        Self {
            peer_details: peer_details.map(|i| i.into()),
        }
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
