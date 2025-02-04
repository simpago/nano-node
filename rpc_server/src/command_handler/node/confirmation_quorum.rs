use crate::command_handler::RpcCommandHandler;
use rsnano_core::utils::NULL_ENDPOINT;
use rsnano_rpc_messages::{ConfirmationQuorumArgs, ConfirmationQuorumResponse, PeerDetailsDto};

impl RpcCommandHandler {
    pub(crate) fn confirmation_quorum(
        &self,
        args: ConfirmationQuorumArgs,
    ) -> ConfirmationQuorumResponse {
        let online_reps = self.node.online_reps.lock().unwrap();

        let mut result = ConfirmationQuorumResponse {
            quorum_delta: online_reps.quorum_delta(),
            online_weight_quorum_percent: online_reps.quorum_percent().into(),
            online_weight_minimum: online_reps.online_weight_minimum(),
            online_stake_total: online_reps.online_weight(),
            trended_stake_total: online_reps.trended_or_minimum_weight(),
            peers_stake_total: online_reps.peered_weight(),
            peers: None,
        };

        if args.peer_details.unwrap_or_default().inner() {
            let peers = online_reps
                .peered_reps()
                .iter()
                .map(|rep| {
                    let endpoint = self
                        .node
                        .network
                        .read()
                        .unwrap()
                        .get(rep.channel_id)
                        .map(|c| c.peer_addr())
                        .unwrap_or(NULL_ENDPOINT);

                    PeerDetailsDto {
                        account: rep.account.into(),
                        ip: endpoint,
                        weight: self.node.ledger.weight(&rep.account),
                    }
                })
                .collect();

            result.peers = Some(peers);
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use crate::command_handler::test_rpc_command;
    use rsnano_core::Amount;
    use rsnano_rpc_messages::{ConfirmationQuorumResponse, RpcCommand};

    #[tokio::test]
    async fn confirmation_quorum() {
        let result: ConfirmationQuorumResponse =
            test_rpc_command(RpcCommand::confirmation_quorum());
        assert!(result.quorum_delta > Amount::zero());
    }
}
