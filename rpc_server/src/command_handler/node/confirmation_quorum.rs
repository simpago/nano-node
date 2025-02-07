use crate::command_handler::RpcCommandHandler;
use rsnano_node::representatives::OnlineReps;
use rsnano_rpc_messages::{ConfirmationQuorumArgs, ConfirmationQuorumResponse, PeerDetailsDto};

impl RpcCommandHandler {
    pub(crate) fn confirmation_quorum(
        &self,
        args: ConfirmationQuorumArgs,
    ) -> ConfirmationQuorumResponse {
        let online_reps = self.node.online_reps.lock().unwrap();
        create_response(args, &online_reps)
    }
}

fn create_response(
    args: ConfirmationQuorumArgs,
    online_reps: &OnlineReps,
) -> ConfirmationQuorumResponse {
    let mut result = ConfirmationQuorumResponse {
        quorum_delta: online_reps.quorum_delta(),
        online_weight_quorum_percent: online_reps.quorum_percent().into(),
        online_weight_minimum: online_reps.online_weight_minimum(),
        online_stake_total: online_reps.online_weight(),
        trended_stake_total: online_reps.trended_or_minimum_weight(),
        peers_stake_total: online_reps.peered_weight(),
        peers: None,
    };

    if args.include_peer_details() {
        let peers = online_reps
            .peered_reps()
            .iter()
            .map(|rep| PeerDetailsDto {
                account: rep.rep_key.into(),
                ip: rep.channel.peer_addr(),
                weight: rep.weight,
            })
            .collect();

        result.peers = Some(peers);
    }

    result
}

#[cfg(test)]
mod tests {
    use super::create_response;
    use crate::command_handler::test_rpc_command;
    use rsnano_core::Amount;
    use rsnano_node::representatives::OnlineReps;
    use rsnano_rpc_messages::{ConfirmationQuorumArgs, ConfirmationQuorumResponse, RpcCommand};

    #[test]
    fn confirmation_quorum_command() {
        let result: ConfirmationQuorumResponse =
            test_rpc_command(RpcCommand::confirmation_quorum());
        assert!(result.quorum_delta > Amount::zero());
    }

    #[test]
    fn quorum_response() {
        let online_reps = OnlineReps::new_test_instance();
        let response = create_response(ConfirmationQuorumArgs { peer_details: None }, &online_reps);
        assert_eq!(response.quorum_delta, online_reps.quorum_delta());
        assert_eq!(
            response.online_weight_quorum_percent,
            online_reps.quorum_percent().into()
        );
        assert_eq!(
            response.online_weight_minimum,
            online_reps.online_weight_minimum()
        );
        assert_eq!(response.online_stake_total, online_reps.online_weight());
        assert_eq!(
            response.trended_stake_total,
            online_reps.trended_or_minimum_weight()
        );
        assert_eq!(response.peers_stake_total, online_reps.peered_weight());
        assert!(response.peers.is_none());
    }

    #[test]
    fn quorum_response_with_peers() {
        let online_reps = OnlineReps::new_test_instance();
        let response = create_response(
            ConfirmationQuorumArgs {
                peer_details: Some(true.into()),
            },
            &online_reps,
        );
        assert_eq!(response.quorum_delta, online_reps.quorum_delta());
        let peers = response.peers.unwrap();
        assert_eq!(peers.len(), online_reps.peered_reps().len());
    }
}
