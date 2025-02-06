use crate::command_handler::RpcCommandHandler;
use rsnano_core::{Account, PublicKey};
use rsnano_node::representatives::OnlineRepInfo;
use rsnano_rpc_messages::{
    DetailedRepresentativesOnline, RepWeightDto, RepresentativesOnlineArgs,
    RepresentativesOnlineResponse, SimpleRepresentativesOnline,
};
use std::collections::{HashMap, HashSet};

impl RpcCommandHandler {
    pub(crate) fn representatives_online(
        &self,
        args: RepresentativesOnlineArgs,
    ) -> RepresentativesOnlineResponse {
        let online_reps = self.node.online_reps.lock().unwrap();
        ResponseBuilder::new(args).create_response(online_reps.online_reps())
    }
}

struct ResponseBuilder {
    include_weight: bool,
    filter: Option<HashSet<PublicKey>>,
    simple_result: Vec<Account>,
    detailed_result: HashMap<Account, RepWeightDto>,
}

impl ResponseBuilder {
    pub fn new(args: RepresentativesOnlineArgs) -> Self {
        let filter = args
            .accounts
            .map(|accs| accs.iter().map(PublicKey::from).collect());

        Self {
            include_weight: args.weight.unwrap_or_default().inner(),
            filter,
            simple_result: Vec::new(),
            detailed_result: HashMap::new(),
        }
    }

    pub fn create_response(
        mut self,
        online_reps: impl IntoIterator<Item = OnlineRepInfo>,
    ) -> RepresentativesOnlineResponse {
        for rep in online_reps {
            if self.should_include(&rep.rep_key) {
                self.add_result(rep);
            }
        }
        self.take_result()
    }

    fn should_include(&self, rep_key: &PublicKey) -> bool {
        if let Some(filter) = &self.filter {
            filter.contains(&rep_key)
        } else {
            true
        }
    }

    fn add_result(&mut self, rep: OnlineRepInfo) {
        if self.include_weight {
            self.detailed_result
                .insert(rep.rep_key.into(), RepWeightDto { weight: rep.weight });
        } else {
            self.simple_result.push(rep.rep_key.into());
        };
    }

    fn take_result(self) -> RepresentativesOnlineResponse {
        if self.include_weight {
            RepresentativesOnlineResponse::Detailed(DetailedRepresentativesOnline {
                representatives: self.detailed_result,
            })
        } else {
            RepresentativesOnlineResponse::Simple(SimpleRepresentativesOnline {
                representatives: self.simple_result,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ResponseBuilder;
    use crate::command_handler::test_rpc_command;
    use rsnano_core::Account;
    use rsnano_rpc_messages::{
        RepWeightDto, RepresentativesOnlineArgs, RepresentativesOnlineResponse, RpcCommand,
        SimpleRepresentativesOnline,
    };
    use std::collections::HashMap;

    #[tokio::test]
    async fn online_reps_command() {
        let result: RepresentativesOnlineResponse = test_rpc_command(
            RpcCommand::RepresentativesOnline(RepresentativesOnlineArgs::default()),
        );
        assert_eq!(
            result,
            RepresentativesOnlineResponse::Simple(SimpleRepresentativesOnline {
                representatives: Vec::new(),
            })
        );
    }

    mod simple_result {
        use super::*;
        use rsnano_core::Amount;
        use rsnano_node::representatives::OnlineRepInfo;

        #[test]
        fn empty() {
            let response = ResponseBuilder::new(Default::default()).create_response([]);
            assert_eq!(simple_response(response).len(), 0);
        }

        #[test]
        fn one_rep() {
            let response =
                ResponseBuilder::new(Default::default()).create_response([OnlineRepInfo {
                    rep_key: 1.into(),
                    weight: Amount::zero(),
                }]);
            assert_eq!(simple_response(response), [Account::from(1)]);
        }

        #[test]
        fn filter_all() {
            let response = ResponseBuilder::new(RepresentativesOnlineArgs {
                weight: None,
                accounts: Some(Vec::new()),
            })
            .create_response([OnlineRepInfo {
                rep_key: 1.into(),
                weight: Amount::zero(),
            }]);
            assert_eq!(simple_response(response), []);
        }

        #[test]
        fn filter_given_accounts() {
            let response = ResponseBuilder::new(RepresentativesOnlineArgs {
                weight: None,
                accounts: Some(vec![2.into(), 3.into()]),
            })
            .create_response([
                OnlineRepInfo {
                    rep_key: 1.into(),
                    weight: Amount::zero(),
                },
                OnlineRepInfo {
                    rep_key: 2.into(),
                    weight: Amount::zero(),
                },
                OnlineRepInfo {
                    rep_key: 3.into(),
                    weight: Amount::zero(),
                },
                OnlineRepInfo {
                    rep_key: 4.into(),
                    weight: Amount::zero(),
                },
            ]);
            assert_eq!(simple_response(response), [2.into(), 3.into()]);
        }
    }

    mod detailed_result {
        use super::*;
        use rsnano_core::{Amount, PublicKey};
        use rsnano_node::representatives::OnlineRepInfo;

        #[test]
        fn empty() {
            let response = ResponseBuilder::new(RepresentativesOnlineArgs {
                weight: Some(true.into()),
                accounts: None,
            })
            .create_response([]);

            assert_eq!(detailed_response(response).len(), 0);
        }

        #[test]
        fn one_rep() {
            let args = RepresentativesOnlineArgs {
                weight: Some(true.into()),
                accounts: None,
            };
            let rep_key = PublicKey::from(1);
            let weight = Amount::nano(100_000);
            let response =
                ResponseBuilder::new(args).create_response([OnlineRepInfo { rep_key, weight }]);

            let response = detailed_response(response);
            assert_eq!(response.len(), 1);
            assert_eq!(response.get(&rep_key.into()).unwrap().weight, weight);
        }
    }

    fn simple_response(response: RepresentativesOnlineResponse) -> Vec<Account> {
        match response {
            RepresentativesOnlineResponse::Simple(i) => i.representatives,
            RepresentativesOnlineResponse::Detailed(_) => panic!("simple response expected"),
        }
    }

    fn detailed_response(
        response: RepresentativesOnlineResponse,
    ) -> HashMap<Account, RepWeightDto> {
        match response {
            RepresentativesOnlineResponse::Simple(_) => panic!("detailed response expected"),
            RepresentativesOnlineResponse::Detailed(i) => i.representatives,
        }
    }
}
