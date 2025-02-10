mod block_inspector;
mod bootstrap_responder;
mod bootstrapper;
mod cleanup;
mod requesters;
mod response_processor;

pub(crate) mod state;
use state::QueryType;
pub use state::{BootstrapCounters, FrontierHeadInfo, FrontierScanConfig};
use std::sync::Arc;

pub use bootstrap_responder::*;
pub use bootstrapper::*;
use rsnano_core::{Account, BlockHash};
use rsnano_messages::{AscPullReqType, FrontiersReqPayload, HashType};
use rsnano_network::Channel;

pub(self) trait BootstrapPromise<T> {
    fn poll(&mut self, state: &mut state::BootstrapState) -> PollResult<T>;
}

pub(self) enum PollResult<T> {
    Progress,
    Wait,
    Finished(T),
}

#[derive(PartialEq, Eq, Debug)]
pub(crate) struct AscPullQuerySpec {
    pub channel: Arc<Channel>,
    pub req_type: AscPullReqType,
    pub account: Account,
    pub hash: BlockHash,
    pub cooldown_account: bool,
}

impl AscPullQuerySpec {
    #[allow(dead_code)]
    pub fn new_test_instance() -> Self {
        Self {
            req_type: AscPullReqType::Frontiers(FrontiersReqPayload {
                start: 100.into(),
                count: 1000,
            }),
            channel: Arc::new(Channel::new_test_instance()),
            account: Account::from(100),
            hash: BlockHash::from(200),
            cooldown_account: false,
        }
    }

    pub fn query_type(&self) -> QueryType {
        match &self.req_type {
            AscPullReqType::Blocks(b) => match b.start_type {
                HashType::Account => QueryType::BlocksByAccount,
                HashType::Block => QueryType::BlocksByHash,
            },
            AscPullReqType::AccountInfo(_) => QueryType::AccountInfoByHash,
            AscPullReqType::Frontiers(_) => QueryType::Frontiers,
        }
    }
}

#[cfg(test)]
pub(self) fn progress<T>(
    requester: &mut impl BootstrapPromise<T>,
    state: &mut state::BootstrapState,
) -> PollResult<T> {
    loop {
        match requester.poll(state) {
            PollResult::Progress => {}
            result => return result,
        }
    }
}
