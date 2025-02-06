mod block_inspector;
mod bootstrap_responder;
mod bootstrapper;
mod cleanup;
mod requesters;
mod response_processor;
pub(crate) mod state;

use std::sync::Arc;

pub use bootstrap_responder::*;
pub use bootstrapper::*;
use rsnano_core::{Account, BlockHash};
use rsnano_messages::{AscPullReqType, FrontiersReqPayload};
use rsnano_network::Channel;

pub(self) trait BootstrapPromise<T> {
    fn poll(&mut self, state: &mut state::BootstrapState) -> PromiseResult<T>;
}

pub(self) enum PromiseResult<T> {
    Progress,
    Wait,
    Finished(T),
}

pub(crate) struct AscPullQuerySpec {
    pub channel: Arc<Channel>,
    pub req_type: AscPullReqType,
    pub account: Account,
    pub hash: BlockHash,
    pub cooldown_account: bool,
}

impl AscPullQuerySpec {
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
}

#[cfg(test)]
pub(self) fn progress<T>(
    requester: &mut impl BootstrapPromise<T>,
    state: &mut state::BootstrapState,
) -> PromiseResult<T> {
    loop {
        match requester.poll(state) {
            PromiseResult::Progress => {}
            result => return result,
        }
    }
}
