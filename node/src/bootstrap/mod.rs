mod block_inspector;
mod bootstrap_responder;
mod bootstrap_state;
mod bootstrapper;
mod candidate_accounts;
mod channel_waiter;
mod cleanup;
mod crawlers;
mod dependency_requester;
mod frontier_scan;
mod peer_scoring;
mod priority_requester;
mod response_handler;
mod running_query_container;

use std::sync::Arc;

pub use bootstrap_responder::*;
use bootstrap_state::BootstrapState;
pub use bootstrapper::*;
pub(crate) use candidate_accounts::*;
use rsnano_core::{Account, BlockHash};
use rsnano_messages::AscPullReqType;
use rsnano_network::Channel;
use rsnano_nullable_clock::Timestamp;

pub(self) trait BootstrapAction<T> {
    fn run(&mut self, state: &mut BootstrapState, now: Timestamp) -> WaitResult<T>;
}

pub(self) enum WaitResult<T> {
    BeginWait,
    ContinueWait,
    Finished(T),
}

pub(self) struct AscPullQuerySpec {
    pub channel: Arc<Channel>,
    pub req_type: AscPullReqType,
    pub account: Account,
    pub hash: BlockHash,
    pub cooldown_account: bool,
}
