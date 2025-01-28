mod bootstrap_responder;
mod bootstrapper;
mod candidate_accounts;
mod channel_waiter;
mod crawlers;
mod frontier_scan;
mod peer_scoring;
mod priority_query;
mod running_query_container;
mod throttle;

use std::sync::Arc;

pub use bootstrap_responder::*;
pub use bootstrapper::*;
pub(crate) use candidate_accounts::*;
use rsnano_core::{Account, BlockHash};
use rsnano_messages::AscPullReqType;
use rsnano_network::Channel;
use rsnano_nullable_clock::Timestamp;

pub(self) trait BootstrapAction<T> {
    fn run(&mut self, logic: &mut BootstrapLogic, now: Timestamp) -> WaitResult<T>;
}

pub(self) enum WaitResult<T> {
    BeginWait,
    ContinueWait,
    Finished(T),
}

pub(super) struct AscPullQuerySpec {
    pub channel: Arc<Channel>,
    pub req_type: AscPullReqType,
    pub account: Account,
    pub hash: BlockHash,
    pub cooldown_account: bool,
}
