mod block_inspector;
mod bootstrap_responder;
mod bootstrapper;
mod cleanup;
mod crawlers;
mod requesters;
mod response_handler;
pub(crate) mod state;

use std::sync::Arc;

pub use bootstrap_responder::*;
pub use bootstrapper::*;
use rsnano_core::{Account, BlockHash};
use rsnano_messages::AscPullReqType;
use rsnano_network::Channel;
use rsnano_nullable_clock::Timestamp;

pub(self) trait BootstrapAction<T> {
    fn run(&mut self, state: &mut state::BootstrapState, now: Timestamp) -> WaitResult<T>;
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
