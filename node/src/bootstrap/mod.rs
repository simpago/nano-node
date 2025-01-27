mod bootstrap_responder;
mod bootstrapper;
mod candidate_accounts;
mod channel_waiter;
mod crawlers;
mod database_scan;
mod frontier_scan;
mod peer_scoring;
mod running_query_container;
mod throttle;

pub use bootstrap_responder::*;
pub use bootstrapper::*;
pub(crate) use candidate_accounts::*;
use rsnano_nullable_clock::Timestamp;

pub(self) trait BootstrapAction<T> {
    fn run(&mut self, logic: &mut BootstrapLogic, now: Timestamp) -> WaitResult<T>;
}

pub(self) enum WaitResult<T> {
    BeginWait,
    ContinueWait,
    Finished(T),
}
