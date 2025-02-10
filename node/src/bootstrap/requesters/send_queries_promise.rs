use super::query_sender::QuerySender;
use crate::bootstrap::{state::BootstrapState, BootstrapPromise};
use crate::bootstrap::{AscPullQuerySpec, PollResult};

/// Promise for sending AscPullReq queries
pub(crate) struct SendQueriesPromise<T>
where
    T: BootstrapPromise<AscPullQuerySpec>,
{
    query_promise: T,
    sender: QuerySender,
}

impl<T> SendQueriesPromise<T>
where
    T: BootstrapPromise<AscPullQuerySpec>,
{
    pub(crate) fn new(query_promise: T, sender: QuerySender) -> Self {
        Self {
            query_promise,
            sender,
        }
    }
}

impl<T> BootstrapPromise<()> for SendQueriesPromise<T>
where
    T: BootstrapPromise<AscPullQuerySpec>,
{
    fn poll(&mut self, state: &mut BootstrapState) -> PollResult<()> {
        match self.query_promise.poll(state) {
            PollResult::Progress => PollResult::Progress,
            PollResult::Wait => PollResult::Wait,
            PollResult::Finished(spec) => {
                self.sender.send(spec, state);
                PollResult::Progress
            }
        }
    }
}
