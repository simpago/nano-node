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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress() {
        let sender = QuerySender::new_null();
        let mut send_queries =
            SendQueriesPromise::new(StubPromise::new(PollResult::Progress), sender);
        let mut state = BootstrapState::default();

        let result = send_queries.poll(&mut state);

        assert!(matches!(result, PollResult::Progress));
    }

    #[test]
    fn wait() {
        let sender = QuerySender::new_null();
        let mut send_queries = SendQueriesPromise::new(StubPromise::new(PollResult::Wait), sender);
        let mut state = BootstrapState::default();

        let result = send_queries.poll(&mut state);

        assert!(matches!(result, PollResult::Wait));
    }

    #[test]
    fn send() {
        let sender = QuerySender::new_null();
        let send_tracker = sender.track();
        let spec = AscPullQuerySpec::new_test_instance();
        let mut send_queries =
            SendQueriesPromise::new(StubPromise::new(PollResult::Finished(spec.clone())), sender);
        let mut state = BootstrapState::default();

        let result = send_queries.poll(&mut state);

        assert!(matches!(result, PollResult::Progress));
        assert_eq!(send_tracker.output(), [spec]);
    }

    struct StubPromise {
        result: Option<PollResult<AscPullQuerySpec>>,
    }

    impl StubPromise {
        fn new(result: PollResult<AscPullQuerySpec>) -> Self {
            Self {
                result: Some(result),
            }
        }
    }

    impl BootstrapPromise<AscPullQuerySpec> for StubPromise {
        fn poll(&mut self, _state: &mut BootstrapState) -> PollResult<AscPullQuerySpec> {
            self.result.take().unwrap()
        }
    }
}
