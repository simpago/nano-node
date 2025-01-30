use crate::bootstrap::{state::BootstrapState, BootstrapPromise, PromiseResult};
use rsnano_network::{bandwidth_limiter::RateLimiter, Channel};
use std::sync::Arc;

/// Waits until a channel becomes available
#[derive(Clone)]
pub(super) struct ChannelWaiter {
    state: ChannelWaitState,
    limiter: Arc<RateLimiter>,
    max_requests: usize,
}

#[derive(Clone)]
enum ChannelWaitState {
    Initial,
    WaitRunningQueries,
    WaitLimiter,
    WaitScoring,
}

impl ChannelWaiter {
    pub fn new(limiter: Arc<RateLimiter>, max_requests: usize) -> Self {
        Self {
            state: ChannelWaitState::Initial,
            limiter,
            max_requests,
        }
    }
}

impl BootstrapPromise<Arc<Channel>> for ChannelWaiter {
    fn poll(&mut self, boot_state: &mut BootstrapState) -> PromiseResult<Arc<Channel>> {
        match self.state {
            ChannelWaitState::Initial => {
                self.state = ChannelWaitState::WaitRunningQueries;
                return PromiseResult::Progress;
            }
            ChannelWaitState::WaitRunningQueries => {
                // Limit the number of in-flight requests
                if boot_state.running_queries.len() < self.max_requests {
                    self.state = ChannelWaitState::WaitLimiter;
                    return PromiseResult::Progress;
                }
            }
            ChannelWaitState::WaitLimiter => {
                // Wait until more requests can be sent
                if self.limiter.should_pass(1) {
                    self.state = ChannelWaitState::WaitScoring;
                    return PromiseResult::Progress;
                }
            }
            ChannelWaitState::WaitScoring => {
                // Wait until a channel is available
                let channel = boot_state.scoring.channel();
                if let Some(channel) = channel {
                    self.state = ChannelWaitState::Initial;
                    return PromiseResult::Finished(channel);
                }
            }
        }

        PromiseResult::Wait
    }
}

impl Default for ChannelWaiter {
    fn default() -> Self {
        Self::new(Arc::new(RateLimiter::new(1024)), 1024)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        bootstrap::{state::RunningQuery, BootstrapConfig},
        stats::Stats,
    };

    #[test]
    fn initial_state() {
        let limiter = Arc::new(RateLimiter::new(TEST_RATE_LIMIT));
        let waiter = ChannelWaiter::new(limiter, MAX_TEST_REQUESTS);
        assert!(matches!(waiter.state, ChannelWaitState::Initial));
    }

    #[test]
    fn happy_path_no_waiting() {
        let limiter = Arc::new(RateLimiter::new(TEST_RATE_LIMIT));
        let mut waiter = ChannelWaiter::new(limiter, MAX_TEST_REQUESTS);
        let mut state = BootstrapState::new(BootstrapConfig::default(), Arc::new(Stats::default()));
        let channel = Arc::new(Channel::new_test_instance());
        state.scoring.sync(vec![channel.clone()]);

        let found = loop {
            match waiter.poll(&mut state) {
                PromiseResult::Progress => {}
                PromiseResult::Wait => {
                    panic!("Should never wait")
                }
                PromiseResult::Finished(c) => break c,
            }
        };

        assert_eq!(channel.channel_id(), found.channel_id());
    }

    #[test]
    fn wait_for_running_queries() {
        let limiter = Arc::new(RateLimiter::new(TEST_RATE_LIMIT));
        let mut waiter = ChannelWaiter::new(limiter, 1);
        let mut state = BootstrapState::new(BootstrapConfig::default(), Arc::new(Stats::default()));

        assert!(matches!(waiter.poll(&mut state), PromiseResult::Progress)); // initial

        state
            .running_queries
            .insert(RunningQuery::new_test_instance());

        assert!(matches!(waiter.poll(&mut state), PromiseResult::Wait));
        assert!(matches!(waiter.state, ChannelWaitState::WaitRunningQueries));

        assert!(matches!(waiter.poll(&mut state), PromiseResult::Wait));

        state.running_queries.clear();
        assert!(matches!(waiter.poll(&mut state), PromiseResult::Progress));
    }

    #[test]
    fn wait_for_limiter() {
        let limiter = Arc::new(RateLimiter::new(TEST_RATE_LIMIT));
        limiter.should_pass(TEST_RATE_LIMIT);
        let mut waiter = ChannelWaiter::new(limiter.clone(), MAX_TEST_REQUESTS);
        let mut state = BootstrapState::new(BootstrapConfig::default(), Arc::new(Stats::default()));

        assert!(matches!(waiter.poll(&mut state), PromiseResult::Progress)); // initial
        assert!(matches!(waiter.poll(&mut state), PromiseResult::Progress)); // running queries

        let result = waiter.poll(&mut state);
        assert!(matches!(result, PromiseResult::Wait));
        assert!(matches!(waiter.state, ChannelWaitState::WaitLimiter));

        let result = waiter.poll(&mut state);
        assert!(matches!(result, PromiseResult::Wait));

        limiter.reset();
        let result = waiter.poll(&mut state);
        assert!(matches!(result, PromiseResult::Progress));
        assert!(matches!(waiter.state, ChannelWaitState::WaitScoring));
    }

    #[test]
    fn wait_scoring() {
        let limiter = Arc::new(RateLimiter::new(TEST_RATE_LIMIT));
        let mut waiter = ChannelWaiter::new(limiter, MAX_TEST_REQUESTS);
        let mut state = BootstrapState::new(BootstrapConfig::default(), Arc::new(Stats::default()));

        assert!(matches!(waiter.poll(&mut state), PromiseResult::Progress)); // initial
        assert!(matches!(waiter.poll(&mut state), PromiseResult::Progress)); // running queries
        assert!(matches!(waiter.poll(&mut state), PromiseResult::Progress)); // limiter

        let result = waiter.poll(&mut state);
        assert!(matches!(result, PromiseResult::Wait));
        assert!(matches!(waiter.state, ChannelWaitState::WaitScoring));

        let result = waiter.poll(&mut state);
        assert!(matches!(result, PromiseResult::Wait));
    }

    const TEST_RATE_LIMIT: usize = 4;
    const MAX_TEST_REQUESTS: usize = 3;
}
