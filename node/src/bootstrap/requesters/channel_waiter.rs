use crate::bootstrap::{state::BootstrapState, BootstrapAction, WaitResult};
use rsnano_network::{bandwidth_limiter::RateLimiter, Channel};
use rsnano_nullable_clock::Timestamp;
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
    Found(Arc<Channel>),
}

impl ChannelWaiter {
    pub fn new(limiter: Arc<RateLimiter>, max_requests: usize) -> Self {
        Self {
            state: ChannelWaitState::Initial,
            limiter,
            max_requests,
        }
    }

    fn transition_state(&mut self, state: &mut BootstrapState) -> bool {
        let mut state_changed = false;
        loop {
            if let Some(new_state) = self.get_next_state(state) {
                self.state = new_state;
                state_changed = true;
            } else {
                return state_changed;
            }
        }
    }

    fn get_next_state(&self, state: &mut BootstrapState) -> Option<ChannelWaitState> {
        match &self.state {
            ChannelWaitState::Initial => Some(ChannelWaitState::WaitRunningQueries),
            ChannelWaitState::WaitRunningQueries => self.wait_running_queries(state),
            ChannelWaitState::WaitLimiter => self.wait_limiter(),
            ChannelWaitState::WaitScoring => Self::wait_scoring(state),
            ChannelWaitState::Found(_) => None,
        }
    }

    /// Limit the number of in-flight requests
    fn wait_running_queries(&self, state: &BootstrapState) -> Option<ChannelWaitState> {
        if state.running_queries.len() < self.max_requests {
            Some(ChannelWaitState::WaitLimiter)
        } else {
            None
        }
    }

    /// Wait until more requests can be sent
    fn wait_limiter(&self) -> Option<ChannelWaitState> {
        if self.limiter.should_pass(1) {
            Some(ChannelWaitState::WaitScoring)
        } else {
            None
        }
    }

    /// Wait until a channel is available
    fn wait_scoring(state: &mut BootstrapState) -> Option<ChannelWaitState> {
        let channel = state.scoring.channel();
        if let Some(channel) = channel {
            Some(ChannelWaitState::Found(channel))
        } else {
            None
        }
    }
}

impl BootstrapAction<Arc<Channel>> for ChannelWaiter {
    fn run(&mut self, state: &mut BootstrapState, _now: Timestamp) -> WaitResult<Arc<Channel>> {
        let state_changed = self.transition_state(state);
        if let ChannelWaitState::Found(channel) = &self.state {
            WaitResult::Finished(channel.clone())
        } else if state_changed {
            WaitResult::BeginWait
        } else {
            WaitResult::ContinueWait
        }
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
        let WaitResult::Finished(found) = waiter.run(&mut state, Timestamp::new_test_instance())
        else {
            panic!("no channel found");
        };
        assert_eq!(channel.channel_id(), found.channel_id());
    }

    #[test]
    fn wait_for_running_queries() {
        let limiter = Arc::new(RateLimiter::new(TEST_RATE_LIMIT));
        let mut waiter = ChannelWaiter::new(limiter, 1);
        let mut state = BootstrapState::new(BootstrapConfig::default(), Arc::new(Stats::default()));
        state
            .running_queries
            .insert(RunningQuery::new_test_instance());
        let result = waiter.run(&mut state, Timestamp::new_test_instance());
        assert!(matches!(result, WaitResult::BeginWait));
        assert!(matches!(waiter.state, ChannelWaitState::WaitRunningQueries));
        let result = waiter.run(&mut state, Timestamp::new_test_instance());
        assert!(matches!(result, WaitResult::ContinueWait));
    }

    #[test]
    fn wait_for_limiter() {
        let limiter = Arc::new(RateLimiter::new(TEST_RATE_LIMIT));
        limiter.should_pass(TEST_RATE_LIMIT);
        let mut waiter = ChannelWaiter::new(limiter, MAX_TEST_REQUESTS);
        let mut state = BootstrapState::new(BootstrapConfig::default(), Arc::new(Stats::default()));
        let result = waiter.run(&mut state, Timestamp::new_test_instance());
        assert!(matches!(result, WaitResult::BeginWait));
        assert!(matches!(waiter.state, ChannelWaitState::WaitLimiter));
        let result = waiter.run(&mut state, Timestamp::new_test_instance());
        assert!(matches!(result, WaitResult::ContinueWait));
    }

    #[test]
    fn wait_scoring() {
        let limiter = Arc::new(RateLimiter::new(TEST_RATE_LIMIT));
        let mut waiter = ChannelWaiter::new(limiter, MAX_TEST_REQUESTS);
        let mut state = BootstrapState::new(BootstrapConfig::default(), Arc::new(Stats::default()));
        let result = waiter.run(&mut state, Timestamp::new_test_instance());
        assert!(matches!(result, WaitResult::BeginWait));
        assert!(matches!(waiter.state, ChannelWaitState::WaitScoring));
        let result = waiter.run(&mut state, Timestamp::new_test_instance());
        assert!(matches!(result, WaitResult::ContinueWait));
    }

    const TEST_RATE_LIMIT: usize = 4;
    const MAX_TEST_REQUESTS: usize = 3;
}
