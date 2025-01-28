use super::{BootstrapAction, BootstrapState, WaitResult};
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
        if let Some(new_state) = self.get_next_state(state) {
            self.state = new_state;
            true // State changed
        } else {
            false // State did not change
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
