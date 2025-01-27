use super::{BootstrapLogic, BootstrapWaiter, WaitResult};
use rsnano_network::Channel;
use rsnano_nullable_clock::Timestamp;
use std::sync::Arc;

/// Waits until a channel becomes available
#[derive(Clone)]
pub(super) struct ChannelWaiter {
    state: ChannelWaitState,
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
    pub fn new() -> Self {
        Self {
            state: ChannelWaitState::Initial,
        }
    }

    fn transition_state(&mut self, logic: &mut BootstrapLogic) -> bool {
        if let Some(new_state) = self.get_next_state(logic) {
            self.state = new_state;
            true // State changed
        } else {
            false // State did not change
        }
    }

    fn get_next_state(&self, logic: &mut BootstrapLogic) -> Option<ChannelWaitState> {
        match &self.state {
            ChannelWaitState::Initial => Some(ChannelWaitState::WaitRunningQueries),
            ChannelWaitState::WaitRunningQueries => Self::wait_running_queries(logic),
            ChannelWaitState::WaitLimiter => Self::wait_limiter(logic),
            ChannelWaitState::WaitScoring => Self::wait_scoring(logic),
            ChannelWaitState::Found(_) => None,
        }
    }

    /// Limit the number of in-flight requests
    fn wait_running_queries(logic: &BootstrapLogic) -> Option<ChannelWaitState> {
        if logic.running_queries.len() < logic.config.max_requests {
            Some(ChannelWaitState::WaitLimiter)
        } else {
            None
        }
    }

    /// Wait until more requests can be sent
    fn wait_limiter(logic: &BootstrapLogic) -> Option<ChannelWaitState> {
        if logic.limiter.should_pass(1) {
            Some(ChannelWaitState::WaitScoring)
        } else {
            None
        }
    }

    /// Wait until a channel is available
    fn wait_scoring(logic: &mut BootstrapLogic) -> Option<ChannelWaitState> {
        let channel = logic.scoring.channel();
        if let Some(channel) = channel {
            Some(ChannelWaitState::Found(channel))
        } else {
            None
        }
    }
}

impl BootstrapWaiter<Arc<Channel>> for ChannelWaiter {
    fn wait(&mut self, logic: &mut BootstrapLogic, _now: Timestamp) -> WaitResult<Arc<Channel>> {
        let state_changed = self.transition_state(logic);
        if let ChannelWaitState::Found(channel) = &self.state {
            WaitResult::Finished(channel.clone())
        } else if state_changed {
            WaitResult::BeginWait
        } else {
            WaitResult::ContinueWait
        }
    }
}

impl Default for ChannelWaiter {
    fn default() -> Self {
        Self::new()
    }
}
