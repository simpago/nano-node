use super::{
    channel_waiter::ChannelWaiter, BootstrapAction, BootstrapLogic, PriorityResult, WaitResult,
};
use crate::block_processing::BlockSource;
use rsnano_network::Channel;
use rsnano_nullable_clock::Timestamp;
use std::sync::Arc;

pub(super) struct PriorityQuery {
    state: PriorityState,
}

impl PriorityQuery {
    pub(super) fn new() -> Self {
        Self {
            state: PriorityState::Initial,
        }
    }
}

enum PriorityState {
    Initial,
    WaitBlockProcessor,
    WaitChannel(ChannelWaiter),
    WaitPriority(Arc<Channel>),
    Done(Arc<Channel>, PriorityResult),
}

impl BootstrapAction<(Arc<Channel>, PriorityResult)> for PriorityQuery {
    fn run(
        &mut self,
        logic: &mut BootstrapLogic,
        now: Timestamp,
    ) -> WaitResult<(Arc<Channel>, PriorityResult)> {
        let mut state_changed = false;
        loop {
            let new_state = match &mut self.state {
                PriorityState::Initial => Some(PriorityState::WaitBlockProcessor),
                PriorityState::WaitBlockProcessor => {
                    if logic.block_processor.queue_len(BlockSource::Bootstrap)
                        < logic.config.block_processor_theshold
                    {
                        let channel_waiter = ChannelWaiter::new();
                        Some(PriorityState::WaitChannel(channel_waiter))
                    } else {
                        None
                    }
                }
                PriorityState::WaitChannel(waiter) => match waiter.run(logic, now) {
                    WaitResult::BeginWait => Some(PriorityState::WaitChannel(waiter.clone())),
                    WaitResult::ContinueWait => None,
                    WaitResult::Finished(channel) => Some(PriorityState::WaitPriority(channel)),
                },
                PriorityState::WaitPriority(channel) => {
                    let next = logic.next_priority(now);
                    if !next.account.is_zero() {
                        Some(PriorityState::Done(channel.clone(), next))
                    } else {
                        None
                    }
                }
                PriorityState::Done(_, _) => None,
            };

            match new_state {
                Some(PriorityState::Done(channel, next)) => {
                    return WaitResult::Finished((channel.clone(), next));
                }
                Some(s) => {
                    self.state = s;
                    state_changed = true;
                }
                None => break,
            }
        }

        if state_changed {
            WaitResult::BeginWait
        } else {
            WaitResult::ContinueWait
        }
    }
}
