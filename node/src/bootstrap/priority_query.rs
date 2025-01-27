use super::{channel_waiter::ChannelWaiter, BootstrapAction, BootstrapLogic, WaitResult};
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
    Done(Arc<Channel>),
}

impl BootstrapAction<Arc<Channel>> for PriorityQuery {
    fn run(&mut self, logic: &mut BootstrapLogic, now: Timestamp) -> WaitResult<Arc<Channel>> {
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
                WaitResult::Finished(channel) => Some(PriorityState::Done(channel)),
            },
            PriorityState::Done(channel) => return WaitResult::Finished(channel.clone()),
        };

        match new_state {
            Some(s) => {
                self.state = s;
                WaitResult::BeginWait
            }
            None => WaitResult::ContinueWait,
        }
    }
}
