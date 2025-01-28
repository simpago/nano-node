use std::sync::Arc;

use super::{
    channel_waiter::ChannelWaiter, AscPullQuerySpec, BootstrapAction, BootstrapLogic, WaitResult,
};
use rsnano_core::BlockHash;
use rsnano_network::Channel;
use rsnano_nullable_clock::Timestamp;

pub(super) struct DependencyQuery {
    state: DependencyQueryState,
}

enum DependencyQueryState {
    Initial,
    WaitChannel(ChannelWaiter),
    WaitBlocking(Arc<Channel>),
    Done(Arc<Channel>, BlockHash),
}

impl DependencyQuery {
    pub(super) fn new() -> Self {
        Self {
            state: DependencyQueryState::Initial,
        }
    }
}

impl BootstrapAction<(Arc<Channel>, BlockHash)> for DependencyQuery {
    fn run(
        &mut self,
        logic: &mut BootstrapLogic,
        now: Timestamp,
    ) -> WaitResult<(Arc<Channel>, BlockHash)> {
        let mut state_changed = false;
        loop {
            let new_state = match &mut self.state {
                DependencyQueryState::Initial => {
                    Some(DependencyQueryState::WaitChannel(ChannelWaiter::new()))
                }
                DependencyQueryState::WaitChannel(waiter) => match waiter.run(logic, now) {
                    WaitResult::BeginWait => {
                        Some(DependencyQueryState::WaitChannel(waiter.clone()))
                    }
                    WaitResult::ContinueWait => None,
                    WaitResult::Finished(channel) => {
                        Some(DependencyQueryState::WaitBlocking(channel))
                    }
                },
                DependencyQueryState::WaitBlocking(channel) => {
                    let next = logic.next_blocking();
                    if next.is_zero() {
                        None
                    } else {
                        //let req_type = rsnano_messages::AscPullReqType::Blocks(());
                        //let spec = AscPullQuerySpec {
                        //    channel: channel.clone(),
                        //    req_type,
                        //    account: todo!(),
                        //    hash: todo!(),
                        //    cooldown_account: todo!(),
                        //};

                        Some(DependencyQueryState::Done(channel.clone(), next))
                    }
                }
                DependencyQueryState::Done(..) => None,
            };

            match new_state {
                Some(DependencyQueryState::Done(channel, hash)) => {
                    return WaitResult::Finished((channel, hash));
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
