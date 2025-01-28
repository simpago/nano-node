use super::{
    channel_waiter::ChannelWaiter, AscPullQuerySpec, BootstrapAction, BootstrapLogic, WaitResult,
};
use crate::stats::{DetailType, StatType};
use rsnano_core::Account;
use rsnano_messages::{AccountInfoReqPayload, AscPullReqType, HashType};
use rsnano_network::Channel;
use rsnano_nullable_clock::Timestamp;
use std::sync::Arc;

pub(super) struct DependencyQuery {
    state: DependencyQueryState,
}

enum DependencyQueryState {
    Initial,
    WaitChannel(ChannelWaiter),
    WaitBlocking(Arc<Channel>),
    Done(AscPullQuerySpec),
}

impl DependencyQuery {
    pub(super) fn new() -> Self {
        Self {
            state: DependencyQueryState::Initial,
        }
    }
}

impl BootstrapAction<AscPullQuerySpec> for DependencyQuery {
    fn run(&mut self, logic: &mut BootstrapLogic, now: Timestamp) -> WaitResult<AscPullQuerySpec> {
        let mut state_changed = false;
        loop {
            let new_state = match &mut self.state {
                DependencyQueryState::Initial => {
                    logic
                        .stats
                        .inc(StatType::Bootstrap, DetailType::LoopDependencies);
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
                        // Query account info by block hash
                        let req_type = AscPullReqType::AccountInfo(AccountInfoReqPayload {
                            target: next.into(),
                            target_type: HashType::Block,
                        });

                        let spec = AscPullQuerySpec {
                            channel: channel.clone(),
                            req_type,
                            account: Account::zero(),
                            hash: next,
                            cooldown_account: false,
                        };

                        Some(DependencyQueryState::Done(spec))
                    }
                }
                DependencyQueryState::Done(..) => None,
            };

            match new_state {
                Some(DependencyQueryState::Done(spec)) => {
                    self.state = DependencyQueryState::Initial;
                    return WaitResult::Finished(spec);
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
