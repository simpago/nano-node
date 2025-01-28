use crate::bootstrap::{
    channel_waiter::ChannelWaiter, AscPullQuerySpec, BootstrapAction, BootstrapState, WaitResult,
};
use crate::stats::{DetailType, StatType, Stats};
use rsnano_core::Account;
use rsnano_messages::{AccountInfoReqPayload, AscPullReqType, HashType};
use rsnano_network::Channel;
use rsnano_nullable_clock::Timestamp;
use std::sync::Arc;

pub(super) struct DependencyRequester {
    state: DependencyState,
    stats: Arc<Stats>,
    channel_waiter: Arc<dyn Fn() -> ChannelWaiter + Send + Sync>,
}

enum DependencyState {
    Initial,
    WaitChannel(ChannelWaiter),
    WaitBlocking(Arc<Channel>),
    Done(AscPullQuerySpec),
}

impl DependencyRequester {
    pub(super) fn new(
        stats: Arc<Stats>,
        channel_waiter: Arc<dyn Fn() -> ChannelWaiter + Send + Sync>,
    ) -> Self {
        Self {
            state: DependencyState::Initial,
            stats,
            channel_waiter,
        }
    }
}

impl BootstrapAction<AscPullQuerySpec> for DependencyRequester {
    fn run(&mut self, state: &mut BootstrapState, now: Timestamp) -> WaitResult<AscPullQuerySpec> {
        let mut state_changed = false;
        loop {
            let new_state = match &mut self.state {
                DependencyState::Initial => {
                    self.stats
                        .inc(StatType::Bootstrap, DetailType::LoopDependencies);
                    let waiter = (self.channel_waiter)();
                    Some(DependencyState::WaitChannel(waiter))
                }
                DependencyState::WaitChannel(waiter) => match waiter.run(state, now) {
                    WaitResult::BeginWait => Some(DependencyState::WaitChannel(waiter.clone())),
                    WaitResult::ContinueWait => None,
                    WaitResult::Finished(channel) => Some(DependencyState::WaitBlocking(channel)),
                },
                DependencyState::WaitBlocking(channel) => {
                    let next = state.next_blocking();
                    if next.is_zero() {
                        None
                    } else {
                        self.stats
                            .inc(StatType::BootstrapNext, DetailType::NextBlocking);

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

                        Some(DependencyState::Done(spec))
                    }
                }
                DependencyState::Done(..) => None,
            };

            match new_state {
                Some(DependencyState::Done(spec)) => {
                    self.state = DependencyState::Initial;
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
