use crate::bootstrap::state::BootstrapState;
use crate::bootstrap::{AscPullQuerySpec, BootstrapPromise, PromiseResult};
use crate::stats::{DetailType, StatType, Stats};
use rsnano_core::Account;
use rsnano_messages::{AccountInfoReqPayload, AscPullReqType, HashType};
use rsnano_network::Channel;
use std::sync::Arc;

use super::channel_waiter::ChannelWaiter;

pub(super) struct DependencyRequester {
    state: DependencyState,
    stats: Arc<Stats>,
    channel_waiter: ChannelWaiter,
}

enum DependencyState {
    Initial,
    WaitChannel,
    WaitBlocking(Arc<Channel>),
}

impl DependencyRequester {
    pub(super) fn new(stats: Arc<Stats>, channel_waiter: ChannelWaiter) -> Self {
        Self {
            state: DependencyState::Initial,
            stats,
            channel_waiter,
        }
    }
}

impl BootstrapPromise<AscPullQuerySpec> for DependencyRequester {
    fn poll(&mut self, state: &mut BootstrapState) -> PromiseResult<AscPullQuerySpec> {
        match self.state {
            DependencyState::Initial => {
                self.stats
                    .inc(StatType::Bootstrap, DetailType::LoopDependencies);
                self.state = DependencyState::WaitChannel;
                return PromiseResult::Progress;
            }
            DependencyState::WaitChannel => match self.channel_waiter.poll(state) {
                PromiseResult::Wait => return PromiseResult::Wait,
                PromiseResult::Progress => return PromiseResult::Progress,
                PromiseResult::Finished(channel) => {
                    self.state = DependencyState::WaitBlocking(channel);
                    return PromiseResult::Progress;
                }
            },
            DependencyState::WaitBlocking(ref channel) => {
                let next = state.next_blocking();
                if !next.is_zero() {
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

                    self.state = DependencyState::Initial;
                    return PromiseResult::Finished(spec);
                }
            }
        };

        PromiseResult::Wait
    }
}
