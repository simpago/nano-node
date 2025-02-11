use super::channel_waiter::ChannelWaiter;
use crate::bootstrap::state::BootstrapState;
use crate::bootstrap::{AscPullQuerySpec, BootstrapPromise, PollResult};
use crate::stats::{DetailType, StatType, Stats};
use rsnano_network::Channel;
use std::sync::Arc;

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
    fn poll(&mut self, state: &mut BootstrapState) -> PollResult<AscPullQuerySpec> {
        match self.state {
            DependencyState::Initial => {
                self.stats
                    .inc(StatType::Bootstrap, DetailType::LoopDependencies);
                self.state = DependencyState::WaitChannel;
                PollResult::Progress
            }
            DependencyState::WaitChannel => match self.channel_waiter.poll(state) {
                PollResult::Wait => PollResult::Wait,
                PollResult::Progress => PollResult::Progress,
                PollResult::Finished(channel) => {
                    self.state = DependencyState::WaitBlocking(channel);
                    PollResult::Progress
                }
            },
            DependencyState::WaitBlocking(ref channel) => {
                if let Some(spec) = state.next_blocking_query(channel) {
                    self.stats
                        .inc(StatType::BootstrapNext, DetailType::NextBlocking);
                    self.state = DependencyState::Initial;
                    PollResult::Finished(spec)
                } else {
                    PollResult::Wait
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::progress;
    use rsnano_core::{Account, BlockHash};
    use rsnano_network::{bandwidth_limiter::RateLimiter, Network};
    use std::sync::RwLock;

    #[test]
    fn happy_path() {
        let network = test_network();
        network.write().unwrap().add_test_channel();
        let mut requester = create_test_requester(network);
        let mut state = BootstrapState::default();

        let account = Account::from(1);
        let dependency = BlockHash::from(2);
        state.candidate_accounts.priority_up(&account);
        state.candidate_accounts.block(account, dependency);

        let result = progress(&mut requester, &mut state);

        let PollResult::Finished(spec) = result else {
            panic!("poll did not finish");
        };

        assert_eq!(spec.hash, dependency);
    }

    #[test]
    fn wait_channel() {
        let network = test_network();
        let mut requester = create_test_requester(network.clone());
        let mut state = BootstrapState::default();

        let result = progress(&mut requester, &mut state);
        assert!(matches!(result, PollResult::Wait));
        assert!(matches!(requester.state, DependencyState::WaitChannel));

        network.write().unwrap().add_test_channel();
        let result = requester.poll(&mut state);
        assert!(matches!(result, PollResult::Progress));
        assert!(matches!(requester.state, DependencyState::WaitBlocking(_)));
    }

    #[test]
    fn wait_dependency() {
        let network = test_network();
        network.write().unwrap().add_test_channel();
        let mut requester = create_test_requester(network);
        let mut state = BootstrapState::default();

        let result = progress(&mut requester, &mut state);
        assert!(matches!(result, PollResult::Wait));
        assert!(matches!(requester.state, DependencyState::WaitBlocking(_)));

        let account = Account::from(1);
        let dependency = BlockHash::from(2);
        state.candidate_accounts.priority_up(&account);
        state.candidate_accounts.block(account, dependency);

        let result = requester.poll(&mut state);
        assert!(matches!(result, PollResult::Finished(_)));
        assert!(matches!(requester.state, DependencyState::Initial));
    }

    fn create_test_requester(network: Arc<RwLock<Network>>) -> DependencyRequester {
        let stats = Arc::new(Stats::default());
        let limiter = Arc::new(RateLimiter::new(1024));
        let channel_waiter = ChannelWaiter::new(network, limiter, 1024);
        DependencyRequester::new(stats, channel_waiter)
    }

    fn test_network() -> Arc<RwLock<Network>> {
        Arc::new(RwLock::new(Network::new_test_instance()))
    }
}
