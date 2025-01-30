use super::channel_waiter::ChannelWaiter;
use crate::{
    bootstrap::{state::BootstrapState, AscPullQuerySpec, BootstrapAction, WaitResult},
    stats::{DetailType, StatType, Stats},
    utils::ThreadPool,
};
use rsnano_core::{Account, BlockHash};
use rsnano_messages::{AscPullReqType, FrontiersReqPayload};
use rsnano_network::{bandwidth_limiter::RateLimiter, Channel};
use rsnano_nullable_clock::SteadyClock;
use std::sync::Arc;

pub(crate) struct FrontierRequester {
    state: FrontierState,
    stats: Arc<Stats>,
    clock: Arc<SteadyClock>,
    frontiers_limiter: RateLimiter,
    workers: Arc<dyn ThreadPool>,
    max_pending: usize,
    channel_waiter: ChannelWaiter,
}

enum FrontierState {
    Initial,
    WaitCandidateAccounts,
    WaitLimiter,
    WaitWorkers,
    WaitChannel,
    WaitFrontier(Arc<Channel>),
}

impl FrontierRequester {
    pub(crate) fn new(
        workers: Arc<dyn ThreadPool>,
        stats: Arc<Stats>,
        clock: Arc<SteadyClock>,
        rate_limit: usize,
        max_pending: usize,
        channel_waiter: ChannelWaiter,
    ) -> Self {
        Self {
            state: FrontierState::Initial,
            stats,
            clock,
            frontiers_limiter: RateLimiter::new(rate_limit),
            workers,
            max_pending,
            channel_waiter,
        }
    }

    fn create_query_spec(channel: &Arc<Channel>, start: Account) -> AscPullQuerySpec {
        let request = Self::request_frontiers(start);
        AscPullQuerySpec {
            channel: channel.clone(),
            req_type: request,
            account: Account::zero(),
            hash: BlockHash::zero(),
            cooldown_account: false,
        }
    }

    fn request_frontiers(start: Account) -> AscPullReqType {
        AscPullReqType::Frontiers(FrontiersReqPayload {
            start,
            count: FrontiersReqPayload::MAX_FRONTIERS,
        })
    }
}

impl BootstrapAction<AscPullQuerySpec> for FrontierRequester {
    fn run(&mut self, boot_state: &mut BootstrapState) -> WaitResult<AscPullQuerySpec> {
        match self.state {
            FrontierState::Initial => {
                self.stats
                    .inc(StatType::Bootstrap, DetailType::LoopFrontiers);
                self.state = FrontierState::WaitCandidateAccounts;
                return WaitResult::Progress;
            }
            FrontierState::WaitCandidateAccounts => {
                if !boot_state.candidate_accounts.priority_half_full() {
                    self.state = FrontierState::WaitLimiter;
                    return WaitResult::Progress;
                }
            }
            FrontierState::WaitLimiter => {
                if self.frontiers_limiter.should_pass(1) {
                    self.state = FrontierState::WaitWorkers;
                    return WaitResult::Progress;
                }
            }
            FrontierState::WaitWorkers => {
                if self.workers.num_queued_tasks() < self.max_pending {
                    self.state = FrontierState::WaitChannel;
                    return WaitResult::Progress;
                }
            }
            FrontierState::WaitChannel => match self.channel_waiter.run(boot_state) {
                WaitResult::Wait => return WaitResult::Wait,
                WaitResult::Progress => return WaitResult::Progress,
                WaitResult::Finished(channel) => {
                    self.state = FrontierState::WaitFrontier(channel);
                    return WaitResult::Progress;
                }
            },
            FrontierState::WaitFrontier(ref channel) => {
                let now = self.clock.now();
                let start = boot_state.account_ranges.next(now);
                if !start.is_zero() {
                    self.stats
                        .inc(StatType::BootstrapNext, DetailType::NextFrontier);
                    let spec = Self::create_query_spec(channel, start);
                    self.state = FrontierState::Initial;
                    return WaitResult::Finished(spec);
                }
            }
        }
        WaitResult::Wait
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        bootstrap::{state::CandidateAccountsConfig, BootstrapConfig},
        utils::ThreadPoolImpl,
    };

    #[test]
    fn happy_path() {
        let mut requester = create_test_requester();
        let mut state = BootstrapState::default();
        add_test_channel_to(&mut state);

        let result = loop {
            match requester.run(&mut state) {
                WaitResult::Progress => {}
                WaitResult::Wait => {
                    panic!("should never wait")
                }
                WaitResult::Finished(i) => break i,
            }
        };

        assert!(matches!(result.req_type, AscPullReqType::Frontiers(_)));
    }

    #[test]
    fn wait_candidate_accounts() {
        let mut requester = create_test_requester();
        let mut state = state_with_max_priorities(1);

        assert!(matches!(requester.run(&mut state), WaitResult::Progress)); // initial

        // Fill up candidate accounts
        state.candidate_accounts.priority_up(&Account::from(1));

        // Should wait because candidate accounts are full enough
        let result = requester.run(&mut state);
        assert!(matches!(result, WaitResult::Wait));
        assert!(matches!(
            requester.state,
            FrontierState::WaitCandidateAccounts
        ));

        // Running again continues waiting
        let result = requester.run(&mut state);
        assert!(matches!(result, WaitResult::Wait));

        // If the accounts are cleared, continue
        state.candidate_accounts.clear();
        let result = requester.run(&mut state);
        assert!(matches!(result, WaitResult::Progress));
        assert!(matches!(requester.state, FrontierState::WaitLimiter));
    }

    #[test]
    fn wait_limiter() {
        let mut requester = create_test_requester();
        let mut state = BootstrapState::default();

        assert!(matches!(requester.run(&mut state), WaitResult::Progress)); // initial
        assert!(matches!(requester.run(&mut state), WaitResult::Progress)); // candidate accounts

        // Should wait because rate limit reached
        requester.frontiers_limiter.should_pass(TEST_RATE_LIMIT);
        let result = requester.run(&mut state);
        assert!(matches!(result, WaitResult::Wait));
        assert!(matches!(requester.state, FrontierState::WaitLimiter));

        // Running again continues waiting
        let result = requester.run(&mut state);
        assert!(matches!(result, WaitResult::Wait));

        // Continue when the limiter is emptied
        requester.frontiers_limiter.reset();
        let result = requester.run(&mut state);
        assert!(matches!(result, WaitResult::Progress));
        assert!(matches!(requester.state, FrontierState::WaitWorkers));
    }

    // Test helpers:

    const TEST_RATE_LIMIT: usize = 1000;

    fn create_test_requester() -> FrontierRequester {
        let workers = Arc::new(ThreadPoolImpl::new_null());
        let stats = Arc::new(Stats::default());
        let waiter = ChannelWaiter::default();
        let clock = Arc::new(SteadyClock::new_null());
        FrontierRequester::new(workers, stats.clone(), clock, TEST_RATE_LIMIT, 1000, waiter)
    }

    fn state_with_max_priorities(max: usize) -> BootstrapState {
        let config = BootstrapConfig {
            candidate_accounts: CandidateAccountsConfig {
                priorities_max: max,
                ..Default::default()
            },
            ..Default::default()
        };
        BootstrapState::new(config, Arc::new(Stats::default()))
    }

    fn add_test_channel_to(state: &mut BootstrapState) {
        state
            .scoring
            .sync(vec![Arc::new(Channel::new_test_instance())]);
    }
}
