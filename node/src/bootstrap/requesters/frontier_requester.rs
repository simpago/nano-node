use super::channel_waiter::ChannelWaiter;
use crate::{
    bootstrap::{state::BootstrapState, AscPullQuerySpec, BootstrapAction, WaitResult},
    stats::{DetailType, StatType, Stats},
    utils::ThreadPool,
};
use rsnano_core::{Account, BlockHash};
use rsnano_messages::{AscPullReqType, FrontiersReqPayload};
use rsnano_network::{bandwidth_limiter::RateLimiter, Channel};
use rsnano_nullable_clock::Timestamp;
use std::sync::Arc;

pub(crate) struct FrontierRequester {
    state: FrontierState,
    stats: Arc<Stats>,
    frontiers_limiter: RateLimiter,
    workers: Arc<dyn ThreadPool>,
    max_pending: usize,
    channel_waiter: Arc<dyn Fn() -> ChannelWaiter + Send + Sync>,
}

enum FrontierState {
    Initial,
    WaitCandidateAccounts,
    WaitLimiter,
    WaitWorkers,
    WaitChannel(ChannelWaiter),
    WaitFrontier(Arc<Channel>),
    Done(Arc<Channel>, AscPullReqType),
}

impl FrontierRequester {
    pub(crate) fn new(
        workers: Arc<dyn ThreadPool>,
        stats: Arc<Stats>,
        rate_limit: usize,
        max_pending: usize,
        channel_waiter: Arc<dyn Fn() -> ChannelWaiter + Send + Sync>,
    ) -> Self {
        Self {
            state: FrontierState::Initial,
            stats,
            frontiers_limiter: RateLimiter::new(rate_limit),
            workers,
            max_pending,
            channel_waiter,
        }
    }

    fn next_state(&mut self, state: &mut BootstrapState, now: Timestamp) -> Option<FrontierState> {
        match &mut self.state {
            FrontierState::Initial => self.initialize(),
            FrontierState::WaitCandidateAccounts => Self::wait_candidate_accounts(state),
            FrontierState::WaitLimiter => self.wait_limiter(),
            FrontierState::WaitWorkers => self.wait_workers(),
            FrontierState::WaitChannel(waiter) => Self::wait_channel(state, waiter, now),
            FrontierState::WaitFrontier(channel) => {
                Self::wait_frontier(state, channel, &self.stats, now)
            }
            FrontierState::Done(_, _) => None,
        }
    }

    fn initialize(&self) -> Option<FrontierState> {
        self.stats
            .inc(StatType::Bootstrap, DetailType::LoopFrontiers);
        Some(FrontierState::WaitCandidateAccounts)
    }

    fn wait_candidate_accounts(state: &BootstrapState) -> Option<FrontierState> {
        if state.candidate_accounts.priority_half_full() {
            None
        } else {
            Some(FrontierState::WaitLimiter)
        }
    }

    fn wait_limiter(&self) -> Option<FrontierState> {
        if self.frontiers_limiter.should_pass(1) {
            Some(FrontierState::WaitWorkers)
        } else {
            None
        }
    }

    fn wait_workers(&self) -> Option<FrontierState> {
        if self.workers.num_queued_tasks() < self.max_pending {
            let waiter = (self.channel_waiter)();
            Some(FrontierState::WaitChannel(waiter))
        } else {
            None
        }
    }

    fn wait_channel(
        state: &mut BootstrapState,
        channel_waiter: &mut ChannelWaiter,
        now: Timestamp,
    ) -> Option<FrontierState> {
        match channel_waiter.run(state, now) {
            WaitResult::BeginWait => Some(FrontierState::WaitChannel(channel_waiter.clone())),
            WaitResult::ContinueWait => None,
            WaitResult::Finished(channel) => Some(FrontierState::WaitFrontier(channel)),
        }
    }

    fn wait_frontier(
        state: &mut BootstrapState,
        channel: &Arc<Channel>,
        stats: &Stats,
        now: Timestamp,
    ) -> Option<FrontierState> {
        let start = state.account_ranges.next(now);
        if !start.is_zero() {
            stats.inc(StatType::BootstrapNext, DetailType::NextFrontier);
            let request = Self::request_frontiers(start);
            Some(FrontierState::Done(channel.clone(), request))
        } else {
            None
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
    fn run(&mut self, state: &mut BootstrapState, now: Timestamp) -> WaitResult<AscPullQuerySpec> {
        let mut state_changed = false;
        loop {
            match self.next_state(state, now) {
                Some(FrontierState::Done(channel, request)) => {
                    self.state = FrontierState::Initial;

                    let spec = AscPullQuerySpec {
                        channel,
                        req_type: request,
                        account: Account::zero(),
                        hash: BlockHash::zero(),
                        cooldown_account: false,
                    };
                    return WaitResult::Finished(spec);
                }
                Some(new_state) => {
                    self.state = new_state;
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

        let result = requester.run(&mut state, Timestamp::new_test_instance());

        let WaitResult::Finished(req) = result else {
            panic!("requester didn't finish'");
        };
        assert!(matches!(req.req_type, AscPullReqType::Frontiers(_)));
    }

    #[test]
    fn wait_candidate_accounts() {
        let mut requester = create_test_requester();
        let mut state = state_with_max_priorities(1);
        // Fill up candidate accounts
        state.candidate_accounts.priority_up(&Account::from(1));

        // Should wait because candidate accounts are full enough
        let result = requester.run(&mut state, Timestamp::new_test_instance());
        assert!(matches!(result, WaitResult::BeginWait));
        assert!(matches!(
            requester.state,
            FrontierState::WaitCandidateAccounts
        ));

        // Running again continues waiting
        let result = requester.run(&mut state, Timestamp::new_test_instance());
        assert!(matches!(result, WaitResult::ContinueWait));

        // If the accounts are cleared, continue
        state.candidate_accounts.clear();
        let result = requester.run(&mut state, Timestamp::new_test_instance());
        assert!(matches!(result, WaitResult::BeginWait));
        assert!(matches!(requester.state, FrontierState::WaitChannel(_)));
    }

    #[test]
    fn wait_limiter() {
        let mut requester = create_test_requester();
        let mut state = BootstrapState::default();
        requester.frontiers_limiter.should_pass(TEST_RATE_LIMIT);

        // Should wait because rate limit reached
        let result = requester.run(&mut state, Timestamp::new_test_instance());
        assert!(matches!(result, WaitResult::BeginWait));
        assert!(matches!(requester.state, FrontierState::WaitLimiter));

        // Running again continues waiting
        let result = requester.run(&mut state, Timestamp::new_test_instance());
        assert!(matches!(result, WaitResult::ContinueWait));

        // Continue when the limiter is emptied
        requester.frontiers_limiter.reset();
        let result = requester.run(&mut state, Timestamp::new_test_instance());
        assert!(matches!(result, WaitResult::BeginWait));
        assert!(matches!(requester.state, FrontierState::WaitChannel(_)));
    }

    // Test helpers:

    const TEST_RATE_LIMIT: usize = 1000;

    fn create_test_requester() -> FrontierRequester {
        let workers = Arc::new(ThreadPoolImpl::new_null());
        let stats = Arc::new(Stats::default());
        let waiter = Arc::new(|| ChannelWaiter::default());
        FrontierRequester::new(workers, stats.clone(), TEST_RATE_LIMIT, 1000, waiter)
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
