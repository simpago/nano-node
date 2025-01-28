use crate::{
    bootstrap::{
        channel_waiter::ChannelWaiter, AscPullQuerySpec, BootstrapAction, BootstrapLogic,
        WaitResult,
    },
    stats::{DetailType, StatType, Stats},
    utils::{ThreadPool, ThreadPoolImpl},
};
use rsnano_core::{Account, BlockHash};
use rsnano_messages::{AscPullReqType, FrontiersReqPayload};
use rsnano_network::{bandwidth_limiter::RateLimiter, Channel};
use rsnano_nullable_clock::Timestamp;
use std::sync::Arc;

pub(crate) struct FrontierQuery {
    state: FrontierQueryState,
    stats: Arc<Stats>,
    frontiers_limiter: RateLimiter,
    workers: Arc<ThreadPoolImpl>,
    max_pending: usize,
}

enum FrontierQueryState {
    Initial,
    WaitCandidateAccounts,
    WaitLimiter,
    WaitWorkers,
    WaitChannel(ChannelWaiter),
    WaitFrontier(Arc<Channel>),
    Done(Arc<Channel>, AscPullReqType),
}

impl FrontierQuery {
    pub(crate) fn new(
        workers: Arc<ThreadPoolImpl>,
        stats: Arc<Stats>,
        rate_limit: usize,
        max_pending: usize,
    ) -> Self {
        Self {
            state: FrontierQueryState::Initial,
            stats,
            frontiers_limiter: RateLimiter::new(rate_limit),
            workers,
            max_pending,
        }
    }

    fn next_state(
        &mut self,
        logic: &mut BootstrapLogic,
        now: Timestamp,
    ) -> Option<FrontierQueryState> {
        match &mut self.state {
            FrontierQueryState::Initial => self.initialize(),
            FrontierQueryState::WaitCandidateAccounts => Self::wait_candidate_accounts(logic),
            FrontierQueryState::WaitLimiter => self.wait_limiter(),
            FrontierQueryState::WaitWorkers => self.wait_workers(),
            FrontierQueryState::WaitChannel(waiter) => Self::wait_channel(logic, waiter, now),
            FrontierQueryState::WaitFrontier(channel) => {
                Self::wait_frontier(logic, channel, &self.stats, now)
            }
            FrontierQueryState::Done(_, _) => None,
        }
    }

    fn initialize(&self) -> Option<FrontierQueryState> {
        self.stats
            .inc(StatType::Bootstrap, DetailType::LoopFrontiers);
        Some(FrontierQueryState::WaitCandidateAccounts)
    }

    fn wait_candidate_accounts(logic: &BootstrapLogic) -> Option<FrontierQueryState> {
        if logic.candidate_accounts.priority_half_full() {
            None
        } else {
            Some(FrontierQueryState::WaitLimiter)
        }
    }

    fn wait_limiter(&self) -> Option<FrontierQueryState> {
        if self.frontiers_limiter.should_pass(1) {
            Some(FrontierQueryState::WaitWorkers)
        } else {
            None
        }
    }

    fn wait_workers(&self) -> Option<FrontierQueryState> {
        if self.workers.num_queued_tasks() < self.max_pending {
            Some(FrontierQueryState::WaitChannel(ChannelWaiter::new()))
        } else {
            None
        }
    }

    fn wait_channel(
        logic: &mut BootstrapLogic,
        channel_waiter: &mut ChannelWaiter,
        now: Timestamp,
    ) -> Option<FrontierQueryState> {
        match channel_waiter.run(logic, now) {
            WaitResult::BeginWait => Some(FrontierQueryState::WaitChannel(channel_waiter.clone())),
            WaitResult::ContinueWait => None,
            WaitResult::Finished(channel) => Some(FrontierQueryState::WaitFrontier(channel)),
        }
    }

    fn wait_frontier(
        logic: &mut BootstrapLogic,
        channel: &Arc<Channel>,
        stats: &Stats,
        now: Timestamp,
    ) -> Option<FrontierQueryState> {
        let start = logic.account_ranges.next(now);
        if !start.is_zero() {
            stats.inc(StatType::BootstrapNext, DetailType::NextFrontier);
            let request = Self::request_frontiers(start);
            Some(FrontierQueryState::Done(channel.clone(), request))
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

impl BootstrapAction<AscPullQuerySpec> for FrontierQuery {
    fn run(&mut self, logic: &mut BootstrapLogic, now: Timestamp) -> WaitResult<AscPullQuerySpec> {
        let mut state_changed = false;
        loop {
            match self.next_state(logic, now) {
                Some(FrontierQueryState::Done(channel, request)) => {
                    self.state = FrontierQueryState::Initial;

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
