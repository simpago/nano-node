use rsnano_core::Account;
use rsnano_messages::{AscPullReqType, FrontiersReqPayload};
use rsnano_network::Channel;
use rsnano_nullable_clock::Timestamp;
use std::sync::Arc;

use crate::{
    bootstrap::{channel_waiter::ChannelWaiter, BootstrapAction, BootstrapLogic, WaitResult},
    stats::{DetailType, StatType},
    utils::ThreadPool,
};

pub(crate) struct FrontierScan {
    state: FrontierScanState,
}

enum FrontierScanState {
    Initial,
    WaitCandidateAccounts,
    WaitLimiter,
    WaitWorkers,
    WaitChannel(ChannelWaiter),
    WaitFrontier(Arc<Channel>),
    Send(Arc<Channel>, Account),
    Done(Arc<Channel>, AscPullReqType),
}

impl FrontierScan {
    pub(crate) fn new() -> Self {
        Self {
            state: FrontierScanState::Initial,
        }
    }

    fn next_state(
        &mut self,
        logic: &mut BootstrapLogic,
        now: Timestamp,
    ) -> Option<FrontierScanState> {
        match &mut self.state {
            FrontierScanState::Initial => Self::initialize(logic),
            FrontierScanState::WaitCandidateAccounts => Self::wait_candidate_accounts(logic),
            FrontierScanState::WaitLimiter => Self::wait_limiter(logic),
            FrontierScanState::WaitWorkers => Self::wait_workers(logic),
            FrontierScanState::WaitChannel(waiter) => Self::wait_channel(logic, waiter, now),
            FrontierScanState::WaitFrontier(channel) => Self::wait_frontier(logic, channel, now),
            FrontierScanState::Send(channel, start) => Self::send(channel.clone(), *start),
            FrontierScanState::Done(_, _) => None,
        }
    }

    fn initialize(logic: &BootstrapLogic) -> Option<FrontierScanState> {
        logic
            .stats
            .inc(StatType::Bootstrap, DetailType::LoopFrontiers);
        Some(FrontierScanState::WaitCandidateAccounts)
    }

    fn wait_candidate_accounts(logic: &BootstrapLogic) -> Option<FrontierScanState> {
        if logic.candidate_accounts.priority_half_full() {
            None
        } else {
            Some(FrontierScanState::WaitLimiter)
        }
    }

    fn wait_limiter(logic: &BootstrapLogic) -> Option<FrontierScanState> {
        if logic.frontiers_limiter.should_pass(1) {
            Some(FrontierScanState::WaitWorkers)
        } else {
            None
        }
    }

    fn wait_workers(logic: &BootstrapLogic) -> Option<FrontierScanState> {
        if logic.workers.num_queued_tasks() < logic.config.frontier_scan.max_pending {
            Some(FrontierScanState::WaitChannel(ChannelWaiter::new()))
        } else {
            None
        }
    }

    fn wait_channel(
        logic: &mut BootstrapLogic,
        waiter: &mut ChannelWaiter,
        now: Timestamp,
    ) -> Option<FrontierScanState> {
        match waiter.run(logic, now) {
            WaitResult::BeginWait => Some(FrontierScanState::WaitChannel(waiter.clone())),
            WaitResult::ContinueWait => None,
            WaitResult::Finished(channel) => Some(FrontierScanState::WaitFrontier(channel)),
        }
    }

    fn wait_frontier(
        logic: &mut BootstrapLogic,
        channel: &Arc<Channel>,
        now: Timestamp,
    ) -> Option<FrontierScanState> {
        let start = logic.account_ranges.next(now);
        if !start.is_zero() {
            logic
                .stats
                .inc(StatType::BootstrapNext, DetailType::NextFrontier);
            Some(FrontierScanState::Send(channel.clone(), start))
        } else {
            None
        }
    }

    fn send(channel: Arc<Channel>, start: Account) -> Option<FrontierScanState> {
        Some(FrontierScanState::Done(
            channel,
            Self::request_frontiers(start),
        ))
    }

    fn request_frontiers(start: Account) -> AscPullReqType {
        AscPullReqType::Frontiers(FrontiersReqPayload {
            start,
            count: FrontiersReqPayload::MAX_FRONTIERS,
        })
    }
}

impl BootstrapAction<(Arc<Channel>, AscPullReqType)> for FrontierScan {
    fn run(
        &mut self,
        logic: &mut BootstrapLogic,
        now: Timestamp,
    ) -> WaitResult<(Arc<Channel>, AscPullReqType)> {
        let mut state_changed = false;
        loop {
            match self.next_state(logic, now) {
                Some(FrontierScanState::Done(channel, request)) => {
                    return WaitResult::Finished((channel, request))
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
