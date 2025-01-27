use rand::{thread_rng, RngCore};
use rsnano_core::Account;
use rsnano_messages::Message;
use rsnano_network::Channel;
use rsnano_nullable_clock::Timestamp;
use std::sync::Arc;

use crate::{
    bootstrap::{
        channel_waiter::ChannelWaiter, running_query_container::QuerySource, BootstrapLogic,
        BootstrapWaiter, WaitResult,
    },
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
}

impl FrontierScan {
    pub(crate) fn new() -> Self {
        Self {
            state: FrontierScanState::Initial,
        }
    }
}

impl BootstrapWaiter<()> for FrontierScan {
    fn wait(&mut self, logic: &mut BootstrapLogic, now: Timestamp) -> WaitResult<()> {
        let mut state_changed = false;
        loop {
            let new_state = match &mut self.state {
                FrontierScanState::Initial => {
                    logic
                        .stats
                        .inc(StatType::Bootstrap, DetailType::LoopFrontiers);
                    Some(FrontierScanState::WaitCandidateAccounts)
                }
                FrontierScanState::WaitCandidateAccounts => {
                    if logic.candidate_accounts.priority_half_full() {
                        None
                    } else {
                        Some(FrontierScanState::WaitLimiter)
                    }
                }
                FrontierScanState::WaitLimiter => {
                    if logic.frontiers_limiter.should_pass(1) {
                        Some(FrontierScanState::WaitWorkers)
                    } else {
                        None
                    }
                }
                FrontierScanState::WaitWorkers => {
                    if logic.workers.num_queued_tasks() < logic.config.frontier_scan.max_pending {
                        Some(FrontierScanState::WaitChannel(ChannelWaiter::new()))
                    } else {
                        None
                    }
                }
                FrontierScanState::WaitChannel(waiter) => match waiter.wait(logic, now) {
                    WaitResult::BeginWait => return WaitResult::BeginWait,
                    WaitResult::ContinueWait => None,
                    WaitResult::Finished(channel) => Some(FrontierScanState::WaitFrontier(channel)),
                },
                FrontierScanState::WaitFrontier(channel) => {
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
                FrontierScanState::Send(channel, start) => {
                    let id = thread_rng().next_u64();
                    let message = logic.request_frontiers(id, now, *start, QuerySource::Frontiers);
                    let _sent = logic.send(channel, &message, id, now);
                    // TODO slow down if queue full
                    Some(FrontierScanState::Initial)
                }
            };

            match new_state {
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
