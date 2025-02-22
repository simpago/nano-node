use super::channel_waiter::ChannelWaiter;
use crate::{
    bootstrap::{state::BootstrapState, AscPullQuerySpec, BootstrapPromise, PollResult},
    stats::{DetailType, StatType, Stats},
};
use rsnano_core::{Account, BlockHash};
use rsnano_messages::{AscPullReqType, FrontiersReqPayload};
use rsnano_network::{bandwidth_limiter::RateLimiter, Channel};
use rsnano_nullable_clock::SteadyClock;
use std::sync::Arc;

/// Creates frontier requests as specified by the frontier scanner
pub(crate) struct FrontierRequester {
    state: FrontierState,
    stats: Arc<Stats>,
    clock: Arc<SteadyClock>,
    frontiers_limiter: RateLimiter,
    channel_waiter: ChannelWaiter,
}

enum FrontierState {
    Initial,
    WaitCandidateAccounts,
    WaitLimiter,
    WaitAckProcessor,
    WaitChannel,
    WaitFrontier(Arc<Channel>),
}

impl FrontierRequester {
    pub(crate) fn new(
        stats: Arc<Stats>,
        clock: Arc<SteadyClock>,
        rate_limit: usize,
        channel_waiter: ChannelWaiter,
    ) -> Self {
        Self {
            state: FrontierState::Initial,
            stats,
            clock,
            frontiers_limiter: RateLimiter::new(rate_limit),
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

impl BootstrapPromise<AscPullQuerySpec> for FrontierRequester {
    fn poll(&mut self, boot_state: &mut BootstrapState) -> PollResult<AscPullQuerySpec> {
        match self.state {
            FrontierState::Initial => {
                self.stats
                    .inc(StatType::Bootstrap, DetailType::LoopFrontiers);
                self.state = FrontierState::WaitCandidateAccounts;
                return PollResult::Progress;
            }
            FrontierState::WaitCandidateAccounts => {
                if !boot_state.candidate_accounts.priority_half_full() {
                    self.state = FrontierState::WaitLimiter;
                    return PollResult::Progress;
                }
            }
            FrontierState::WaitLimiter => {
                if self.frontiers_limiter.should_pass(1) {
                    self.state = FrontierState::WaitAckProcessor;
                    return PollResult::Progress;
                }
            }
            FrontierState::WaitAckProcessor => {
                if !boot_state.frontier_ack_processor_busy {
                    self.state = FrontierState::WaitChannel;
                    return PollResult::Progress;
                }
            }
            FrontierState::WaitChannel => match self.channel_waiter.poll(boot_state) {
                PollResult::Wait => return PollResult::Wait,
                PollResult::Progress => return PollResult::Progress,
                PollResult::Finished(channel) => {
                    self.state = FrontierState::WaitFrontier(channel);
                    return PollResult::Progress;
                }
            },
            FrontierState::WaitFrontier(ref channel) => {
                let now = self.clock.now();
                let start = boot_state.frontier_scan.next(now);
                if !start.is_zero() {
                    self.stats
                        .inc(StatType::BootstrapNext, DetailType::NextFrontier);
                    let spec = Self::create_query_spec(channel, start);
                    self.state = FrontierState::Initial;
                    return PollResult::Finished(spec);
                } else {
                    self.stats
                        .inc(StatType::BootstrapFrontierScan, DetailType::NextNone);
                }
            }
        }
        PollResult::Wait
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::{
        progress,
        state::{CandidateAccountsConfig, FrontierScan},
        BootstrapConfig,
    };
    use rsnano_network::Network;
    use std::sync::RwLock;

    #[test]
    fn happy_path() {
        let (mut requester, network) = create_test_requester();
        let mut state = BootstrapState::default();
        network.write().unwrap().add_test_channel();

        let PollResult::Finished(result) = progress(&mut requester, &mut state) else {
            panic!("promise did not finish!")
        };

        assert!(matches!(result.req_type, AscPullReqType::Frontiers(_)));
    }

    #[test]
    fn wait_candidate_accounts() {
        let (mut requester, _) = create_test_requester();
        let mut state = state_with_max_priorities(1);

        // Fill up candidate accounts
        state.candidate_accounts.priority_up(&Account::from(1));

        // Should wait because candidate accounts are full enough
        let result = progress(&mut requester, &mut state);
        assert!(matches!(result, PollResult::Wait));
        assert!(matches!(
            requester.state,
            FrontierState::WaitCandidateAccounts
        ));

        // Running again continues waiting
        let result = requester.poll(&mut state);
        assert!(matches!(result, PollResult::Wait));

        // If the accounts are cleared, continue
        state.candidate_accounts.clear();
        let result = requester.poll(&mut state);
        assert!(matches!(result, PollResult::Progress));
        assert!(matches!(requester.state, FrontierState::WaitLimiter));
    }

    #[test]
    fn wait_limiter() {
        let (mut requester, _) = create_test_requester();
        let mut state = BootstrapState::default();

        // Should wait because rate limit reached
        requester.frontiers_limiter.should_pass(TEST_RATE_LIMIT);

        let result = progress(&mut requester, &mut state);
        assert!(matches!(result, PollResult::Wait));
        assert!(matches!(requester.state, FrontierState::WaitLimiter));

        // Running again continues waiting
        let result = requester.poll(&mut state);
        assert!(matches!(result, PollResult::Wait));

        // Continue when the limiter is emptied
        requester.frontiers_limiter.reset();
        let result = requester.poll(&mut state);
        assert!(matches!(result, PollResult::Progress));
        assert!(matches!(requester.state, FrontierState::WaitAckProcessor));
    }

    #[test]
    fn wait_channel() {
        let (mut requester, network) = create_test_requester();
        let mut state = BootstrapState::default();

        let result = progress(&mut requester, &mut state);
        assert!(matches!(result, PollResult::Wait));
        assert!(matches!(requester.state, FrontierState::WaitChannel));

        // Running again continues waiting
        let result = requester.poll(&mut state);
        assert!(matches!(result, PollResult::Wait));

        network.write().unwrap().add_test_channel();
        let result = requester.poll(&mut state);
        assert!(matches!(result, PollResult::Progress));
    }

    #[test]
    fn finish_request() {
        let (mut requester, network) = create_test_requester();
        let mut state = BootstrapState::default();
        network.write().unwrap().add_test_channel();

        let PollResult::Finished(spec) = progress(&mut requester, &mut state) else {
            panic!("did not finish");
        };

        let AscPullReqType::Frontiers(frontiers) = spec.req_type else {
            panic!("not a frontier request");
        };

        assert_eq!(frontiers.start, Account::from(1));
        assert!(matches!(requester.state, FrontierState::Initial));
    }

    #[test]
    fn wait_when_frontier_scan_rate_limited() {
        let (mut requester, network) = create_test_requester();
        let mut state = BootstrapState::default();
        network.write().unwrap().add_test_channel();
        state.frontier_scan = FrontierScan::new_test_instance_blocked();

        let result = progress(&mut requester, &mut state);

        assert!(matches!(result, PollResult::Wait));
        assert!(matches!(requester.state, FrontierState::WaitFrontier(_)));
    }

    // Test helpers:

    const TEST_RATE_LIMIT: usize = 1000;

    fn create_test_requester() -> (FrontierRequester, Arc<RwLock<Network>>) {
        let stats = Arc::new(Stats::default());
        let network = Arc::new(RwLock::new(Network::new_test_instance()));
        let limiter = Arc::new(RateLimiter::new(1024));
        let waiter = ChannelWaiter::new(network.clone(), limiter, 1024);
        let clock = Arc::new(SteadyClock::new_null());
        let requester = FrontierRequester::new(stats.clone(), clock, TEST_RATE_LIMIT, waiter);
        (requester, network)
    }

    fn state_with_max_priorities(max: usize) -> BootstrapState {
        let config = BootstrapConfig {
            candidate_accounts: CandidateAccountsConfig {
                priorities_max: max,
                ..Default::default()
            },
            ..Default::default()
        };
        BootstrapState::new(config)
    }
}
