use super::pull_count_decider::PullCountDecider;
use super::pull_type_decider::PullTypeDecider;
use super::query_factory::QueryFactory;
use crate::bootstrap::requesters::channel_waiter::ChannelWaiter;
use crate::bootstrap::BootstrapConfig;
use crate::bootstrap::{state::BootstrapState, AscPullQuerySpec, BootstrapPromise, PollResult};
use crate::{
    block_processing::{BlockProcessor, BlockSource},
    stats::{DetailType, StatType, Stats},
};
use rsnano_ledger::Ledger;
use rsnano_network::Channel;
use rsnano_nullable_clock::SteadyClock;
use std::sync::Arc;

pub(crate) struct PriorityRequester {
    state: PriorityState,
    block_processor: Arc<BlockProcessor>,
    stats: Arc<Stats>,
    channel_waiter: ChannelWaiter,
    pub block_processor_threshold: usize,
    query_factory: QueryFactory,
    clock: Arc<SteadyClock>,
}

impl PriorityRequester {
    pub(crate) fn new(
        block_processor: Arc<BlockProcessor>,
        stats: Arc<Stats>,
        channel_waiter: ChannelWaiter,
        clock: Arc<SteadyClock>,
        ledger: Arc<Ledger>,
        config: &BootstrapConfig,
    ) -> Self {
        let pull_type_decider = PullTypeDecider::new(config.optimistic_request_percentage);
        let pull_count_decider = PullCountDecider::new(config.max_pull_count);
        let query_factory = QueryFactory::new(ledger, pull_type_decider, pull_count_decider);

        Self {
            state: PriorityState::Initial,
            block_processor,
            stats,
            channel_waiter,
            query_factory,
            block_processor_threshold: 1000,
            clock,
        }
    }

    fn block_processor_free(&self) -> bool {
        self.block_processor.queue_len(BlockSource::Bootstrap) < self.block_processor_threshold
    }
}

enum PriorityState {
    Initial,
    WaitBlockProcessor,
    WaitChannel,
    WaitPriority(Arc<Channel>),
}

impl BootstrapPromise<AscPullQuerySpec> for PriorityRequester {
    fn poll(&mut self, state: &mut BootstrapState) -> PollResult<AscPullQuerySpec> {
        match self.state {
            PriorityState::Initial => {
                self.stats.inc(StatType::Bootstrap, DetailType::Loop);
                self.state = PriorityState::WaitBlockProcessor;
                PollResult::Progress
            }
            PriorityState::WaitBlockProcessor => {
                if self.block_processor_free() {
                    self.state = PriorityState::WaitChannel;
                    PollResult::Progress
                } else {
                    PollResult::Wait
                }
            }
            PriorityState::WaitChannel => match self.channel_waiter.poll(state) {
                PollResult::Progress => PollResult::Progress,
                PollResult::Wait => PollResult::Wait,
                PollResult::Finished(channel) => {
                    self.state = PriorityState::WaitPriority(channel);
                    PollResult::Progress
                }
            },
            PriorityState::WaitPriority(ref channel) => {
                if let Some(query) =
                    self.query_factory
                        .next_priority_query(state, channel.clone(), self.clock.now())
                {
                    self.state = PriorityState::Initial;
                    PollResult::Finished(query)
                } else {
                    PollResult::Wait
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PriorityRequester;
    use crate::{
        block_processing::BlockProcessor,
        bootstrap::{
            progress,
            requesters::{
                channel_waiter::ChannelWaiter, priority::priority_requester::PriorityState,
            },
            state::BootstrapState,
            BootstrapConfig, PollResult,
        },
        stats::Stats,
    };
    use rsnano_core::Account;
    use rsnano_ledger::Ledger;
    use rsnano_network::{bandwidth_limiter::RateLimiter, Network};
    use rsnano_nullable_clock::SteadyClock;
    use std::sync::{Arc, RwLock};

    #[test]
    fn happy_path() {
        let mut state = BootstrapState::default();
        let account = Account::from(42);
        state.candidate_accounts.priority_up(&account);

        let (mut requester, network) = create_requester();
        network.write().unwrap().add_test_channel();
        let PollResult::Finished(result) = progress(&mut requester, &mut state) else {
            panic!("Finished expected");
        };

        assert_eq!(result.account, account);
    }

    #[test]
    fn wait_block_processor() {
        let mut state = BootstrapState::default();

        let (mut requester, _) = create_requester();
        requester.block_processor_threshold = 0;

        let result = progress(&mut requester, &mut state);

        assert!(matches!(result, PollResult::Wait));
        assert!(matches!(requester.state, PriorityState::WaitBlockProcessor));
    }

    #[test]
    fn wait_channel() {
        let mut state = BootstrapState::default();
        let (mut requester, _) = create_requester();

        let result = progress(&mut requester, &mut state);

        assert!(matches!(result, PollResult::Wait));
        assert!(matches!(requester.state, PriorityState::WaitChannel));
    }

    #[test]
    fn wait_priority() {
        let mut state = BootstrapState::default();
        let (mut requester, network) = create_requester();
        network.write().unwrap().add_test_channel();

        let result = progress(&mut requester, &mut state);

        assert!(matches!(result, PollResult::Wait));
        assert!(matches!(requester.state, PriorityState::WaitPriority(_)));
    }

    fn create_requester() -> (PriorityRequester, Arc<RwLock<Network>>) {
        let block_processor = Arc::new(BlockProcessor::new_null());
        let stats = Arc::new(Stats::default());
        let network = Arc::new(RwLock::new(Network::new_test_instance()));
        let rate_limiter = Arc::new(RateLimiter::new(1024));
        let channel_waiter = ChannelWaiter::new(network.clone(), rate_limiter, 1024);
        let clock = Arc::new(SteadyClock::new_null());
        let ledger = Arc::new(Ledger::new_null());
        let config = BootstrapConfig::default();

        let requester = PriorityRequester::new(
            block_processor,
            stats,
            channel_waiter,
            clock,
            ledger,
            &config,
        );

        (requester, network)
    }
}
