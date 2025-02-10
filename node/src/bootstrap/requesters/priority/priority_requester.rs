use super::priority_pull_count_decider::PriorityPullCountDecider;
use super::priority_pull_type_decider::PriorityPullTypeDecider;
use super::priority_query_factory::PriorityQueryFactory;
use crate::bootstrap::requesters::channel_waiter::ChannelWaiter;
use crate::bootstrap::BootstrapConfig;
use crate::bootstrap::{state::BootstrapState, AscPullQuerySpec, BootstrapPromise, PromiseResult};
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
    query_factory: PriorityQueryFactory,
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
        let pull_type_decider = PriorityPullTypeDecider::new(config.optimistic_request_percentage);
        let pull_count_decider = PriorityPullCountDecider::new(config.max_pull_count);
        let query_factory =
            PriorityQueryFactory::new(ledger, pull_type_decider, pull_count_decider);

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
    fn poll(&mut self, state: &mut BootstrapState) -> PromiseResult<AscPullQuerySpec> {
        match self.state {
            PriorityState::Initial => {
                self.stats.inc(StatType::Bootstrap, DetailType::Loop);
                self.state = PriorityState::WaitBlockProcessor;
                PromiseResult::Progress
            }
            PriorityState::WaitBlockProcessor => {
                if self.block_processor_free() {
                    self.state = PriorityState::WaitChannel;
                    PromiseResult::Progress
                } else {
                    PromiseResult::Wait
                }
            }
            PriorityState::WaitChannel => match self.channel_waiter.poll(state) {
                PromiseResult::Progress => PromiseResult::Progress,
                PromiseResult::Wait => PromiseResult::Wait,
                PromiseResult::Finished(channel) => {
                    self.state = PriorityState::WaitPriority(channel);
                    PromiseResult::Progress
                }
            },
            PriorityState::WaitPriority(ref channel) => {
                if let Some(query) =
                    self.query_factory
                        .next_priority_query(state, channel.clone(), self.clock.now())
                {
                    self.state = PriorityState::Initial;
                    PromiseResult::Finished(query)
                } else {
                    PromiseResult::Wait
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
            BootstrapConfig, PromiseResult,
        },
        stats::Stats,
    };
    use rsnano_core::Account;
    use rsnano_ledger::Ledger;
    use rsnano_nullable_clock::SteadyClock;
    use std::sync::Arc;

    #[test]
    fn happy_path() {
        let mut state = BootstrapState::new_test_instance();
        state.add_test_channel();
        let account = Account::from(42);
        state.candidate_accounts.priority_up(&account);

        let mut requester = create_requester();
        let PromiseResult::Finished(result) = progress(&mut requester, &mut state) else {
            panic!("Finished expected");
        };

        assert_eq!(result.account, account);
    }

    #[test]
    fn wait_block_processor() {
        let mut state = BootstrapState::new_test_instance();

        let mut requester = create_requester();
        requester.block_processor_threshold = 0;

        let result = progress(&mut requester, &mut state);

        assert!(matches!(result, PromiseResult::Wait));
        assert!(matches!(requester.state, PriorityState::WaitBlockProcessor));
    }

    #[test]
    fn wait_channel() {
        let mut state = BootstrapState::new_test_instance();
        let mut requester = create_requester();

        let result = progress(&mut requester, &mut state);

        assert!(matches!(result, PromiseResult::Wait));
        assert!(matches!(requester.state, PriorityState::WaitChannel));
    }

    #[test]
    fn wait_priority() {
        let mut state = BootstrapState::new_test_instance();
        let mut requester = create_requester();
        state.add_test_channel();

        let result = progress(&mut requester, &mut state);

        assert!(matches!(result, PromiseResult::Wait));
        assert!(matches!(requester.state, PriorityState::WaitPriority(_)));
    }

    fn create_requester() -> PriorityRequester {
        let block_processor = Arc::new(BlockProcessor::new_null());
        let stats = Arc::new(Stats::default());
        let channel_waiter = ChannelWaiter::default();
        let clock = Arc::new(SteadyClock::new_null());
        let ledger = Arc::new(Ledger::new_null());
        let config = BootstrapConfig::default();

        PriorityRequester::new(
            block_processor,
            stats,
            channel_waiter,
            clock,
            ledger,
            &config,
        )
    }
}
