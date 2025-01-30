use super::channel_waiter::ChannelWaiter;
use super::priority_query_factory::PriorityQueryFactory;
use crate::bootstrap::{state::BootstrapState, AscPullQuerySpec, BootstrapPromise, PromiseResult};
use crate::{
    block_processing::{BlockProcessor, BlockSource},
    stats::{DetailType, StatType, Stats},
};
use rsnano_network::Channel;
use std::sync::Arc;

pub(super) struct PriorityRequester {
    state: PriorityState,
    block_processor: Arc<BlockProcessor>,
    stats: Arc<Stats>,
    channel_waiter: ChannelWaiter,
    pub block_processor_threshold: usize,
    query_factory: PriorityQueryFactory,
}

impl PriorityRequester {
    pub(super) fn new(
        block_processor: Arc<BlockProcessor>,
        stats: Arc<Stats>,
        channel_waiter: ChannelWaiter,
        query_factory: PriorityQueryFactory,
    ) -> Self {
        Self {
            state: PriorityState::Initial,
            block_processor,
            stats,
            channel_waiter,
            query_factory,
            block_processor_threshold: 1000,
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
                if let Some(query) = self
                    .query_factory
                    .next_priority_query(state, channel.clone())
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
