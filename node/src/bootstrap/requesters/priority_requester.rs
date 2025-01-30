use crate::bootstrap::{
    state::BootstrapState, AscPullQuerySpec, BootstrapAction, BootstrapConfig, BootstrapResponder,
    WaitResult,
};
use crate::{
    block_processing::{BlockProcessor, BlockSource},
    stats::{DetailType, StatType, Stats},
};
use num::clamp;
use rand::{thread_rng, Rng};
use rsnano_core::{BlockHash, HashOrAccount};
use rsnano_ledger::Ledger;
use rsnano_messages::{AscPullReqType, BlocksReqPayload, HashType};
use rsnano_network::Channel;
use rsnano_nullable_clock::SteadyClock;
use std::{cmp::min, sync::Arc};

use super::channel_waiter::ChannelWaiter;

pub(super) struct PriorityRequester {
    state: PriorityState,
    ledger: Arc<Ledger>,
    block_processor: Arc<BlockProcessor>,
    stats: Arc<Stats>,
    clock: Arc<SteadyClock>,
    channel_waiter: ChannelWaiter,
    config: BootstrapConfig,
}

impl PriorityRequester {
    pub(super) fn new(
        ledger: Arc<Ledger>,
        block_processor: Arc<BlockProcessor>,
        stats: Arc<Stats>,
        clock: Arc<SteadyClock>,
        channel_waiter: ChannelWaiter,
        config: BootstrapConfig,
    ) -> Self {
        Self {
            state: PriorityState::Initial,
            ledger,
            block_processor,
            stats,
            clock,
            channel_waiter,
            config,
        }
    }
}

enum PriorityState {
    Initial,
    WaitBlockProcessor,
    WaitChannel,
    WaitPriority(Arc<Channel>),
}

impl BootstrapAction<AscPullQuerySpec> for PriorityRequester {
    fn run(&mut self, state: &mut BootstrapState) -> WaitResult<AscPullQuerySpec> {
        let now = self.clock.now();
        match self.state {
            PriorityState::Initial => {
                self.stats.inc(StatType::Bootstrap, DetailType::Loop);
                self.state = PriorityState::WaitBlockProcessor;
                return WaitResult::Progress;
            }
            PriorityState::WaitBlockProcessor => {
                if self.block_processor.queue_len(BlockSource::Bootstrap)
                    < self.config.block_processor_theshold
                {
                    self.state = PriorityState::WaitChannel;
                    return WaitResult::Progress;
                }
            }
            PriorityState::WaitChannel => match self.channel_waiter.run(state) {
                WaitResult::Progress => return WaitResult::Progress,
                WaitResult::Wait => return WaitResult::Wait,
                WaitResult::Finished(channel) => {
                    self.state = PriorityState::WaitPriority(channel);
                    return WaitResult::Progress;
                }
            },
            PriorityState::WaitPriority(ref channel) => {
                let next = state.next_priority(now);
                if !next.account.is_zero() {
                    self.stats
                        .inc(StatType::BootstrapNext, DetailType::NextPriority);

                    // Decide how many blocks to request
                    const MIN_PULL_COUNT: usize = 2;
                    let pull_count = clamp(
                        f64::from(next.priority) as usize,
                        MIN_PULL_COUNT,
                        BootstrapResponder::MAX_BLOCKS,
                    );
                    // Limit the max number of blocks to pull
                    let pull_count = min(pull_count, self.config.max_pull_count);

                    let account_info = {
                        let tx = self.ledger.read_txn();
                        self.ledger.store.account.get(&tx, &next.account)
                    };
                    let account = next.account;
                    let tx = self.ledger.read_txn();
                    // Check if the account picked has blocks, if it does, start the pull from the highest block
                    let (start_type, start, hash) = match account_info {
                        Some(info) => {
                            // Probabilistically choose between requesting blocks from account frontier or confirmed frontier
                            // Optimistic requests start from the (possibly unconfirmed) account frontier and are vulnerable to bootstrap poisoning
                            // Safe requests start from the confirmed frontier and given enough time will eventually resolve forks
                            let optimistic_request = thread_rng().gen_range(0..100)
                                < self.config.optimistic_request_percentage;

                            if optimistic_request {
                                self.stats
                                    .inc(StatType::BootstrapRequestBlocks, DetailType::Optimistic);
                                (HashType::Block, HashOrAccount::from(info.head), info.head)
                            } else {
                                // Pessimistic (safe) request case
                                self.stats
                                    .inc(StatType::BootstrapRequestBlocks, DetailType::Safe);

                                let conf_info =
                                    self.ledger.store.confirmation_height.get(&tx, &account);
                                if let Some(conf_info) = conf_info {
                                    (
                                        HashType::Block,
                                        HashOrAccount::from(conf_info.frontier),
                                        BlockHash::from(conf_info.height),
                                    )
                                } else {
                                    (HashType::Account, account.into(), BlockHash::zero())
                                }
                            }
                        }
                        None => {
                            self.stats
                                .inc(StatType::BootstrapRequestBlocks, DetailType::Base);
                            (
                                HashType::Account,
                                HashOrAccount::from(account),
                                BlockHash::zero(),
                            )
                        }
                    };
                    let req_type = AscPullReqType::Blocks(BlocksReqPayload {
                        start_type,
                        start,
                        count: pull_count as u8,
                    });

                    // Only cooldown accounts that are likely to have more blocks
                    // This is to avoid requesting blocks from the same frontier multiple times, before the block processor had a chance to process them
                    // Not throttling accounts that are probably up-to-date allows us to evict them from the priority set faster
                    let cooldown_account = next.fails == 0;

                    let result = AscPullQuerySpec {
                        channel: channel.clone(),
                        req_type,
                        hash,
                        account,
                        cooldown_account,
                    };

                    self.state = PriorityState::Initial;
                    return WaitResult::Finished(result);
                }
            }
        };

        WaitResult::Wait
    }
}
