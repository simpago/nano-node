use super::channel_waiter::ChannelWaiter;
use crate::bootstrap::state::PriorityResult;
use crate::bootstrap::{
    state::BootstrapState, AscPullQuerySpec, BootstrapConfig, BootstrapPromise, BootstrapResponder,
    PromiseResult,
};
use crate::{
    block_processing::{BlockProcessor, BlockSource},
    stats::{DetailType, StatType, Stats},
};
use num::clamp;
use rand::{thread_rng, Rng};
use rsnano_core::{Account, AccountInfo, BlockHash, ConfirmationHeightInfo, HashOrAccount};
use rsnano_ledger::Ledger;
use rsnano_messages::{AscPullReqType, BlocksReqPayload, HashType};
use rsnano_network::Channel;
use rsnano_nullable_clock::SteadyClock;
use std::{cmp::min, sync::Arc};

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

    fn next_priority_query(
        &self,
        state: &mut BootstrapState,
        channel: Arc<Channel>,
    ) -> Option<AscPullQuerySpec> {
        let now = self.clock.now();
        let next = state.next_priority(now);

        if next.account.is_zero() {
            return None;
        }
        let (account_info, conf_info) = self.get_account_infos(&next.account);
        let pull_type = self.decide_pull_type();

        Some(self.create_priority_query(&next, channel, pull_type, account_info, conf_info))
    }

    fn get_account_infos(
        &self,
        account: &Account,
    ) -> (Option<AccountInfo>, Option<ConfirmationHeightInfo>) {
        let tx = self.ledger.read_txn();
        let account_info = self.ledger.store.account.get(&tx, account);
        let conf_info = self.ledger.store.confirmation_height.get(&tx, account);
        (account_info, conf_info)
    }

    fn create_priority_query(
        &self,
        next: &PriorityResult,
        channel: Arc<Channel>,
        pull_type: PriorityPullType,
        account_info: Option<AccountInfo>,
        conf_info: Option<ConfirmationHeightInfo>,
    ) -> AscPullQuerySpec {
        let pull_start = {
            PullStart::new(
                pull_type,
                next.account,
                account_info.as_ref(),
                conf_info.as_ref(),
            )
        };
        let req_type = AscPullReqType::Blocks(BlocksReqPayload {
            start_type: pull_start.start_type,
            start: pull_start.start,
            count: self.pull_count(&next),
        });

        // Only cooldown accounts that are likely to have more blocks
        // This is to avoid requesting blocks from the same frontier multiple times, before the block processor had a chance to process them
        // Not throttling accounts that are probably up-to-date allows us to evict them from the priority set faster
        let cooldown_account = next.fails == 0;

        AscPullQuerySpec {
            channel,
            req_type,
            hash: pull_start.hash,
            account: next.account,
            cooldown_account,
        }
    }

    fn pull_count(&self, next: &PriorityResult) -> u8 {
        // Decide how many blocks to request
        const MIN_PULL_COUNT: usize = 2;
        let pull_count = clamp(
            f64::from(next.priority) as usize,
            MIN_PULL_COUNT,
            BootstrapResponder::MAX_BLOCKS,
        );

        // Limit the max number of blocks to pull
        min(pull_count, self.config.max_pull_count) as u8
    }

    /// Probabilistically choose between requesting blocks from account frontier
    /// or confirmed frontier.
    /// Optimistic requests start from the (possibly unconfirmed) account frontier
    /// and are vulnerable to bootstrap poisoning.
    /// Safe requests start from the confirmed frontier and given enough time
    /// will eventually resolve forks
    fn decide_pull_type(&self) -> PriorityPullType {
        if thread_rng().gen_range(0..100) < self.config.optimistic_request_percentage {
            PriorityPullType::Optimistic
        } else {
            PriorityPullType::Safe
        }
    }

    fn block_processor_free(&self) -> bool {
        self.block_processor.queue_len(BlockSource::Bootstrap)
            < self.config.block_processor_theshold
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
                if let Some(query) = self.next_priority_query(state, channel.clone()) {
                    self.state = PriorityState::Initial;
                    PromiseResult::Finished(query)
                } else {
                    PromiseResult::Wait
                }
            }
        }
    }
}

struct PullStart {
    pull_type: PriorityPullType,
    start: HashOrAccount,
    start_type: HashType,
    hash: BlockHash,
}

impl PullStart {
    fn new(
        pull_type: PriorityPullType,
        account: Account,
        account_info: Option<&AccountInfo>,
        conf_info: Option<&ConfirmationHeightInfo>,
    ) -> Self {
        // Check if the account picked has blocks, if it does, start the pull from the highest block
        match account_info {
            Some(info) => match pull_type {
                PriorityPullType::Optimistic => PullStart::optimistic(&info),
                PriorityPullType::Safe => PullStart::safe(account, conf_info),
            },
            None => PullStart::safe_account(account),
        }
    }

    fn safe(account: Account, conf_info: Option<&ConfirmationHeightInfo>) -> Self {
        if let Some(conf_info) = conf_info {
            PullStart::safe_block(conf_info)
        } else {
            PullStart::safe_account(account)
        }
    }

    fn safe_account(account: Account) -> Self {
        Self {
            pull_type: PriorityPullType::Safe,
            start: account.into(),
            start_type: HashType::Account,
            hash: BlockHash::zero(),
        }
    }

    fn safe_block(conf_info: &ConfirmationHeightInfo) -> Self {
        Self {
            pull_type: PriorityPullType::Safe,
            start: conf_info.frontier.into(),
            start_type: HashType::Block,
            hash: conf_info.height.into(),
        }
    }

    fn optimistic(acc_info: &AccountInfo) -> Self {
        Self {
            pull_type: PriorityPullType::Optimistic,
            start: acc_info.head.into(),
            start_type: HashType::Block,
            hash: acc_info.head,
        }
    }
}

#[derive(Clone, Copy)]
enum PriorityPullType {
    /// Optimistic requests start from the (possibly unconfirmed) account frontier
    /// and are vulnerable to bootstrap poisoning.
    Optimistic,
    /// Safe requests start from the confirmed frontier and given enough time
    /// will eventually resolve forks
    Safe,
}

impl From<PriorityPullType> for DetailType {
    fn from(value: PriorityPullType) -> Self {
        match value {
            PriorityPullType::Optimistic => DetailType::Optimistic,
            PriorityPullType::Safe => DetailType::Safe,
        }
    }
}
