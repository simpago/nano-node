use crate::bootstrap::{
    state::{BootstrapState, PriorityResult},
    AscPullQuerySpec, BootstrapResponder,
};
use num::clamp;
use rand::{thread_rng, Rng};
use rsnano_core::{Account, BlockHash, HashOrAccount};
use rsnano_ledger::Ledger;
use rsnano_messages::{AscPullReqType, BlocksAckPayload, BlocksReqPayload, HashType};
use rsnano_network::Channel;
use rsnano_nullable_clock::SteadyClock;
use std::{cmp::min, sync::Arc};

pub(super) struct PriorityQueryFactory {
    clock: Arc<SteadyClock>,
    ledger: Arc<Ledger>,
    pub max_pull_count: usize,
    pub optimistic_request_percentage: u8,
}

impl PriorityQueryFactory {
    pub(super) fn new(clock: Arc<SteadyClock>, ledger: Arc<Ledger>) -> Self {
        Self {
            clock,
            ledger,
            max_pull_count: BlocksAckPayload::MAX_BLOCKS,
            optimistic_request_percentage: 75,
        }
    }

    pub fn next_priority_query(
        &self,
        state: &mut BootstrapState,
        channel: Arc<Channel>,
    ) -> Option<AscPullQuerySpec> {
        let now = self.clock.now();
        let next = state.next_priority(now);

        if next.account.is_zero() {
            return None;
        }
        let (head, confirmed_frontier, conf_height) = self.get_account_infos(&next.account);
        let pull_type = self.decide_pull_type();

        Some(self.create_priority_query(
            &next,
            channel,
            pull_type,
            head,
            confirmed_frontier,
            conf_height,
        ))
    }

    fn get_account_infos(&self, account: &Account) -> (BlockHash, BlockHash, u64) {
        let tx = self.ledger.read_txn();
        let account_info = self.ledger.store.account.get(&tx, account);
        let head = account_info.map(|i| i.head).unwrap_or_default();

        if let Some(conf_info) = self.ledger.store.confirmation_height.get(&tx, account) {
            (head, conf_info.frontier, conf_info.height)
        } else {
            (head, BlockHash::zero(), 0)
        }
    }

    fn create_priority_query(
        &self,
        next: &PriorityResult,
        channel: Arc<Channel>,
        pull_type: PriorityPullType,
        head: BlockHash,
        confirmed_frontier: BlockHash,
        conf_height: u64,
    ) -> AscPullQuerySpec {
        let pull_start = {
            PullStart::new(
                pull_type,
                next.account,
                head,
                confirmed_frontier,
                conf_height,
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
        min(pull_count, self.max_pull_count) as u8
    }

    /// Probabilistically choose between requesting blocks from account frontier
    /// or confirmed frontier.
    /// Optimistic requests start from the (possibly unconfirmed) account frontier
    /// and are vulnerable to bootstrap poisoning.
    /// Safe requests start from the confirmed frontier and given enough time
    /// will eventually resolve forks
    fn decide_pull_type(&self) -> PriorityPullType {
        if thread_rng().gen_range(0..100) < self.optimistic_request_percentage {
            PriorityPullType::Optimistic
        } else {
            PriorityPullType::Safe
        }
    }
}

struct PullStart {
    start: HashOrAccount,
    start_type: HashType,
    hash: BlockHash,
}

impl PullStart {
    fn new(
        pull_type: PriorityPullType,
        account: Account,
        head: BlockHash,
        confirmed_frontier: BlockHash,
        conf_height: u64,
    ) -> Self {
        // Check if the account picked has blocks, if it does, start the pull from the highest block
        if head.is_zero() {
            PullStart::account(account)
        } else {
            match pull_type {
                PriorityPullType::Optimistic => PullStart::block(head, head),
                PriorityPullType::Safe => PullStart::safe(account, confirmed_frontier, conf_height),
            }
        }
    }

    fn safe(account: Account, confirmed_frontier: BlockHash, conf_height: u64) -> Self {
        if confirmed_frontier.is_zero() {
            PullStart::account(account)
        } else {
            PullStart::block(confirmed_frontier, conf_height.into())
        }
    }

    fn account(account: Account) -> Self {
        Self {
            start: account.into(),
            start_type: HashType::Account,
            hash: BlockHash::zero(),
        }
    }

    fn block(start: BlockHash, hash: BlockHash) -> Self {
        Self {
            start: start.into(),
            start_type: HashType::Block,
            hash,
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
