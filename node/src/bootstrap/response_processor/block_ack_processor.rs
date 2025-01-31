use crate::{
    block_processing::{BlockProcessor, BlockSource},
    bootstrap::state::{BootstrapState, PriorityDownResult, QueryType, RunningQuery, VerifyResult},
    stats::{DetailType, Direction, StatType, Stats},
};
use rsnano_messages::BlocksAckPayload;
use rsnano_network::ChannelId;
use std::sync::{Arc, Condvar, Mutex};

pub(crate) struct BlockAckProcessor {
    state: Arc<Mutex<BootstrapState>>,
    stats: Arc<Stats>,
    condition: Arc<Condvar>,
    block_processor: Arc<BlockProcessor>,
}

impl BlockAckProcessor {
    pub(crate) fn new(
        state: Arc<Mutex<BootstrapState>>,
        stats: Arc<Stats>,
        condition: Arc<Condvar>,
        block_processor: Arc<BlockProcessor>,
    ) -> Self {
        Self {
            state,
            stats,
            condition,
            block_processor,
        }
    }

    pub fn process(&self, query: &RunningQuery, response: &BlocksAckPayload) -> bool {
        self.stats
            .inc(StatType::BootstrapProcess, DetailType::Blocks);

        let result = verify_response(response, query);
        match result {
            VerifyResult::Ok => {
                self.stats
                    .inc(StatType::BootstrapVerifyBlocks, DetailType::Ok);
                self.stats.add_dir(
                    StatType::Bootstrap,
                    DetailType::Blocks,
                    Direction::In,
                    response.blocks().len() as u64,
                );

                let mut blocks = response.blocks().clone();

                // Avoid re-processing the block we already have
                assert!(blocks.len() >= 1);
                if blocks.front().unwrap().hash() == query.start.into() {
                    blocks.pop_front();
                }

                while let Some(block) = blocks.pop_front() {
                    if blocks.is_empty() {
                        // It's the last block submitted for this account chain, reset timestamp to allow more requests
                        let stats = self.stats.clone();
                        let data = self.state.clone();
                        let condition = self.condition.clone();
                        let account = query.account;
                        self.block_processor.add_with_callback(
                            block,
                            BlockSource::Bootstrap,
                            ChannelId::LOOPBACK,
                            Box::new(move |_| {
                                stats.inc(StatType::Bootstrap, DetailType::TimestampReset);
                                {
                                    let mut guard = data.lock().unwrap();
                                    guard.candidate_accounts.timestamp_reset(&account);
                                }
                                condition.notify_all();
                            }),
                        );
                    } else {
                        self.block_processor.add(
                            block,
                            BlockSource::Bootstrap,
                            ChannelId::LOOPBACK,
                        );
                    }
                }
                true
            }
            VerifyResult::NothingNew => {
                self.stats
                    .inc(StatType::BootstrapVerifyBlocks, DetailType::NothingNew);

                {
                    let mut guard = self.state.lock().unwrap();
                    match guard.candidate_accounts.priority_down(&query.account) {
                        PriorityDownResult::Deprioritized => {
                            self.stats
                                .inc(StatType::BootstrapAccountSets, DetailType::Deprioritize);
                        }
                        PriorityDownResult::Erased => {
                            self.stats
                                .inc(StatType::BootstrapAccountSets, DetailType::Deprioritize);
                            self.stats.inc(
                                StatType::BootstrapAccountSets,
                                DetailType::PriorityEraseThreshold,
                            );
                        }
                        PriorityDownResult::AccountNotFound => {
                            self.stats.inc(
                                StatType::BootstrapAccountSets,
                                DetailType::DeprioritizeFailed,
                            );
                        }
                        PriorityDownResult::InvalidAccount => {}
                    }

                    guard.candidate_accounts.timestamp_reset(&query.account);
                }
                self.condition.notify_all();
                true
            }
            VerifyResult::Invalid => {
                self.stats
                    .inc(StatType::BootstrapVerifyBlocks, DetailType::Invalid);
                false
            }
        }
    }
}

///
/// Verifies whether the received response is valid. Returns:
/// - invalid: when received blocks do not correspond to requested hash/account or they do not make a valid chain
/// - nothing_new: when received response indicates that the account chain does not have more blocks
/// - ok: otherwise, if all checks pass
pub(super) fn verify_response(response: &BlocksAckPayload, query: &RunningQuery) -> VerifyResult {
    let blocks = response.blocks();
    if blocks.is_empty() {
        return VerifyResult::NothingNew;
    }
    if blocks.len() == 1 && blocks.front().unwrap().hash() == query.start.into() {
        return VerifyResult::NothingNew;
    }
    if blocks.len() > query.count {
        return VerifyResult::Invalid;
    }

    let first = blocks.front().unwrap();
    match query.query_type {
        QueryType::BlocksByHash => {
            if first.hash() != query.start.into() {
                // TODO: Stat & log
                return VerifyResult::Invalid;
            }
        }
        QueryType::BlocksByAccount => {
            // Open & state blocks always contain account field
            if first.account_field().unwrap() != query.start.into() {
                // TODO: Stat & log
                return VerifyResult::Invalid;
            }
        }
        QueryType::AccountInfoByHash | QueryType::Frontiers | QueryType::Invalid => {
            return VerifyResult::Invalid;
        }
    }

    // Verify blocks make a valid chain
    let mut previous_hash = first.hash();
    for block in blocks.iter().skip(1) {
        if block.previous() != previous_hash {
            // TODO: Stat & log
            return VerifyResult::Invalid; // Blocks do not make a chain
        }
        previous_hash = block.hash();
    }

    VerifyResult::Ok
}
