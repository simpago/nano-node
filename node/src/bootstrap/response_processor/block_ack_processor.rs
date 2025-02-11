use crate::{
    block_processing::{BlockProcessor, BlockSource},
    bootstrap::state::{BootstrapState, PriorityDownResult, RunningQuery, VerifyResult},
    stats::{DetailType, Direction, StatType, Stats},
};
use rsnano_messages::BlocksAckPayload;
use rsnano_network::ChannelId;
use std::sync::{Arc, Mutex};

pub(crate) struct BlockAckProcessor {
    state: Arc<Mutex<BootstrapState>>,
    stats: Arc<Stats>,
    block_processor: Arc<BlockProcessor>,
}

impl BlockAckProcessor {
    pub(crate) fn new(
        state: Arc<Mutex<BootstrapState>>,
        stats: Arc<Stats>,
        block_processor: Arc<BlockProcessor>,
    ) -> Self {
        Self {
            state,
            stats,
            block_processor,
        }
    }

    pub fn process(&self, query: &RunningQuery, response: &BlocksAckPayload) -> bool {
        self.stats
            .inc(StatType::BootstrapProcess, DetailType::Blocks);

        let result = query.verify_blocks(response);
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
                        let state = self.state.clone();
                        let account = query.account;
                        self.block_processor.add_with_callback(
                            block,
                            BlockSource::Bootstrap,
                            ChannelId::LOOPBACK,
                            Box::new(move |_| {
                                stats.inc(StatType::Bootstrap, DetailType::TimestampReset);
                                {
                                    let mut guard = state.lock().unwrap();
                                    guard.candidate_accounts.reset_last_request(&account);
                                }
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

                    guard.candidate_accounts.reset_last_request(&query.account);
                }
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
