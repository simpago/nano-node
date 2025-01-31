use super::{
    super::{
        state::{BootstrapState, CandidateAccounts, PriorityDownResult, QueryType, RunningQuery},
        BootstrapConfig,
    },
    account_processor::AccountProcessor,
    frontier_processor::FrontierProcessor,
};
use crate::{
    block_processing::{BlockProcessor, BlockSource},
    bootstrap::state::VerifyResult,
    stats::{DetailType, Direction, StatType, Stats},
    utils::ThreadPoolImpl,
};
use rsnano_ledger::Ledger;
use rsnano_messages::{AccountInfoAckPayload, AscPullAck, AscPullAckType, BlocksAckPayload};
use rsnano_network::ChannelId;
use rsnano_nullable_clock::Timestamp;
use std::{
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

pub(crate) struct ResponseProcessor {
    state: Arc<Mutex<BootstrapState>>,
    stats: Arc<Stats>,
    block_processor: Arc<BlockProcessor>,
    condition: Arc<Condvar>,
    frontier_processor: FrontierProcessor,
    account_processor: AccountProcessor,
}

pub(crate) enum ProcessError {
    NoRunningQueryFound,
    InvalidResponseType,
    InvalidResponse,
}

pub(crate) struct ProcessInfo {
    pub query_type: QueryType,
    pub response_time: Duration,
}

impl ProcessInfo {
    pub fn new(query: &RunningQuery, now: Timestamp) -> Self {
        Self {
            query_type: query.query_type,
            response_time: query.sent.elapsed(now),
        }
    }
}

impl ResponseProcessor {
    pub fn new(
        state: Arc<Mutex<BootstrapState>>,
        stats: Arc<Stats>,
        block_processor: Arc<BlockProcessor>,
        condition: Arc<Condvar>,
        workers: Arc<ThreadPoolImpl>,
        ledger: Arc<Ledger>,
        config: BootstrapConfig,
    ) -> Self {
        let frontier_processor =
            FrontierProcessor::new(stats.clone(), ledger, state.clone(), config, workers);

        let account_processor = AccountProcessor::new(stats.clone(), state.clone());

        Self {
            state,
            stats,
            block_processor,
            condition,
            frontier_processor,
            account_processor,
        }
    }

    pub fn process(
        &self,
        response: AscPullAck,
        channel_id: ChannelId,
        now: Timestamp,
    ) -> Result<ProcessInfo, ProcessError> {
        let query = self.take_running_query_for(&response)?;
        self.process_response(&query, response)?;
        self.update_peer_scoring(channel_id);
        self.condition.notify_all();
        Ok(ProcessInfo::new(&query, now))
    }

    fn take_running_query_for(&self, response: &AscPullAck) -> Result<RunningQuery, ProcessError> {
        let mut guard = self.state.lock().unwrap();

        // Only process messages that have a known running query
        let Some(query) = guard.running_queries.remove(response.id) else {
            return Err(ProcessError::NoRunningQueryFound);
        };

        if !query.is_valid_response(response) {
            return Err(ProcessError::InvalidResponseType);
        }

        Ok(query)
    }

    fn update_peer_scoring(&self, channel_id: ChannelId) {
        self.state
            .lock()
            .unwrap()
            .scoring
            .received_message(channel_id);
    }

    fn process_response(
        &self,
        query: &RunningQuery,
        response: AscPullAck,
    ) -> Result<(), ProcessError> {
        let ok = match response.pull_type {
            AscPullAckType::Blocks(blocks) => self.process_blocks(&blocks, query),
            AscPullAckType::AccountInfo(info) => self.account_processor.process(query, &info),
            AscPullAckType::Frontiers(frontiers) => {
                self.frontier_processor.process(query, frontiers)
            }
        };

        if ok {
            Ok(())
        } else {
            Err(ProcessError::InvalidResponse)
        }
    }

    pub fn process_blocks(&self, response: &BlocksAckPayload, query: &RunningQuery) -> bool {
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

    pub fn process_accounts(&self, response: &AccountInfoAckPayload, query: &RunningQuery) -> bool {
        if response.account.is_zero() {
            self.stats
                .inc(StatType::BootstrapProcess, DetailType::AccountInfoEmpty);
            // OK, but nothing to do
            return true;
        }

        self.stats
            .inc(StatType::BootstrapProcess, DetailType::AccountInfo);

        // Prioritize account containing the dependency
        {
            let mut guard = self.state.lock().unwrap();
            let updated = guard
                .candidate_accounts
                .dependency_update(&query.hash, response.account);
            if updated > 0 {
                self.stats.add(
                    StatType::BootstrapAccountSets,
                    DetailType::DependencyUpdate,
                    updated as u64,
                );
            } else {
                self.stats.inc(
                    StatType::BootstrapAccountSets,
                    DetailType::DependencyUpdateFailed,
                );
            }

            if guard
                .candidate_accounts
                .priority_set(&response.account, CandidateAccounts::PRIORITY_CUTOFF)
            {
                self.priority_inserted();
            } else {
                self.priority_insertion_failed()
            };
        }
        // OK, no way to verify the response
        true
    }

    fn priority_inserted(&self) {
        self.stats
            .inc(StatType::BootstrapAccountSets, DetailType::PriorityInsert);
    }

    fn priority_insertion_failed(&self) {
        self.stats
            .inc(StatType::BootstrapAccountSets, DetailType::PrioritizeFailed);
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
