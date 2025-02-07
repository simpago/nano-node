use super::{
    super::state::{BootstrapState, QueryType, RunningQuery},
    account_ack_processor::AccountAckProcessor,
    block_ack_processor::BlockAckProcessor,
    frontier_ack_processor::FrontierAckProcessor,
};
use crate::{block_processing::BlockProcessor, stats::Stats};
use rsnano_ledger::Ledger;
use rsnano_messages::{AscPullAck, AscPullAckType};
use rsnano_network::ChannelId;
use rsnano_nullable_clock::Timestamp;
use std::{
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

pub(crate) struct ResponseProcessor {
    state: Arc<Mutex<BootstrapState>>,
    condition: Arc<Condvar>,
    frontiers: FrontierAckProcessor,
    accounts: AccountAckProcessor,
    blocks: BlockAckProcessor,
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
        ledger: Arc<Ledger>,
    ) -> Self {
        let frontiers = FrontierAckProcessor::new(stats.clone(), ledger, state.clone());
        let accounts = AccountAckProcessor::new(stats.clone(), state.clone());
        let blocks =
            BlockAckProcessor::new(state.clone(), stats, condition.clone(), block_processor);

        Self {
            state,
            condition,
            frontiers,
            accounts,
            blocks,
        }
    }

    pub fn set_max_pending_frontiers(&mut self, max_pending: usize) {
        self.frontiers.max_pending = max_pending;
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

        if !query.is_valid_response_type(response) {
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
            AscPullAckType::Blocks(blocks) => self.blocks.process(query, &blocks),
            AscPullAckType::AccountInfo(info) => self.accounts.process(query, &info),
            AscPullAckType::Frontiers(frontiers) => self.frontiers.process(query, frontiers),
        };

        if ok {
            Ok(())
        } else {
            Err(ProcessError::InvalidResponse)
        }
    }
}
