use super::frontier_worker::FrontierWorker;
use crate::{
    bootstrap::state::{BootstrapState, RunningQuery, VerifyResult},
    stats::{DetailType, Direction, StatType, Stats},
    utils::{ThreadPool, ThreadPoolImpl},
};
use rsnano_core::Frontier;
use rsnano_ledger::Ledger;
use std::sync::{Arc, Mutex};

/// Processes responses to AscPullReqs by the frontier scan
pub(crate) struct FrontierAckProcessor {
    stats: Arc<Stats>,
    ledger: Arc<Ledger>,
    state: Arc<Mutex<BootstrapState>>,
    workers: Arc<ThreadPoolImpl>,
    pub max_pending: usize,
}

impl FrontierAckProcessor {
    pub(crate) fn new(
        stats: Arc<Stats>,
        ledger: Arc<Ledger>,
        state: Arc<Mutex<BootstrapState>>,
    ) -> Self {
        let workers = Arc::new(ThreadPoolImpl::create(1, "Bootstrap work"));
        Self {
            stats,
            ledger,
            state,
            workers,
            max_pending: 16,
        }
    }

    pub fn process(&self, query: &RunningQuery, frontiers: Vec<Frontier>) -> bool {
        if frontiers.is_empty() {
            self.stats
                .inc(StatType::BootstrapProcess, DetailType::FrontiersEmpty);
            // OK, but nothing to do
            return true;
        }

        self.stats
            .inc(StatType::BootstrapProcess, DetailType::Frontiers);

        match query.verify_frontiers(&frontiers) {
            VerifyResult::Ok => {
                self.stats
                    .inc(StatType::BootstrapVerifyFrontiers, DetailType::Ok);
                self.process_valid_frontiers(query, frontiers);
                true
            }
            VerifyResult::NothingNew => {
                self.stats
                    .inc(StatType::BootstrapVerifyFrontiers, DetailType::NothingNew);
                true
            }
            VerifyResult::Invalid => {
                self.stats
                    .inc(StatType::BootstrapVerifyFrontiers, DetailType::Invalid);
                false
            }
        }
    }

    fn process_valid_frontiers(&self, query: &RunningQuery, frontiers: Vec<Frontier>) {
        self.stats.add_dir(
            StatType::Bootstrap,
            DetailType::Frontiers,
            Direction::In,
            frontiers.len() as u64,
        );

        self.stats
            .inc(StatType::BootstrapFrontierScan, DetailType::Process);

        self.update_state(query, &frontiers);

        let ledger = self.ledger.clone();
        let stats = self.stats.clone();
        let state = self.state.clone();
        self.workers.post(Box::new(move || {
            let tx = ledger.read_txn();
            let mut worker = FrontierWorker::new(&ledger, &tx, &stats, &state);
            worker.process(frontiers);
        }));
    }

    fn update_state(&self, query: &RunningQuery, frontiers: &[Frontier]) {
        let mut guard = self.state.lock().unwrap();
        guard.frontier_scan.process(query.start.into(), &frontiers);
        guard.frontier_ack_processor_busy = self.workers.num_queued_tasks() >= self.max_pending;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bootstrap::state::{QuerySource, QueryType};

    #[test]
    fn update_account_ranges() {
        let stats = Arc::new(Stats::default());
        let ledger = Arc::new(Ledger::new_null());
        let state = Arc::new(Mutex::new(BootstrapState::default()));
        let processor = FrontierAckProcessor::new(stats, ledger, state.clone());

        let query = RunningQuery {
            source: QuerySource::Frontiers,
            query_type: QueryType::Frontiers,
            start: 1.into(),
            ..RunningQuery::new_test_instance()
        };

        let success = processor.process(&query, Vec::new());
        assert!(success);

        let success = processor.process(&query, vec![Frontier::new_test_instance()]);
        assert!(success);
        assert_eq!(
            state
                .lock()
                .unwrap()
                .frontier_scan
                .total_requests_completed(),
            1
        );
    }
}
