use super::frontier_worker::FrontierWorker;
use crate::{
    bootstrap::{
        state::{BootstrapState, RunningQuery, VerifyResult},
        BootstrapConfig,
    },
    stats::{DetailType, Direction, StatType, Stats},
    utils::{ThreadPool, ThreadPoolImpl},
};
use rsnano_core::Frontier;
use rsnano_ledger::Ledger;
use std::sync::{Arc, Mutex};

/// Processes responses to AscPullReqs by the frontier scan
pub(crate) struct FrontierProcessor {
    stats: Arc<Stats>,
    ledger: Arc<Ledger>,
    state: Arc<Mutex<BootstrapState>>,
    config: BootstrapConfig,
    workers: Arc<ThreadPoolImpl>,
}

impl FrontierProcessor {
    pub(crate) fn new(
        stats: Arc<Stats>,
        ledger: Arc<Ledger>,
        state: Arc<Mutex<BootstrapState>>,
        config: BootstrapConfig,
        workers: Arc<ThreadPoolImpl>,
    ) -> Self {
        Self {
            stats,
            ledger,
            state,
            config,
            workers,
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
                self.stats.add_dir(
                    StatType::Bootstrap,
                    DetailType::Frontiers,
                    Direction::In,
                    frontiers.len() as u64,
                );

                self.update_account_ranges(query, &frontiers);

                // Allow some overfill to avoid unnecessarily dropping responses
                if self.workers.num_queued_tasks() < self.config.frontier_scan.max_pending * 4 {
                    let worker = FrontierWorker::new(
                        self.ledger.clone(),
                        self.stats.clone(),
                        self.state.clone(),
                    );
                    self.workers.post(Box::new(move || {
                        worker.process(frontiers);
                    }));
                } else {
                    self.stats.add(
                        StatType::Bootstrap,
                        DetailType::FrontiersDropped,
                        frontiers.len() as u64,
                    );
                }
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

    fn update_account_ranges(&self, query: &RunningQuery, frontiers: &[Frontier]) {
        let mut guard = self.state.lock().unwrap();
        self.stats
            .inc(StatType::BootstrapFrontierScan, DetailType::Process);
        let done = guard.account_ranges.process(query.start.into(), &frontiers);
        if done {
            self.stats
                .inc(StatType::BootstrapFrontierScan, DetailType::Done);
        }
    }
}
