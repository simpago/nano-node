use super::state::{BootstrapState, RunningQuery};
use crate::stats::{DetailType, StatType, Stats};
use rsnano_nullable_clock::SteadyClock;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

pub(super) struct BootstrapCleanup {
    clock: Arc<SteadyClock>,
    stats: Arc<Stats>,
    sync_dependencies_interval: Instant,
}

impl BootstrapCleanup {
    pub(super) fn new(clock: Arc<SteadyClock>, stats: Arc<Stats>) -> Self {
        Self {
            clock,
            stats,
            sync_dependencies_interval: Instant::now(),
        }
    }

    pub fn cleanup(&mut self, state: &mut BootstrapState) {
        let now = self.clock.now();
        self.stats.inc(StatType::Bootstrap, DetailType::LoopCleanup);
        state.scoring.timeout();

        let should_timeout = |query: &RunningQuery| query.response_cutoff < now;

        while let Some(front) = state.running_queries.front() {
            if !should_timeout(front) {
                break;
            }

            self.stats.inc(StatType::Bootstrap, DetailType::Timeout);
            self.stats
                .inc(StatType::BootstrapTimeout, front.query_type.into());
            state.running_queries.pop_front();
        }

        if self.sync_dependencies_interval.elapsed() >= Duration::from_secs(60) {
            self.sync_dependencies_interval = Instant::now();
            self.stats
                .inc(StatType::Bootstrap, DetailType::SyncDependencies);
            let inserted = state.candidate_accounts.sync_dependencies();
            if inserted > 0 {
                self.stats.add(
                    StatType::BootstrapAccountSets,
                    DetailType::PriorityInsert,
                    inserted as u64,
                );
                self.stats.add(
                    StatType::BootstrapAccountSets,
                    DetailType::DependencySynced,
                    inserted as u64,
                );
            }
        }
    }
}
