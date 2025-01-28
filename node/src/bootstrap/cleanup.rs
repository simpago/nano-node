use super::{running_query_container::RunningQuery, BootstrapLogic};
use crate::stats::{DetailType, StatType, Stats};
use rsnano_network::Network;
use rsnano_nullable_clock::SteadyClock;
use std::{
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};

pub(super) struct BootstrapCleanup {
    clock: Arc<SteadyClock>,
    stats: Arc<Stats>,
    network: Arc<RwLock<Network>>,
    sync_dependencies_interval: Instant,
}

impl BootstrapCleanup {
    pub(super) fn new(
        clock: Arc<SteadyClock>,
        stats: Arc<Stats>,
        network: Arc<RwLock<Network>>,
    ) -> Self {
        Self {
            clock,
            stats,
            network,
            sync_dependencies_interval: Instant::now(),
        }
    }

    pub fn cleanup(&mut self, logic: &mut BootstrapLogic) {
        let now = self.clock.now();
        self.stats.inc(StatType::Bootstrap, DetailType::LoopCleanup);
        let channels = self.network.read().unwrap().list_realtime_channels(0);
        logic.scoring.sync(channels);
        logic.scoring.timeout();

        let should_timeout = |query: &RunningQuery| query.response_cutoff < now;

        while let Some(front) = logic.running_queries.front() {
            if !should_timeout(front) {
                break;
            }

            self.stats.inc(StatType::Bootstrap, DetailType::Timeout);
            self.stats
                .inc(StatType::BootstrapTimeout, front.query_type.into());
            logic.running_queries.pop_front();
        }

        if self.sync_dependencies_interval.elapsed() >= Duration::from_secs(60) {
            self.sync_dependencies_interval = Instant::now();
            self.stats
                .inc(StatType::Bootstrap, DetailType::SyncDependencies);
            let inserted = logic.candidate_accounts.sync_dependencies();
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
