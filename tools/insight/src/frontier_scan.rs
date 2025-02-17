use std::collections::VecDeque;

use rsnano_core::Account;
use rsnano_node::bootstrap::{BootstrapCounters, Bootstrapper, FrontierHeadInfo};
use rsnano_nullable_clock::Timestamp;

use crate::rate_calculator::RateCalculator;

#[derive(Default)]
pub(crate) struct FrontierScanInfo {
    frontiers_rate: RateCalculator,
    outdated_rate: RateCalculator,
    pub frontier_heads: Vec<FrontierHeadInfo>,
    pub frontiers_total: usize,
    pub outdated_total: usize,
    pub outdated_accounts: VecDeque<Account>,
}

impl FrontierScanInfo {
    pub(crate) fn update(&mut self, bootstrapper: &Bootstrapper, now: Timestamp) {
        let counters = bootstrapper.counters();
        self.update_counters(&counters, now);
        self.frontier_heads = bootstrapper.frontier_heads();
        self.outdated_accounts = bootstrapper.last_outdated_accounts();
    }

    fn update_counters(&mut self, counters: &BootstrapCounters, now: Timestamp) {
        self.frontiers_rate
            .sample(counters.received_frontiers as u64, now);
        self.outdated_rate
            .sample(counters.outdated_accounts_found as u64, now);
        self.frontiers_total = counters.received_frontiers;
        self.outdated_total = counters.outdated_accounts_found;
    }

    pub(crate) fn frontiers_rate(&self) -> i64 {
        self.frontiers_rate.rate()
    }

    pub(crate) fn outdated_rate(&self) -> i64 {
        self.outdated_rate.rate()
    }
}
