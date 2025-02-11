use crate::rate_calculator::RateCalculator;
use rsnano_core::Account;
use rsnano_node::bootstrap::{BootstrapCounters, Bootstrapper, FrontierHeadInfo};
use rsnano_nullable_clock::Timestamp;
use std::collections::VecDeque;

pub(crate) struct BootstrapViewModel {
    frontiers_rate: RateCalculator,
    outdated_rate: RateCalculator,
    pub frontier_heads: Vec<FrontierHeadViewModel>,
    pub frontiers_total: usize,
    pub outdated_total: usize,
    pub outdated_accounts: Vec<String>,
}

impl BootstrapViewModel {
    pub(crate) fn update(&mut self, bootstrapper: &Bootstrapper, now: Timestamp) {
        let counters = bootstrapper.counters();
        self.update_counters(&counters, now);
        self.update_frontier_heads(&bootstrapper.frontier_heads());
        self.update_outdated_accounts(&bootstrapper.last_outdated_accounts());
    }

    fn update_counters(&mut self, counters: &BootstrapCounters, now: Timestamp) {
        self.frontiers_rate
            .sample(counters.received_frontiers as u64, now);
        self.outdated_rate
            .sample(counters.outdated_accounts_found as u64, now);
        self.frontiers_total = counters.received_frontiers;
        self.outdated_total = counters.outdated_accounts_found;
    }

    fn update_frontier_heads(&mut self, heads: &[FrontierHeadInfo]) {
        self.frontier_heads.clear();
        for head in heads {
            self.frontier_heads.push(FrontierHeadViewModel {
                start: abbrev(&head.start),
                end: abbrev(&head.end),
                current: format!("{:.1}%", head.done_normalized() * 100.0),
                done_normalized: head.done_normalized(),
            });
        }
    }

    fn update_outdated_accounts(&mut self, accounts: &VecDeque<Account>) {
        self.outdated_accounts = accounts.iter().rev().map(|a| a.encode_account()).collect();
    }

    pub(crate) fn frontiers_rate(&self) -> i64 {
        self.frontiers_rate.rate()
    }

    pub(crate) fn outdated_rate(&self) -> i64 {
        self.outdated_rate.rate()
    }
}

impl Default for BootstrapViewModel {
    fn default() -> Self {
        Self {
            frontiers_rate: RateCalculator::new(),
            outdated_rate: RateCalculator::new(),
            frontier_heads: Default::default(),
            frontiers_total: 0,
            outdated_total: 0,
            outdated_accounts: Vec::new(),
        }
    }
}

pub(crate) struct FrontierHeadViewModel {
    pub start: String,
    pub end: String,
    pub current: String,
    pub done_normalized: f32,
}

fn abbrev(account: &Account) -> String {
    let mut result = account.encode_hex();
    result.truncate(4);
    result.push_str("...");
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abbreviate_acccount() {
        assert_eq!(abbrev(&Account::from(0)), "0000...");
    }
}
