use rsnano_node::Node;
use rsnano_nullable_clock::Timestamp;

use crate::rate_calculator::RateCalculator;

pub(crate) struct LedgerStats {
    pub total_blocks: u64,
    pub cemented_blocks: u64,
    cps_rate: RateCalculator,
    bps_rate: RateCalculator,
}

impl LedgerStats {
    pub(crate) fn new() -> Self {
        Self {
            total_blocks: 0,
            cemented_blocks: 0,
            cps_rate: RateCalculator::new(),
            bps_rate: RateCalculator::new(),
        }
    }

    pub(crate) fn update(&mut self, node: &Node, now: Timestamp) {
        self.total_blocks = node.ledger.block_count();
        self.cemented_blocks = node.ledger.cemented_count();
        self.cps_rate.sample(self.cemented_blocks, now);
        self.bps_rate.sample(self.total_blocks, now);
    }

    pub(crate) fn blocks_per_second(&self) -> i64 {
        self.bps_rate.rate()
    }

    pub(crate) fn confirmations_per_second(&self) -> i64 {
        self.cps_rate.rate()
    }
}
