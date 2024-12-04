use super::OnlineReps;
use rsnano_core::Amount;
use rsnano_ledger::RepWeightCache;
use std::{sync::Arc, time::Duration};

pub const DEFAULT_ONLINE_WEIGHT_MINIMUM: Amount = Amount::nano(60_000_000);

pub struct OnlineRepsBuilder {
    rep_weights: Option<Arc<RepWeightCache>>,
    weight_period: Duration,
    online_weight_minimum: Amount,
    trended: Option<Amount>,
}

impl OnlineRepsBuilder {
    pub(super) fn new() -> Self {
        Self {
            rep_weights: None,
            weight_period: Duration::from_secs(5 * 60),
            online_weight_minimum: DEFAULT_ONLINE_WEIGHT_MINIMUM,
            trended: None,
        }
    }
    pub fn rep_weights(mut self, weights: Arc<RepWeightCache>) -> Self {
        self.rep_weights = Some(weights);
        self
    }

    pub fn weight_period(mut self, period: Duration) -> Self {
        self.weight_period = period;
        self
    }

    pub fn online_weight_minimum(mut self, minimum: Amount) -> Self {
        self.online_weight_minimum = minimum;
        self
    }

    pub fn trended(mut self, trended: Amount) -> Self {
        self.trended = Some(trended);
        self
    }

    pub fn finish(self) -> OnlineReps {
        let rep_weights = self
            .rep_weights
            .unwrap_or_else(|| Arc::new(RepWeightCache::new()));

        let mut online_reps =
            OnlineReps::new(rep_weights, self.weight_period, self.online_weight_minimum);
        if let Some(trended) = self.trended {
            online_reps.set_trended(trended);
        }
        online_reps
    }
}
