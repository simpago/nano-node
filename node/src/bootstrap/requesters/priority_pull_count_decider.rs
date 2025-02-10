use crate::bootstrap::{state::Priority, BootstrapResponder};
use num::clamp;
use rsnano_messages::BlocksAckPayload;
use std::cmp::min;

/// Decides how many blocks to pull
pub(super) struct PriorityPullCountDecider {
    pub max_pull_count: u8,
}

impl PriorityPullCountDecider {
    pub fn new(max_pull_count: u8) -> Self {
        Self { max_pull_count }
    }

    pub fn pull_count(&self, priority: Priority) -> u8 {
        // Decide how many blocks to request
        const MIN_PULL_COUNT: u8 = 2;

        let pull_count = clamp(
            f64::from(priority) as u8,
            MIN_PULL_COUNT,
            BootstrapResponder::MAX_BLOCKS,
        );

        // Limit the max number of blocks to pull
        min(pull_count, self.max_pull_count) as u8
    }
}

impl Default for PriorityPullCountDecider {
    fn default() -> Self {
        Self::new(BlocksAckPayload::MAX_BLOCKS)
    }
}

#[cfg(test)]
mod tests {
    use super::PriorityPullCountDecider;
    use crate::bootstrap::{state::Priority, BootstrapResponder};

    #[test]
    fn min_count() {
        assert_pull_count(Priority::ZERO, 2);
    }

    #[test]
    fn priority_equals_pull_count() {
        assert_pull_count(Priority::new(5.1), 5);
        assert_pull_count(Priority::new(5.9), 5);
        assert_pull_count(Priority::new(6.0), 6);
    }

    #[test]
    fn max() {
        assert_pull_count(Priority::new(999.0), BootstrapResponder::MAX_BLOCKS as u8);
    }

    #[test]
    fn configured_max() {
        let max = 5;
        let decider = PriorityPullCountDecider::new(max);
        assert_eq!(decider.pull_count(Priority::new(999.0)), max);
    }

    fn assert_pull_count(priority: Priority, expected: u8) {
        let decider = PriorityPullCountDecider::default();
        assert_eq!(decider.pull_count(priority), expected);
    }
}
