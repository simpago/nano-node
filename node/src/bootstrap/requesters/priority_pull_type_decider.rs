use rand::Rng;
use rsnano_nullable_random::NullableRng;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PriorityPullType {
    /// Optimistic requests start from the (possibly unconfirmed) account frontier
    /// and are vulnerable to bootstrap poisoning.
    Optimistic,
    /// Safe requests start from the confirmed frontier and given enough time
    /// will eventually resolve forks
    Safe,
}

/// Decides whether to make an optimistic of a safe priority pull request
pub(super) struct PriorityPullTypeDecider {
    optimistic_request_percentage: u8,
    rng: NullableRng,
}

impl PriorityPullTypeDecider {
    const DEFAULT_OPTIMISTIC_REQUEST_PERCENTAGE: u8 = 75;

    #[allow(dead_code)]
    pub fn new_null_with(result: PriorityPullType) -> Self {
        let rnd_result = match result {
            PriorityPullType::Optimistic => 0,
            PriorityPullType::Safe => 100,
        };
        Self::with(
            rng_that_returns(rnd_result),
            Self::DEFAULT_OPTIMISTIC_REQUEST_PERCENTAGE,
        )
    }

    pub(super) fn new(optimistic_request_percentage: u8) -> Self {
        Self::with(NullableRng::thread_rng(), optimistic_request_percentage)
    }

    fn with(rng: NullableRng, optimistic_request_percentage: u8) -> Self {
        Self {
            optimistic_request_percentage,
            rng,
        }
    }
    /// Probabilistically choose between requesting blocks from account frontier
    /// or confirmed frontier.
    /// Optimistic requests start from the (possibly unconfirmed) account frontier
    /// and are vulnerable to bootstrap poisoning.
    /// Safe requests start from the confirmed frontier and given enough time
    /// will eventually resolve forks
    pub fn decide_pull_type(&mut self) -> PriorityPullType {
        if self.rng.gen_range(0..100) < self.optimistic_request_percentage {
            PriorityPullType::Optimistic
        } else {
            PriorityPullType::Safe
        }
    }
}

impl Default for PriorityPullTypeDecider {
    fn default() -> Self {
        Self::new(Self::DEFAULT_OPTIMISTIC_REQUEST_PERCENTAGE)
    }
}

fn rng_that_returns(result: u8) -> NullableRng {
    NullableRng::new_null_bytes(&((result as f32 / 100.0 * 255.0) as u32).to_ne_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rng() {
        assert_eq!(rng_that_returns(75).gen_range(0..=100), 75_u8);
        assert_eq!(rng_that_returns(74).gen_range(0..=100), 74_u8);
        assert_eq!(rng_that_returns(0).gen_range(0..=100), 0_u8);
        assert_eq!(rng_that_returns(100).gen_range(0..=100), 100_u8);
    }

    #[test]
    fn decide_optimistic() {
        let rng = rng_that_returns(75);
        let mut decider = PriorityPullTypeDecider::with(rng, 75);
        assert_eq!(decider.decide_pull_type(), PriorityPullType::Optimistic);
        assert_eq!(decider.decide_pull_type(), PriorityPullType::Optimistic);
        assert_eq!(decider.decide_pull_type(), PriorityPullType::Optimistic);
    }

    #[test]
    fn decide_safe() {
        let rng = rng_that_returns(76);
        let mut decider = PriorityPullTypeDecider::with(rng, 75);
        assert_eq!(decider.decide_pull_type(), PriorityPullType::Safe);
        assert_eq!(decider.decide_pull_type(), PriorityPullType::Safe);
        assert_eq!(decider.decide_pull_type(), PriorityPullType::Safe);
    }

    #[test]
    fn can_be_nulled() {
        let mut optimistic_decider =
            PriorityPullTypeDecider::new_null_with(PriorityPullType::Optimistic);
        let mut safe_decider = PriorityPullTypeDecider::new_null_with(PriorityPullType::Safe);

        assert_eq!(
            optimistic_decider.decide_pull_type(),
            PriorityPullType::Optimistic
        );
        assert_eq!(safe_decider.decide_pull_type(), PriorityPullType::Safe);
    }

    #[test]
    fn default() {
        let mut decider = PriorityPullTypeDecider::default();
        let _ = decider.decide_pull_type();
    }
}
