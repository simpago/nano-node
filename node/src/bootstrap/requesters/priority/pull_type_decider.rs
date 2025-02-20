use rand::Rng;
use rsnano_nullable_random::NullableRng;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PullType {
    /// Optimistic requests start from the (possibly unconfirmed) account frontier
    /// and are vulnerable to bootstrap poisoning.
    Optimistic,
    /// Safe requests start from the confirmed frontier and given enough time
    /// will eventually resolve forks
    Safe,
}

/// Decides whether to make an optimistic of a safe priority pull request
pub(super) struct PullTypeDecider {
    optimistic_request_percentage: u8,
    rng: NullableRng,
}

impl PullTypeDecider {
    const DEFAULT_OPTIMISTIC_REQUEST_PERCENTAGE: u8 = 75;

    #[allow(dead_code)]
    pub fn new_null_with(result: PullType) -> Self {
        let rnd_result = match result {
            PullType::Optimistic => 0,
            PullType::Safe => 100,
        };
        Self::with(
            rng_that_returns(rnd_result),
            Self::DEFAULT_OPTIMISTIC_REQUEST_PERCENTAGE,
        )
    }

    pub(super) fn new(optimistic_request_percentage: u8) -> Self {
        Self::with(NullableRng::rng(), optimistic_request_percentage)
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
    pub fn decide_pull_type(&mut self) -> PullType {
        if self.rng.random_range(0..100) < self.optimistic_request_percentage {
            PullType::Optimistic
        } else {
            PullType::Safe
        }
    }
}

impl Default for PullTypeDecider {
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
        assert_eq!(rng_that_returns(75).random_range(0..=100), 75_u8);
        assert_eq!(rng_that_returns(74).random_range(0..=100), 74_u8);
        assert_eq!(rng_that_returns(0).random_range(0..=100), 0_u8);
        assert_eq!(rng_that_returns(100).random_range(0..=100), 100_u8);
    }

    #[test]
    fn decide_optimistic() {
        let rng = rng_that_returns(75);
        let mut decider = PullTypeDecider::with(rng, 75);
        assert_eq!(decider.decide_pull_type(), PullType::Optimistic);
        assert_eq!(decider.decide_pull_type(), PullType::Optimistic);
        assert_eq!(decider.decide_pull_type(), PullType::Optimistic);
    }

    #[test]
    fn decide_safe() {
        let rng = rng_that_returns(76);
        let mut decider = PullTypeDecider::with(rng, 75);
        assert_eq!(decider.decide_pull_type(), PullType::Safe);
        assert_eq!(decider.decide_pull_type(), PullType::Safe);
        assert_eq!(decider.decide_pull_type(), PullType::Safe);
    }

    #[test]
    fn can_be_nulled() {
        let mut optimistic_decider = PullTypeDecider::new_null_with(PullType::Optimistic);
        let mut safe_decider = PullTypeDecider::new_null_with(PullType::Safe);

        assert_eq!(optimistic_decider.decide_pull_type(), PullType::Optimistic);
        assert_eq!(safe_decider.decide_pull_type(), PullType::Safe);
    }

    #[test]
    fn default() {
        let mut decider = PullTypeDecider::default();
        let _ = decider.decide_pull_type();
    }
}
