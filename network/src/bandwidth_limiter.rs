use rsnano_core::utils::ContainerInfo;

use crate::{token_bucket::TokenBucket, TrafficType};
use std::sync::Mutex;

pub struct RateLimiter {
    bucket: Mutex<TokenBucket>,
}

impl RateLimiter {
    pub fn new(limit: usize) -> Self {
        Self::with_burst_ratio(limit, 1.0)
    }

    pub fn with_burst_ratio(limit: usize, limit_burst_ratio: f64) -> Self {
        Self {
            bucket: Mutex::new(TokenBucket::new(
                (limit as f64 * limit_burst_ratio) as usize,
                limit,
            )),
        }
    }

    pub fn should_pass(&self, message_size: usize) -> bool {
        self.bucket.lock().unwrap().try_consume(message_size)
    }

    pub fn reset(&self) {
        self.bucket.lock().unwrap().reset()
    }

    pub fn size(&self) -> usize {
        self.bucket.lock().unwrap().size()
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct BandwidthLimiterConfig {
    pub generic_limit: usize,
    pub generic_burst_ratio: f64,

    pub bootstrap_limit: usize,
    pub bootstrap_burst_ratio: f64,
}

impl Default for BandwidthLimiterConfig {
    fn default() -> Self {
        Self {
            generic_limit: 10 * 1024 * 1024,
            generic_burst_ratio: 3_f64,
            bootstrap_limit: 5 * 1024 * 1024,
            bootstrap_burst_ratio: 1_f64,
        }
    }
}

pub struct BandwidthLimiter {
    limiter_generic: RateLimiter,
    limiter_bootstrap: RateLimiter,
}

impl BandwidthLimiter {
    pub fn new(config: BandwidthLimiterConfig) -> Self {
        Self {
            limiter_generic: RateLimiter::with_burst_ratio(
                config.generic_limit,
                config.generic_burst_ratio,
            ),
            limiter_bootstrap: RateLimiter::with_burst_ratio(
                config.bootstrap_limit,
                config.bootstrap_burst_ratio,
            ),
        }
    }

    /**
     * Check whether packet falls withing bandwidth limits and should be allowed
     * @return true if OK, false if needs to be dropped
     */
    pub fn should_pass(&self, buffer_size: usize, limit_type: TrafficType) -> bool {
        self.select_limiter(limit_type).should_pass(buffer_size)
    }

    fn select_limiter(&self, limit_type: TrafficType) -> &RateLimiter {
        match limit_type {
            TrafficType::BootstrapServer => &self.limiter_bootstrap,
            _ => &self.limiter_generic,
        }
    }

    pub fn container_info(&self) -> ContainerInfo {
        [
            ("generic", self.limiter_generic.size(), 0),
            ("bootstrap", self.limiter_bootstrap.size(), 0),
        ]
        .into()
    }
}

impl Default for BandwidthLimiter {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mock_instant::thread_local::MockClock;
    use std::time::Duration;

    #[test]
    fn test_limit() {
        let limiter = RateLimiter::with_burst_ratio(10, 1.5);
        assert_eq!(limiter.should_pass(15), true);
        assert_eq!(limiter.should_pass(1), false);
        MockClock::advance(Duration::from_millis(100));
        assert_eq!(limiter.should_pass(1), true);
        assert_eq!(limiter.should_pass(1), false);
    }
}
