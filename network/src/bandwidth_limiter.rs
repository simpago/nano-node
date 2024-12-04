use crate::{token_bucket::TokenBucket, TrafficType};
use std::sync::Mutex;

pub struct RateLimiter {
    bucket: Mutex<TokenBucket>,
}

impl RateLimiter {
    pub fn new(limit_burst_ratio: f64, limit: usize) -> Self {
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

    pub fn reset(&self, limit_burst_ratio: f64, limit: usize) {
        self.bucket
            .lock()
            .unwrap()
            .reset((limit as f64 * limit_burst_ratio) as usize, limit)
    }
}

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
            limiter_generic: RateLimiter::new(config.generic_burst_ratio, config.generic_limit),
            limiter_bootstrap: RateLimiter::new(
                config.bootstrap_burst_ratio,
                config.bootstrap_limit,
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

    pub fn reset(&self, limit: usize, burst_ratio: f64, limit_type: TrafficType) {
        self.select_limiter(limit_type).reset(burst_ratio, limit);
    }

    fn select_limiter(&self, limit_type: TrafficType) -> &RateLimiter {
        match limit_type {
            TrafficType::Generic => &self.limiter_generic,
            TrafficType::Bootstrap => &self.limiter_bootstrap,
        }
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
        let limiter = RateLimiter::new(1.5, 10);
        assert_eq!(limiter.should_pass(15), true);
        assert_eq!(limiter.should_pass(1), false);
        MockClock::advance(Duration::from_millis(100));
        assert_eq!(limiter.should_pass(1), true);
        assert_eq!(limiter.should_pass(1), false);
    }
}
