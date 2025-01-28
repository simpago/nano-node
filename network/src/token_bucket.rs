#[cfg(test)]
use mock_instant::thread_local::Instant;
use std::time::Duration;
#[cfg(not(test))]
use std::time::Instant;

/**
 * Token bucket based rate limiting. This is suitable for rate limiting ipc/api calls
 * and network traffic, while allowing short bursts.
 *
 * Tokens are refilled at N tokens per second and there's a bucket capacity to limit
 * bursts.
 *
 * A bucket has low overhead and can be instantiated for various purposes, such as one
 * bucket per session, or one for bandwidth limiting. A token can represent bytes,
 * messages, or the cost of API invocations.
 */
pub struct TokenBucket {
    last_refill: Instant,
    current_size: usize,
    max_token_count: usize,

    /** The minimum observed bucket size, from which the largest burst can be derived */
    smallest_size: usize,
    refill_rate: usize,
}

const UNLIMITED: usize = 1_000_000_000;

impl TokenBucket {
    /**
     * Set up a token bucket.
     * @param max_token_count Maximum number of tokens in this bucket, which limits bursts.
     * @param refill_rate Token refill rate, which limits the long term rate (tokens per seconds)
     */
    pub fn new(max_token_count: usize, refill_rate: usize) -> Self {
        let mut result = Self {
            last_refill: Instant::now(),
            max_token_count,
            refill_rate,
            current_size: 0,
            smallest_size: 0,
        };

        result.reset_with(max_token_count, refill_rate);
        result
    }

    /**
     * Determine if an operation of cost \p tokens_required_a is possible, and deduct from the
     * bucket if that's the case.
     * The default cost is 1 token, but resource intensive operations may request
     * more tokens to be available.
     */
    pub fn try_consume(&mut self, tokens_required: usize) -> bool {
        debug_assert!(tokens_required <= UNLIMITED);
        self.refill();
        let possible = self.current_size >= tokens_required;
        if possible {
            self.current_size -= tokens_required;
        } else if tokens_required == UNLIMITED {
            self.current_size = 0;
        }

        // Keep track of smallest observed bucket size so burst size can be computed (for tests and stats)
        self.smallest_size = std::cmp::min(self.smallest_size, self.current_size);

        possible || self.refill_rate == UNLIMITED
    }

    pub fn reset(&mut self) {
        self.reset_with(self.max_token_count, self.refill_rate);
    }

    /** Update the max_token_count and/or refill_rate_a parameters */
    pub fn reset_with(&mut self, mut max_token_count: usize, mut refill_rate: usize) {
        // A token count of 0 indicates unlimited capacity. We use 1e9 as
        // a sentinel, allowing largest burst to still be computed.
        if max_token_count == 0 || refill_rate == 0 {
            refill_rate = UNLIMITED;
            max_token_count = UNLIMITED;
        }
        self.smallest_size = max_token_count;
        self.max_token_count = max_token_count;
        self.current_size = max_token_count;
        self.refill_rate = refill_rate;
        self.last_refill = Instant::now()
    }

    /** Returns the largest burst observed */
    #[allow(dead_code)]
    pub fn largest_burst(&self) -> usize {
        self.max_token_count - self.smallest_size
    }

    pub fn size(&self) -> usize {
        self.current_size
    }

    fn refill(&mut self) {
        let tokens_to_add =
            (self.elapsed().as_nanos() as f64 / 1e9_f64 * self.refill_rate as f64) as usize;
        // Only update if there are any tokens to add
        if tokens_to_add > 0 {
            self.current_size =
                std::cmp::min(self.current_size + tokens_to_add, self.max_token_count);
            self.last_refill = Instant::now();
        }
    }

    fn elapsed(&mut self) -> Duration {
        Instant::now().duration_since(self.last_refill)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mock_instant::thread_local::MockClock;

    #[test]
    fn basic() {
        let mut bucket = TokenBucket::new(10, 10);

        // Initial burst
        assert_eq!(bucket.try_consume(10), true);
        assert_eq!(bucket.try_consume(10), false);

        // With a fill rate of 10 tokens/sec, await 1/3 sec and get 3 tokens
        MockClock::advance(Duration::from_millis(300));
        assert_eq!(bucket.try_consume(3), true);
        assert_eq!(bucket.try_consume(10), false);

        // Allow time for the bucket to completely refill and do a full burst
        MockClock::advance(Duration::from_secs(1));
        assert_eq!(bucket.try_consume(10), true);
        assert_eq!(bucket.largest_burst(), 10);
    }

    #[test]
    fn network() {
        // For the purpose of the test, one token represents 1MB instead of one byte.
        // Allow for 10 mb/s bursts (max bucket size), 5 mb/s long term rate
        let mut bucket = TokenBucket::new(10, 5);

        // Initial burst of 10 mb/s over two calls
        assert_eq!(bucket.try_consume(5), true);
        assert_eq!(bucket.largest_burst(), 5);
        assert_eq!(bucket.try_consume(5), true);
        assert_eq!(bucket.largest_burst(), 10);
        assert_eq!(bucket.try_consume(5), false);

        // After 200 ms, the 5 mb/s fillrate means we have 1 mb available
        MockClock::advance(Duration::from_millis(200));
        assert_eq!(bucket.try_consume(1), true);
        assert_eq!(bucket.try_consume(1), false);
    }

    #[test]
    fn reset() {
        let mut bucket = TokenBucket::new(0, 0);

        // consume lots of tokens, buckets should be unlimited
        assert!(bucket.try_consume(1000000));
        assert!(bucket.try_consume(1000000));

        // set bucket to be limited
        bucket.reset_with(1000, 1000);
        assert_eq!(bucket.try_consume(1001), false);
        assert_eq!(bucket.try_consume(1000), true);
        assert_eq!(bucket.try_consume(1000), false);
        MockClock::advance(Duration::from_millis(2));
        assert_eq!(bucket.try_consume(2), true);

        // reduce the limit
        bucket.reset_with(100, 100 * 1000);
        assert_eq!(bucket.try_consume(101), false);
        assert_eq!(bucket.try_consume(100), true);
        MockClock::advance(Duration::from_millis(1));
        assert_eq!(bucket.try_consume(100), true);

        // increase the limit
        bucket.reset_with(2000, 1);
        assert_eq!(bucket.try_consume(2001), false);
        assert_eq!(bucket.try_consume(2000), true);

        // back to unlimited
        bucket.reset_with(0, 0);
        assert_eq!(bucket.try_consume(1000000), true);
        assert_eq!(bucket.try_consume(1000000), true);
    }

    #[test]
    fn unlimited_rate() {
        let mut bucket = TokenBucket::new(0, 0);
        assert_eq!(bucket.try_consume(5), true);
        assert_eq!(bucket.largest_burst(), 5);
        assert_eq!(bucket.try_consume(1_000_000_000), true);
        assert_eq!(bucket.largest_burst(), 1_000_000_000);

        // With unlimited tokens, consuming always succeed
        assert_eq!(bucket.try_consume(1_000_000_000), true);
        assert_eq!(bucket.largest_burst(), 1_000_000_000);
    }

    #[test]
    fn busy_spin() {
        // Bucket should refill at a rate of 1 token per second
        let mut bucket = TokenBucket::new(1, 1);

        // Run a very tight loop for 5 seconds + a bit of wiggle room
        let mut counter = 0;
        let start = Instant::now();
        let mut now = start;
        while now < start + Duration::from_millis(5500) {
            if bucket.try_consume(1) {
                counter += 1;
            }

            MockClock::advance(Duration::from_millis(250));
            now = Instant::now();
        }

        // Bucket starts fully refilled, therefore we see 1 additional request
        assert_eq!(counter, 6);
    }
}
