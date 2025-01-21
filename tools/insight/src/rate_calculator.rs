use rsnano_nullable_clock::Timestamp;
use std::collections::VecDeque;

#[derive(Default)]
pub(crate) struct RateCalculator {
    values: VecDeque<(u64, Timestamp)>,
}

impl RateCalculator {
    const MAX_SAMPLES: usize = 60;

    pub(crate) fn new() -> Self {
        Self {
            values: VecDeque::new(),
        }
    }

    pub(crate) fn rate(&self) -> i64 {
        if self.values.len() < 2 {
            0
        } else {
            let (val_a, time_a) = *self.values.front().unwrap();
            let (val_b, time_b) = *self.values.back().unwrap();
            let change = val_b as i64 - val_a as i64;
            let time = time_b - time_a;
            (change as f64 / time.as_secs_f64()) as i64
        }
    }

    pub(crate) fn sample(&mut self, value: u64, timestamp: Timestamp) {
        self.values.push_back((value, timestamp));
        if self.values.len() > Self::MAX_SAMPLES {
            self.values.pop_front();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn empty() {
        let rate = RateCalculator::new();
        assert_eq!(rate.rate(), 0);
    }

    #[test]
    fn add_one() {
        let mut rate = RateCalculator::new();
        rate.sample(123, Timestamp::new_test_instance());
        assert_eq!(rate.rate(), 0);
    }

    #[test]
    fn add_two() {
        let mut rate = RateCalculator::new();
        let ts = Timestamp::new_test_instance();
        rate.sample(200, ts);
        rate.sample(300, ts + Duration::from_millis(1000));
        assert_eq!(rate.rate(), 100);
    }

    #[test]
    fn calculate_rate_by_second() {
        let mut rate = RateCalculator::new();
        let ts = Timestamp::new_test_instance();
        rate.sample(200, ts);
        rate.sample(300, ts + Duration::from_millis(100));
        assert_eq!(rate.rate(), 1000);
    }

    #[test]
    fn add_three() {
        let mut rate = RateCalculator::new();
        let ts = Timestamp::new_test_instance();
        rate.sample(200, ts);
        rate.sample(300, ts + Duration::from_millis(1000));
        rate.sample(500, ts + Duration::from_millis(2000));
        assert_eq!(rate.rate(), 150);
    }

    #[test]
    fn limit_to_60_samples() {
        let mut rate = RateCalculator::new();
        let mut ts = Timestamp::new_test_instance();
        for i in 0..60 {
            rate.sample(i * 100, ts);
            ts = ts + Duration::from_millis(500);
        }
        rate.sample(10000, ts);
        assert_eq!(rate.values.len(), 60);
        assert_eq!(rate.rate(), 335);
    }

    #[test]
    fn negative_bps() {
        let mut rate = RateCalculator::new();
        let ts = Timestamp::new_test_instance();
        rate.sample(200, ts);
        rate.sample(300, ts + Duration::from_millis(1000));
        rate.sample(150, ts + Duration::from_millis(2000));
        assert_eq!(rate.rate(), -25);
    }
}
