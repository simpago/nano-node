use std::collections::VecDeque;

/// Used to throttle the ascending bootstrapper once it reaches a steady state
/// Tracks verify_result samples and signals throttling if no tracked samples have gotten results
pub(crate) struct Throttle {
    /// Bit set that tracks sample results. True when something was retrieved, false otherwise
    samples: VecDeque<bool>,
    successes: usize,
}

impl Throttle {
    pub fn new(size: usize) -> Self {
        debug_assert!(size > 0);
        Self {
            samples: vec![true; size].into(),
            successes: size,
        }
    }

    #[allow(dead_code)]
    pub fn throttled(&self) -> bool {
        self.successes == 0
    }

    pub fn add(&mut self, sample: bool) {
        self.pop();
        self.samples.push_back(sample);
        if sample {
            self.successes += 1;
        }
    }

    /// Resizes the number of samples tracked
    /// Drops the oldest samples if the size decreases
    /// Adds false samples if the size increases
    pub fn resize(&mut self, size: usize) {
        debug_assert!(size > 0);
        while self.samples.len() > size {
            self.pop();
        }
        while self.samples.len() < size {
            self.samples.push_back(false);
        }
    }

    pub fn len(&self) -> usize {
        self.samples.len()
    }

    pub fn successes(&self) -> usize {
        self.successes
    }

    fn pop(&mut self) {
        if let Some(sample) = self.samples.pop_front() {
            if sample {
                self.successes -= 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty() {
        let throttle = Throttle::new(2);
        assert_eq!(throttle.throttled(), false);
    }

    #[test]
    fn throttled() {
        let mut throttle = Throttle::new(2);
        throttle.add(false);
        assert_eq!(throttle.throttled(), false);
        throttle.add(false);
        assert_eq!(throttle.throttled(), true);
    }

    #[test]
    fn resize_up() {
        let mut throttle = Throttle::new(2);
        throttle.add(false);
        throttle.resize(4);
        assert_eq!(throttle.throttled(), false);
        throttle.add(false);
        assert_eq!(throttle.throttled(), true);
    }

    #[test]
    fn resize_down() {
        let mut throttle = Throttle::new(4);
        throttle.add(false);
        throttle.resize(2);
        assert_eq!(throttle.throttled(), false);
        throttle.add(false);
        assert_eq!(throttle.throttled(), true);
    }
}
