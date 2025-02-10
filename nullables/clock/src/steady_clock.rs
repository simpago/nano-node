use std::{
    collections::VecDeque,
    ops::{Add, Sub},
    sync::Mutex,
    time::{Duration, Instant},
};

pub struct SteadyClock {
    time_source: TimeSource,
}

impl SteadyClock {
    pub fn new_null() -> Self {
        let mut offsets = VecDeque::new();
        offsets.push_back(DEFAULT_STUB_DURATION);
        Self {
            time_source: TimeSource::Stub(Mutex::new(offsets)),
        }
    }

    pub fn new_null_with(now: Timestamp) -> Self {
        Self::new_null_with_offsets([Duration::from_millis(now.0 as u64)])
    }

    pub fn new_null_with_offsets(offsets: impl IntoIterator<Item = Duration>) -> Self {
        let mut last = DEFAULT_STUB_DURATION;
        let mut nows = VecDeque::new();
        nows.push_back(last);
        for offset in offsets.into_iter() {
            let now = last + offset.as_millis() as i64;
            nows.push_back(now);
            last = now;
        }
        Self {
            time_source: TimeSource::Stub(Mutex::new(nows)),
        }
    }

    pub fn now(&self) -> Timestamp {
        Timestamp(self.time_source.now())
    }
}

impl Default for SteadyClock {
    fn default() -> Self {
        SteadyClock {
            time_source: TimeSource::System(Instant::now()),
        }
    }
}

enum TimeSource {
    System(Instant),
    Stub(Mutex<VecDeque<i64>>),
}

impl TimeSource {
    fn now(&self) -> i64 {
        match self {
            TimeSource::System(instant) => instant.elapsed().as_millis() as i64,
            TimeSource::Stub(nows) => {
                let mut guard = nows.lock().unwrap();
                if guard.len() == 1 {
                    *guard.front().unwrap()
                } else {
                    guard.pop_front().unwrap()
                }
            }
        }
    }
}

const DEFAULT_STUB_DURATION: i64 = 1000 * 60 * 60 * 24 * 365;

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Copy, Default, Hash)]
pub struct Timestamp(i64);

impl Timestamp {
    pub const MAX: Self = Self(i64::MAX);

    pub const fn new_test_instance() -> Self {
        Self(DEFAULT_STUB_DURATION)
    }

    pub fn elapsed(&self, now: Timestamp) -> Duration {
        Duration::from_millis(now.0.checked_sub(self.0).unwrap_or_default() as u64)
    }

    pub fn checked_sub(&self, rhs: Duration) -> Option<Self> {
        self.0.checked_sub(rhs.as_millis() as i64).map(Self)
    }

    pub const DEFAULT_STUB_NOW: Timestamp = Timestamp(DEFAULT_STUB_DURATION);
}

impl Add<Duration> for Timestamp {
    type Output = Timestamp;

    fn add(self, rhs: Duration) -> Self::Output {
        Self(self.0.add(rhs.as_millis() as i64))
    }
}

impl Sub<Timestamp> for Timestamp {
    type Output = Duration;

    fn sub(self, rhs: Timestamp) -> Self::Output {
        Duration::from_millis((self.0 - rhs.0) as u64)
    }
}

impl Sub<Duration> for Timestamp {
    type Output = Timestamp;

    fn sub(self, rhs: Duration) -> Self::Output {
        Self(self.0 - rhs.as_millis() as i64)
    }
}

impl From<i64> for Timestamp {
    fn from(value: i64) -> Self {
        Self(value)
    }
}

impl From<Timestamp> for i64 {
    fn from(value: Timestamp) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    mod timestamp {
        use super::*;

        #[test]
        fn add_duration() {
            assert_eq!(
                Timestamp::from(1000) + Duration::from_millis(300),
                Timestamp::from(1300)
            );
        }

        #[test]
        fn sub() {
            assert_eq!(
                Timestamp::from(1000) - Timestamp::from(300),
                Duration::from_millis(700)
            );
        }
    }

    #[test]
    fn now() {
        let clock = SteadyClock::default();
        let now1 = clock.now();
        sleep(Duration::from_millis(1));
        let now2 = clock.now();
        assert!(now2 > now1);
    }

    mod nullability {
        use super::*;

        #[test]
        fn can_be_nulled() {
            let clock = SteadyClock::new_null();
            let now1 = clock.now();
            let now2 = clock.now();
            assert_eq!(now1, now2);
        }

        #[test]
        fn configure_multiple_responses() {
            let clock = SteadyClock::new_null_with_offsets([
                Duration::from_secs(1),
                Duration::from_secs(10),
                Duration::from_secs(3),
            ]);
            let now1 = clock.now();
            let now2 = clock.now();
            let now3 = clock.now();
            let now4 = clock.now();
            let now5 = clock.now();
            let now6 = clock.now();
            assert_eq!(now2, now1 + Duration::from_secs(1));
            assert_eq!(now3, now2 + Duration::from_secs(10));
            assert_eq!(now4, now3 + Duration::from_secs(3));
            assert_eq!(now5, now4);
            assert_eq!(now6, now4);
        }
    }
}
