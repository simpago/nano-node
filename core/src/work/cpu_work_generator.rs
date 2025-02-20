use crate::{
    difficulty::{Difficulty, DifficultyV1},
    Root,
};

use super::{WorkGenerator, WorkRng, WorkTicket, XorShift1024Star};
#[cfg(test)]
use std::sync::{Arc, Mutex};
use std::{thread, time::Duration};

pub(crate) trait Sleeper {
    fn sleep(&mut self, duration: Duration);
}

pub(crate) struct ThreadSleeper {}

impl ThreadSleeper {
    pub(crate) fn new() -> Self {
        Self {}
    }
}

impl Sleeper for ThreadSleeper {
    fn sleep(&mut self, duration: Duration) {
        thread::sleep(duration);
    }
}

#[cfg(test)]
pub(crate) struct StubSleeper {
    calls: Arc<Mutex<Vec<Duration>>>,
}

#[cfg(test)]
impl StubSleeper {
    pub(crate) fn new() -> Self {
        Self {
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub(crate) fn calls(&self) -> Vec<Duration> {
        self.calls.lock().unwrap().clone()
    }
}

#[cfg(test)]
impl Sleeper for StubSleeper {
    fn sleep(&mut self, duration: Duration) {
        let mut lock = self.calls.lock().unwrap();
        lock.push(duration);
    }
}

#[cfg(test)]
impl Clone for StubSleeper {
    fn clone(&self) -> Self {
        Self {
            calls: Arc::clone(&self.calls),
        }
    }
}

pub(crate) struct CpuWorkGenerator<
    Rng = XorShift1024Star,
    Diff = DifficultyV1,
    Sleep = ThreadSleeper,
> where
    Rng: WorkRng,
    Diff: Difficulty,
    Sleep: Sleeper,
{
    // Quick RNG for work attempts.
    rng: Rng,
    difficulty: Diff,
    sleeper: Sleep,
    rate_limiter: Duration,
    pub iteration_size: usize,
}

const DEFAULT_ITERATION_SIZE: usize = 256;

pub(crate) fn create_cpu_work_generator<R, D, S>(
    rng: R,
    difficulty: D,
    sleeper: S,
    rate_limiter: Duration,
) -> CpuWorkGenerator<R, D, S>
where
    R: WorkRng,
    D: Difficulty,
    S: Sleeper,
{
    CpuWorkGenerator {
        rng,
        difficulty,
        sleeper,
        rate_limiter,
        iteration_size: DEFAULT_ITERATION_SIZE,
    }
}

impl CpuWorkGenerator {
    pub fn new(rate_limiter: Duration) -> Self {
        create_cpu_work_generator(
            XorShift1024Star::new(),
            DifficultyV1::default(),
            ThreadSleeper::new(),
            rate_limiter,
        )
    }
}

// Single threaded PoW generation on the CPU
impl<Rng, Diff, Sleep> CpuWorkGenerator<Rng, Diff, Sleep>
where
    Rng: WorkRng,
    Diff: Difficulty,
    Sleep: Sleeper,
{
    fn next(&mut self, item: &Root) -> (u64, u64) {
        let work = self.rng.next_work();
        let difficulty = self.difficulty.get_difficulty(item, work);
        (work, difficulty)
    }

    /// Tries to create PoW in a batch of 256 iterations
    fn try_create_batch(&mut self, item: &Root, min_difficulty: u64) -> Option<u64> {
        // Don't query main memory every iteration in order to reduce memory bus traffic
        // All operations here operate on stack memory
        // Count iterations down to zero since comparing to zero is easier than comparing to another number
        let mut iteration = self.iteration_size;
        let mut work = 0;
        let mut difficulty = 0;
        while difficulty < min_difficulty && iteration > 0 {
            (work, difficulty) = self.next(item);
            iteration -= 1;
        }

        if difficulty >= min_difficulty {
            Some(work)
        } else {
            None
        }
    }
}

impl<Rng, Diff, Sleep> WorkGenerator for CpuWorkGenerator<Rng, Diff, Sleep>
where
    Rng: WorkRng,
    Diff: Difficulty,
    Sleep: Sleeper,
{
    fn create(
        &mut self,
        item: &Root,
        min_difficulty: u64,
        work_ticket: &WorkTicket,
    ) -> Option<u64> {
        while !work_ticket.expired() {
            let result = self.try_create_batch(item, min_difficulty);
            if result.is_some() {
                return result;
            }

            // Add a rate limiter (if specified) to the pow calculation to save some CPUs which don't want to operate at full throttle
            if !self.rate_limiter.is_zero() {
                self.sleeper.sleep(self.rate_limiter);
            }
        }
        None
    }
}

#[cfg(test)]
pub(crate) struct StubWorkRng {
    preset_random_numbers: Vec<u64>,
    index: usize,
}

#[cfg(test)]
impl StubWorkRng {
    pub(crate) fn new(preset_random_numbers: Vec<u64>) -> Self {
        Self {
            preset_random_numbers,
            index: 0,
        }
    }
}

#[cfg(test)]
impl WorkRng for StubWorkRng {
    fn next_work(&mut self) -> u64 {
        let result = self.preset_random_numbers[self.index];
        self.index += 1;
        result
    }
}

#[cfg(test)]
mod tests {
    use crate::StubDifficulty;

    use super::*;

    #[test]
    fn stub_work_rng() {
        let mut rng = StubWorkRng::new(vec![1, 2, 3]);
        assert_eq!(rng.next_work(), 1);
        assert_eq!(rng.next_work(), 2);
        assert_eq!(rng.next_work(), 3);
    }

    #[test]
    #[should_panic]
    fn stub_work_rng_out_of_bounds() {
        let mut rng = StubWorkRng::new(vec![1]);
        assert_eq!(rng.next_work(), 1);
        let _ = rng.next_work();
    }

    #[test]
    fn initialization() {
        let rate_limiter = Duration::from_millis(100);
        let generator = CpuWorkGenerator::new(rate_limiter);
        assert_eq!(generator.iteration_size, 256);
        assert_eq!(generator.rate_limiter, rate_limiter);
    }

    #[test]
    fn create_work() {
        let root = Root::from(1);
        let work = 2;
        let difficulty = 100;

        let stub_rng = StubWorkRng::new(vec![work]);
        let mut difficulty_calc = StubDifficulty::new();
        difficulty_calc.set_difficulty(root, work, difficulty);
        let mut generator =
            create_cpu_work_generator(stub_rng, difficulty_calc, StubSleeper::new(), RATE_LIMIT);

        let result = generator.create(&root, difficulty, &WorkTicket::never_expires());

        assert_eq!(result, Some(work))
    }

    #[test]
    fn create_work_multiple_tries() {
        let root = Root::from(1);
        let work = 5;
        let difficulty = 100;

        let stub_rng = StubWorkRng::new(vec![1, 2, 3, 4, work]);
        let mut difficulty_calc = StubDifficulty::new();
        difficulty_calc.set_difficulty(root, work, difficulty);

        let sleeper = StubSleeper::new();
        let mut generator =
            create_cpu_work_generator(stub_rng, difficulty_calc, sleeper.clone(), RATE_LIMIT);

        let result = generator.create(&root, difficulty, &WorkTicket::never_expires());

        assert_eq!(result, Some(work));
        assert!(sleeper.calls().is_empty());
    }

    #[test]
    fn rate_limit() {
        let root = Root::from(1);
        let work = 5;
        let difficulty = 100;

        let stub_rng = StubWorkRng::new(vec![1, 2, 3, 4, work]);
        let mut difficulty_calc = StubDifficulty::new();
        difficulty_calc.set_difficulty(root, work, difficulty);

        let sleeper = StubSleeper::new();
        let mut generator =
            create_cpu_work_generator(stub_rng, difficulty_calc, sleeper.clone(), RATE_LIMIT);
        generator.iteration_size = 2;

        let result = generator.create(&root, difficulty, &WorkTicket::never_expires());

        assert_eq!(result, Some(work));
        assert_eq!(sleeper.calls(), vec![RATE_LIMIT, RATE_LIMIT]);
    }

    #[test]
    fn expired_work_ticket() {
        let root = Root::from(1);

        let stub_rng = StubWorkRng::new(vec![]);
        let sleeper = StubSleeper::new();
        let mut generator =
            create_cpu_work_generator(stub_rng, StubDifficulty::new(), sleeper.clone(), RATE_LIMIT);

        let result = generator.create(&root, 100, &WorkTicket::already_expired());

        assert_eq!(result, None);
        assert_eq!(sleeper.calls(), vec![]);
    }

    const RATE_LIMIT: Duration = Duration::from_millis(1000);
}
