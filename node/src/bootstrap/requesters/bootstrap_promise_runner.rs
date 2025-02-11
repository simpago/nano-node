use crate::bootstrap::{state::BootstrapState, BootstrapPromise, PollResult};
use std::{
    cmp::min,
    sync::{Arc, Condvar, Mutex},
    time::Duration,
};

/// Calls a requester to create a bootstrap request and then sends it to
/// the peered node
pub(crate) struct BootstrapPromiseRunner {
    pub state: Arc<Mutex<BootstrapState>>,
    pub throttle_wait: Duration,
    pub state_changed: Arc<Condvar>,
}

impl BootstrapPromiseRunner {
    const INITIAL_INTERVAL: Duration = Duration::from_millis(5);

    pub fn run<P, T>(&self, mut promise: P) -> Option<T>
    where
        P: BootstrapPromise<T>,
    {
        let mut interval = Self::INITIAL_INTERVAL;
        let mut state = self.state.lock().unwrap();
        loop {
            if state.stopped {
                return None;
            }

            match self.progress(&mut promise, &mut state, interval) {
                Ok(result) => return Some(result),
                Err(i) => interval = i,
            }

            state = self.state_changed.wait_timeout(state, interval).unwrap().0;
        }
    }

    fn progress<A, T>(
        &self,
        action: &mut A,
        state: &mut BootstrapState,
        mut wait_interval: Duration,
    ) -> Result<T, Duration>
    where
        A: BootstrapPromise<T>,
    {
        let mut reset_wait_interval = false;
        loop {
            match action.poll(state) {
                PollResult::Progress => {
                    if state.stopped {
                        return Err(Self::INITIAL_INTERVAL);
                    }
                    reset_wait_interval = true;
                }
                PollResult::Wait => {
                    wait_interval = if reset_wait_interval {
                        Self::INITIAL_INTERVAL
                    } else {
                        min(wait_interval * 2, self.throttle_wait)
                    };
                    return Err(wait_interval);
                }
                PollResult::Finished(result) => return Ok(result),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsnano_core::utils::OneShotNotification;
    use std::thread::spawn;

    #[test]
    fn abort_promise_when_stopped() {
        let state = Arc::new(Mutex::new(BootstrapState::default()));
        let state_changed = Arc::new(Condvar::new());

        let runner = BootstrapPromiseRunner {
            state: state.clone(),
            throttle_wait: Duration::from_millis(100),
            state_changed: state_changed.clone(),
        };

        let promise = StubPromise::new();
        let polled = promise.polled.clone();
        let finished = Arc::new(OneShotNotification::new());
        let finished2 = finished.clone();

        spawn(move || {
            runner.run(promise);
            finished2.notify();
        });

        polled.wait();
        {
            let mut state = state.lock().unwrap();
            state.stopped = true;
        }
        state_changed.notify_all();
        finished.wait();
    }

    #[test]
    fn return_result_when_finished() {
        let state = Arc::new(Mutex::new(BootstrapState::default()));
        let state_changed = Arc::new(Condvar::new());

        let runner = BootstrapPromiseRunner {
            state: state.clone(),
            throttle_wait: Duration::from_millis(100),
            state_changed: state_changed.clone(),
        };

        let mut promise = StubPromise::new();
        promise.result = Some(42);

        let result = runner.run(promise);

        assert_eq!(result, Some(42));
    }

    struct StubPromise {
        polled: Arc<OneShotNotification>,
        result: Option<i32>,
    }

    impl StubPromise {
        fn new() -> Self {
            Self {
                polled: Arc::new(OneShotNotification::new()),
                result: None,
            }
        }
    }

    impl BootstrapPromise<i32> for StubPromise {
        fn poll(&mut self, _state: &mut BootstrapState) -> PollResult<i32> {
            self.polled.notify();
            if let Some(result) = self.result.take() {
                PollResult::Finished(result)
            } else {
                PollResult::Wait
            }
        }
    }
}
