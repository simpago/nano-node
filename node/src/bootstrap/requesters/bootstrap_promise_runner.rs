use crate::bootstrap::{state::BootstrapState, BootstrapPromise, PollResult};
use std::{
    cmp::min,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex,
    },
    time::Duration,
};

/// Calls a requester to create a bootstrap request and then sends it to
/// the peered node
pub(crate) struct BootstrapPromiseRunner {
    pub state: Arc<Mutex<BootstrapState>>,
    pub throttle_wait: Duration,
    pub stopped: Arc<AtomicBool>,
    pub stopped_notification: Arc<Condvar>,
}

impl BootstrapPromiseRunner {
    const INITIAL_INTERVAL: Duration = Duration::from_millis(5);

    pub fn run<P, T>(&self, mut promise: P) -> Option<T>
    where
        P: BootstrapPromise<T>,
    {
        let mut interval = Self::INITIAL_INTERVAL;
        let mut guard = self.state.lock().unwrap();
        loop {
            if self.stopped.load(Ordering::SeqCst) {
                return None;
            }

            match self.progress(&mut promise, &mut guard, interval) {
                Ok(result) => return Some(result),
                Err(i) => interval = i,
            }

            guard = self
                .stopped_notification
                .wait_timeout_while(guard, interval, |_| !self.stopped.load(Ordering::SeqCst))
                .unwrap()
                .0;
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
                    if self.stopped.load(Ordering::SeqCst) {
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
        let state = Arc::new(Mutex::new(BootstrapState::new_test_instance()));
        let stopped = Arc::new(AtomicBool::new(false));
        let stopped_notification = Arc::new(Condvar::new());

        let runner = BootstrapPromiseRunner {
            state: state.clone(),
            throttle_wait: Duration::from_millis(100),
            stopped: stopped.clone(),
            stopped_notification: stopped_notification.clone(),
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
            let _guard = state.lock().unwrap();
            stopped.store(true, Ordering::SeqCst);
        }
        stopped_notification.notify_all();
        finished.wait();
    }

    struct StubPromise {
        polled: Arc<OneShotNotification>,
    }

    impl StubPromise {
        fn new() -> Self {
            Self {
                polled: Arc::new(OneShotNotification::new()),
            }
        }
    }

    impl BootstrapPromise<()> for StubPromise {
        fn poll(&mut self, _state: &mut BootstrapState) -> PollResult<()> {
            self.polled.notify();
            PollResult::Wait
        }
    }
}
