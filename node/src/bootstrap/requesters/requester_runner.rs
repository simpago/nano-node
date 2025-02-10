use crate::bootstrap::{state::BootstrapState, BootstrapConfig, BootstrapPromise};
use crate::bootstrap::{AscPullQuerySpec, PollResult};
use std::{
    cmp::min,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex,
    },
    time::Duration,
};

use super::asc_pull_query_sender::AscPullQuerySender;

/// Calls a requester to create a bootstrap request and then sends it to
/// the peered node
pub(crate) struct BootstrapPromiseRunner {
    pub state: Arc<Mutex<BootstrapState>>,
    pub config: BootstrapConfig,
    pub stopped: Arc<AtomicBool>,
    pub condition: Arc<Condvar>,
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
                .condition
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
                        min(wait_interval * 2, self.config.throttle_wait)
                    };
                    return Err(wait_interval);
                }
                PollResult::Finished(result) => return Ok(result),
            }
        }
    }
}

pub(crate) struct SendAscPullQueryPromise<T>
where
    T: BootstrapPromise<AscPullQuerySpec>,
{
    query_promise: T,
    sender: AscPullQuerySender,
}

impl<T> SendAscPullQueryPromise<T>
where
    T: BootstrapPromise<AscPullQuerySpec>,
{
    pub(crate) fn new(query_promise: T, sender: AscPullQuerySender) -> Self {
        Self {
            query_promise,
            sender,
        }
    }
}

impl<T> BootstrapPromise<()> for SendAscPullQueryPromise<T>
where
    T: BootstrapPromise<AscPullQuerySpec>,
{
    fn poll(&mut self, state: &mut BootstrapState) -> PollResult<()> {
        match self.query_promise.poll(state) {
            PollResult::Progress => PollResult::Progress,
            PollResult::Wait => PollResult::Wait,
            PollResult::Finished(spec) => {
                self.sender.send(spec, state);
                PollResult::Progress
            }
        }
    }
}
