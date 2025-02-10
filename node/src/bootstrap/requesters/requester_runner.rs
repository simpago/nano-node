use crate::bootstrap::PollResult;
use crate::bootstrap::{
    state::BootstrapState, AscPullQuerySpec, BootstrapConfig, BootstrapPromise,
};
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

    pub fn run_queries<T: BootstrapPromise<AscPullQuerySpec>>(
        &self,
        mut query_factory: T,
        mut query_sender: AscPullQuerySender,
    ) {
        loop {
            let Some(spec) = self.wait_for(&mut query_factory) else {
                return;
            };
            query_sender.send(spec);
        }
    }

    fn wait_for<A, T>(&self, action: &mut A) -> Option<T>
    where
        A: BootstrapPromise<T>,
    {
        let mut interval = Self::INITIAL_INTERVAL;
        let mut guard = self.state.lock().unwrap();
        loop {
            if self.stopped.load(Ordering::SeqCst) {
                return None;
            }

            match self.progress(action, &mut guard, interval) {
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
