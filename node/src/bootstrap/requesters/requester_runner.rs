use crate::bootstrap::state::{QueryType, RunningQuery};
use crate::bootstrap::{
    state::BootstrapState, AscPullQuerySpec, BootstrapConfig, BootstrapPromise,
};
use crate::{
    bootstrap::PollResult,
    stats::{DetailType, StatType, Stats},
    transport::MessageSender,
};
use rand::{thread_rng, RngCore};
use rsnano_messages::{AscPullReq, Message};
use rsnano_network::TrafficType;
use rsnano_nullable_clock::SteadyClock;
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
pub(crate) struct RequesterRunner {
    pub message_sender: MessageSender,
    pub state: Arc<Mutex<BootstrapState>>,
    pub clock: Arc<SteadyClock>,
    pub config: BootstrapConfig,
    pub stats: Arc<Stats>,
    pub stopped: Arc<AtomicBool>,
    pub condition: Arc<Condvar>,
}

impl RequesterRunner {
    const INITIAL_INTERVAL: Duration = Duration::from_millis(5);

    pub fn run_queries<T: BootstrapPromise<AscPullQuerySpec>>(
        &self,
        mut query_factory: T,
        mut message_sender: MessageSender,
    ) {
        loop {
            let Some(spec) = self.wait_for(&mut query_factory) else {
                return;
            };
            self.send_request(spec, &mut message_sender);
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

    fn send_request(&self, spec: AscPullQuerySpec, message_sender: &mut MessageSender) {
        let id = thread_rng().next_u64();
        let now = self.clock.now();
        let query = RunningQuery::from_spec(id, &spec, now, self.config.request_timeout);

        let request = AscPullReq {
            id,
            req_type: spec.req_type,
        };

        let mut guard = self.state.lock().unwrap();
        guard.running_queries.insert(query);
        let message = Message::AscPullReq(request);
        let sent = message_sender.try_send(&spec.channel, &message, TrafficType::BootstrapRequests);

        if sent {
            self.stats.inc(StatType::Bootstrap, DetailType::Request);
            let query_type = QueryType::from(&message);
            self.stats
                .inc(StatType::BootstrapRequest, query_type.into());
        } else {
            self.stats
                .inc(StatType::Bootstrap, DetailType::RequestFailed);
        }

        if sent {
            // After the request has been sent, the peer has a limited time to respond
            let response_cutoff = now + self.config.request_timeout;
            guard.set_response_cutoff(id, response_cutoff);
        } else {
            guard.remove_query(id);
        }
        if sent && spec.cooldown_account {
            guard.candidate_accounts.timestamp_set(&spec.account, now);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
