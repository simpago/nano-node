use crate::bootstrap::state::{QueryType, RunningQuery};
use crate::bootstrap::{state::BootstrapState, AscPullQuerySpec, BootstrapAction, BootstrapConfig};
use crate::{
    bootstrap::WaitResult,
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
    pub fn run_queries<T: BootstrapAction<AscPullQuerySpec>>(
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
        A: BootstrapAction<T>,
    {
        const INITIAL_INTERVAL: Duration = Duration::from_millis(5);
        let mut interval = INITIAL_INTERVAL;
        let mut guard = self.state.lock().unwrap();
        loop {
            if self.stopped.load(Ordering::SeqCst) {
                return None;
            }

            match action.run(&mut *guard, self.clock.now()) {
                WaitResult::BeginWait => {
                    interval = INITIAL_INTERVAL;
                }
                WaitResult::ContinueWait => {
                    interval = min(interval * 2, self.config.throttle_wait);
                }
                WaitResult::Finished(result) => return Some(result),
            }

            guard = self
                .condition
                .wait_timeout_while(guard, interval, |_| !self.stopped.load(Ordering::SeqCst))
                .unwrap()
                .0;
        }
    }

    fn send_request(&self, spec: AscPullQuerySpec, message_sender: &mut MessageSender) {
        let id = thread_rng().next_u64();
        let now = self.clock.now();
        let query = RunningQuery::from_request(id, &spec, now, self.config.request_timeout);

        let request = AscPullReq {
            id,
            req_type: spec.req_type,
        };

        let mut guard = self.state.lock().unwrap();
        guard.running_queries.insert(query);
        let message = Message::AscPullReq(request);
        let sent = message_sender.try_send_channel(
            &spec.channel,
            &message,
            TrafficType::BootstrapRequests,
        );

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
