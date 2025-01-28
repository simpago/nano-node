use crate::bootstrap::{
    bootstrap_state::BootstrapState,
    channel_waiter::ChannelWaiter,
    running_query_container::{QueryType, RunningQuery},
    AscPullQuerySpec, BootstrapAction, BootstrapConfig,
};
use crate::{
    block_processing::BlockProcessor,
    bootstrap::WaitResult,
    stats::{DetailType, StatType, Stats},
    transport::MessageSender,
    utils::ThreadPoolImpl,
};
use rand::{thread_rng, RngCore};
use rsnano_ledger::Ledger;
use rsnano_messages::{AscPullReq, Message};
use rsnano_network::{bandwidth_limiter::RateLimiter, TrafficType};
use rsnano_nullable_clock::SteadyClock;
use std::{
    cmp::min,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex,
    },
    thread::JoinHandle,
    time::Duration,
};

use super::{
    dependency_requester::DependencyRequester, frontier_requester::FrontierRequester,
    priority_requester::PriorityRequester,
};

/// Manages the threads that send out AscPullReqs
pub(crate) struct Requesters {
    limiter: Arc<RateLimiter>,
    config: BootstrapConfig,
    workers: Arc<ThreadPoolImpl>,
    stats: Arc<Stats>,
    message_sender: MessageSender,
    state: Arc<Mutex<BootstrapState>>,
    condition: Arc<Condvar>,
    clock: Arc<SteadyClock>,
    threads: Mutex<Option<RequesterThreads>>,
    stopped: Arc<AtomicBool>,
    ledger: Arc<Ledger>,
    block_processor: Arc<BlockProcessor>,
}

impl Requesters {
    pub(crate) fn new(
        limiter: Arc<RateLimiter>,
        config: BootstrapConfig,
        workers: Arc<ThreadPoolImpl>,
        stats: Arc<Stats>,
        message_sender: MessageSender,
        state: Arc<Mutex<BootstrapState>>,
        condition: Arc<Condvar>,
        clock: Arc<SteadyClock>,
        ledger: Arc<Ledger>,
        block_processor: Arc<BlockProcessor>,
    ) -> Self {
        Self {
            limiter,
            config,
            workers,
            stats,
            message_sender,
            state,
            condition,
            clock,
            ledger,
            block_processor,
            threads: Mutex::new(None),
            stopped: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn start(&self) {
        let limiter = self.limiter.clone();
        let max_requests = self.config.max_requests;
        let channel_waiter = Arc::new(move || ChannelWaiter::new(limiter.clone(), max_requests));

        let runner = Arc::new(RequesterRunner {
            message_sender: self.message_sender.clone(),
            state: self.state.clone(),
            clock: self.clock.clone(),
            config: self.config.clone(),
            stats: self.stats.clone(),
            condition: self.condition.clone(),
            stopped: self.stopped.clone(),
        });

        let frontiers = if self.config.enable_frontier_scan {
            Some(spawn_query(
                "Bootstrap front",
                FrontierRequester::new(
                    self.workers.clone(),
                    self.stats.clone(),
                    self.config.frontier_rate_limit,
                    self.config.frontier_scan.max_pending,
                    channel_waiter.clone(),
                ),
                runner.clone(),
            ))
        } else {
            None
        };

        let priorities = if self.config.enable_scan {
            Some(spawn_query(
                "Bootstrap",
                PriorityRequester::new(
                    self.ledger.clone(),
                    self.block_processor.clone(),
                    self.stats.clone(),
                    channel_waiter.clone(),
                    self.config.clone(),
                ),
                runner.clone(),
            ))
        } else {
            None
        };

        let dependencies = if self.config.enable_dependency_walker {
            Some(spawn_query(
                "Bootstrap walkr",
                DependencyRequester::new(self.stats.clone(), channel_waiter),
                runner.clone(),
            ))
        } else {
            None
        };

        let requesters = RequesterThreads {
            priorities,
            frontiers,
            dependencies,
        };

        *self.threads.lock().unwrap() = Some(requesters);
    }

    pub fn stop(&self) {
        {
            let _guard = self.state.lock().unwrap();
            self.stopped.store(true, Ordering::SeqCst);
        }
        self.condition.notify_all();

        let threads = self.threads.lock().unwrap().take();
        if let Some(mut threads) = threads {
            threads.join();
        }
    }
}

pub struct RequesterThreads {
    pub priorities: Option<JoinHandle<()>>,
    pub dependencies: Option<JoinHandle<()>>,
    pub frontiers: Option<JoinHandle<()>>,
}

impl RequesterThreads {
    pub fn join(&mut self) {
        if let Some(handle) = self.priorities.take() {
            handle.join().unwrap();
        }
        if let Some(dependencies) = self.dependencies.take() {
            dependencies.join().unwrap();
        }
        if let Some(frontiers) = self.frontiers.take() {
            frontiers.join().unwrap();
        }
    }
}

fn spawn_query<T>(
    name: impl Into<String>,
    query_factory: T,
    runner: Arc<RequesterRunner>,
) -> JoinHandle<()>
where
    T: BootstrapAction<AscPullQuerySpec> + Send + 'static,
{
    let message_sender = runner.message_sender.clone();
    std::thread::Builder::new()
        .name(name.into())
        .spawn(Box::new(move || {
            runner.run_queries(query_factory, message_sender)
        }))
        .unwrap()
}

/// Calls a requester to create a bootstrap request and then sends it to
/// the peered node
struct RequesterRunner {
    message_sender: MessageSender,
    state: Arc<Mutex<BootstrapState>>,
    clock: Arc<SteadyClock>,
    config: BootstrapConfig,
    stats: Arc<Stats>,
    stopped: Arc<AtomicBool>,
    condition: Arc<Condvar>,
}

impl RequesterRunner {
    fn run_queries<T: BootstrapAction<AscPullQuerySpec>>(
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
