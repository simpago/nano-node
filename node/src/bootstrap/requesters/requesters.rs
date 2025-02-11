use crate::bootstrap::{
    state::BootstrapState, AscPullQuerySpec, BootstrapConfig, BootstrapPromise,
};
use crate::{block_processing::BlockProcessor, stats::Stats, transport::MessageSender};
use rsnano_ledger::Ledger;
use rsnano_network::bandwidth_limiter::RateLimiter;
use rsnano_network::Network;
use rsnano_nullable_clock::SteadyClock;
use std::sync::RwLock;
use std::{
    sync::{Arc, Condvar, Mutex},
    thread::JoinHandle,
};

use super::bootstrap_promise_runner::BootstrapPromiseRunner;
use super::query_sender::QuerySender;
use super::send_queries_promise::SendQueriesPromise;
use super::{
    channel_waiter::ChannelWaiter, dependency_requester::DependencyRequester,
    frontier_requester::FrontierRequester, priority::PriorityRequester,
};

/// Manages the threads that send out AscPullReqs
pub(crate) struct Requesters {
    limiter: Arc<RateLimiter>,
    config: BootstrapConfig,
    stats: Arc<Stats>,
    message_sender: MessageSender,
    state: Arc<Mutex<BootstrapState>>,
    state_changed: Arc<Condvar>,
    clock: Arc<SteadyClock>,
    threads: Mutex<Option<RequesterThreads>>,
    ledger: Arc<Ledger>,
    block_processor: Arc<BlockProcessor>,
    network: Arc<RwLock<Network>>,
}

impl Requesters {
    pub(crate) fn new(
        limiter: Arc<RateLimiter>,
        config: BootstrapConfig,
        stats: Arc<Stats>,
        message_sender: MessageSender,
        state: Arc<Mutex<BootstrapState>>,
        state_changed: Arc<Condvar>,
        clock: Arc<SteadyClock>,
        ledger: Arc<Ledger>,
        block_processor: Arc<BlockProcessor>,
        network: Arc<RwLock<Network>>,
    ) -> Self {
        Self {
            limiter,
            config,
            stats,
            message_sender,
            state,
            state_changed,
            clock,
            ledger,
            block_processor,
            network,
            threads: Mutex::new(None),
        }
    }

    pub fn start(&self) {
        let limiter = self.limiter.clone();
        let max_requests = self.config.max_requests;
        let channel_waiter =
            ChannelWaiter::new(self.network.clone(), limiter.clone(), max_requests);

        let runner = Arc::new(BootstrapPromiseRunner {
            state: self.state.clone(),
            throttle_wait: self.config.throttle_wait,
            state_changed: self.state_changed.clone(),
        });

        let frontiers = if self.config.enable_frontier_scan {
            Some(self.spawn_query(
                "Bootstrap front",
                FrontierRequester::new(
                    self.stats.clone(),
                    self.clock.clone(),
                    self.config.frontier_rate_limit,
                    channel_waiter.clone(),
                ),
                runner.clone(),
            ))
        } else {
            None
        };

        let priorities = if self.config.enable_scan {
            let mut requester = PriorityRequester::new(
                self.block_processor.clone(),
                self.stats.clone(),
                channel_waiter.clone(),
                self.clock.clone(),
                self.ledger.clone(),
                &self.config,
            );
            requester.block_processor_threshold = self.config.block_processor_theshold;

            Some(self.spawn_query("Bootstrap", requester, runner.clone()))
        } else {
            None
        };

        let dependencies = if self.config.enable_dependency_walker {
            Some(self.spawn_query(
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
            let mut state = self.state.lock().unwrap();
            state.stopped = true;
        }
        self.state_changed.notify_all();

        let threads = self.threads.lock().unwrap().take();
        if let Some(mut threads) = threads {
            threads.join();
        }
    }

    fn spawn_query<T>(
        &self,
        name: impl Into<String>,
        query_factory: T,
        runner: Arc<BootstrapPromiseRunner>,
    ) -> JoinHandle<()>
    where
        T: BootstrapPromise<AscPullQuerySpec> + Send + 'static,
    {
        let mut query_sender = QuerySender::new(
            self.message_sender.clone(),
            self.clock.clone(),
            self.stats.clone(),
        );
        query_sender.set_request_timeout(self.config.request_timeout);

        let send_promise = SendQueriesPromise::new(query_factory, query_sender);

        std::thread::Builder::new()
            .name(name.into())
            .spawn(Box::new(move || {
                runner.run(send_promise);
            }))
            .unwrap()
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
