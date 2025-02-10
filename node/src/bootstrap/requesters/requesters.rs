use crate::bootstrap::{
    state::BootstrapState, AscPullQuerySpec, BootstrapConfig, BootstrapPromise,
};
use crate::{block_processing::BlockProcessor, stats::Stats, transport::MessageSender};
use rsnano_ledger::Ledger;
use rsnano_network::bandwidth_limiter::RateLimiter;
use rsnano_nullable_clock::SteadyClock;
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex,
    },
    thread::JoinHandle,
};

use super::priority_pull_count_decider::PriorityPullCountDecider;
use super::priority_pull_type_decider::PriorityPullTypeDecider;
use super::priority_query_factory::PriorityQueryFactory;
use super::requester_runner::RequesterRunner;
use super::{
    channel_waiter::ChannelWaiter, dependency_requester::DependencyRequester,
    frontier_requester::FrontierRequester, priority_requester::PriorityRequester,
};

/// Manages the threads that send out AscPullReqs
pub(crate) struct Requesters {
    limiter: Arc<RateLimiter>,
    config: BootstrapConfig,
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
        let channel_waiter = ChannelWaiter::new(limiter.clone(), max_requests);

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
            let pull_type_decider =
                PriorityPullTypeDecider::new(self.config.optimistic_request_percentage);

            let pull_count_decider = PriorityPullCountDecider::new(self.config.max_pull_count);

            let query_factory = PriorityQueryFactory::new(
                self.clock.clone(),
                self.ledger.clone(),
                pull_type_decider,
                pull_count_decider,
            );

            let mut requester = PriorityRequester::new(
                self.block_processor.clone(),
                self.stats.clone(),
                channel_waiter.clone(),
                query_factory,
            );
            requester.block_processor_threshold = self.config.block_processor_theshold;

            Some(spawn_query("Bootstrap", requester, runner.clone()))
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
    T: BootstrapPromise<AscPullQuerySpec> + Send + 'static,
{
    let message_sender = runner.message_sender.clone();
    std::thread::Builder::new()
        .name(name.into())
        .spawn(Box::new(move || {
            runner.run_queries(query_factory, message_sender)
        }))
        .unwrap()
}
