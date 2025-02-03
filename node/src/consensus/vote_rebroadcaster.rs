use crate::{transport::MessageFlooder, wallets::Wallets};
use rsnano_core::{BlockHash, Vote, VoteCode};
use rsnano_messages::{ConfirmAck, Message};
use rsnano_network::TrafficType;
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Condvar, Mutex,
    },
    thread::JoinHandle,
};

pub(crate) struct VoteRebroadcaster {
    queue: Arc<VoteRebroadcastQueue>,
    join_handle: Option<JoinHandle<()>>,
    wallets: Arc<Wallets>,
    message_flooder: MessageFlooder,
}

impl VoteRebroadcaster {
    pub(crate) fn new(
        queue: Arc<VoteRebroadcastQueue>,
        wallets: Arc<Wallets>,
        message_flooder: MessageFlooder,
    ) -> Self {
        Self {
            queue,
            wallets,
            join_handle: None,
            message_flooder,
        }
    }

    pub fn start(&mut self) {
        let mut rebroadcast_loop = RebroadcastLoop {
            queue: self.queue.clone(),
            wallets: self.wallets.clone(),
            message_flooder: self.message_flooder.clone(),
        };

        let handle = std::thread::Builder::new()
            .name("Vote rebroad".to_owned())
            .spawn(move || rebroadcast_loop.run())
            .unwrap();
        self.join_handle = Some(handle);
    }

    pub fn stop(&mut self) {
        self.queue.stop();
        if let Some(handle) = self.join_handle.take() {
            handle.join().unwrap();
        }
    }
}

struct RebroadcastLoop {
    queue: Arc<VoteRebroadcastQueue>,
    wallets: Arc<Wallets>,
    message_flooder: MessageFlooder,
}

impl RebroadcastLoop {
    fn run(&mut self) {
        while let Some(vote) = self.queue.dequeue() {
            // Republish vote if it is new and the node does not host a principal representative (or close to)
            if self
                .wallets
                .should_republish_vote(vote.voting_account.into())
            {
                let ack = Message::ConfirmAck(ConfirmAck::new_with_rebroadcasted_vote(
                    vote.as_ref().clone(),
                ));
                self.message_flooder
                    .flood(&ack, TrafficType::VoteRebroadcast, 0.5);
            }
        }
    }
}

pub(crate) struct VoteRebroadcastQueue {
    queue: Mutex<VecDeque<Arc<Vote>>>,
    enqueued: Condvar,
    stopped: AtomicBool,
}

impl VoteRebroadcastQueue {
    const MAX_QUEUE: usize = 1024 * 16;

    pub(crate) fn new() -> Self {
        Self {
            queue: Mutex::new(Default::default()),
            enqueued: Condvar::new(),
            stopped: AtomicBool::new(false),
        }
    }

    pub fn handle_processed_vote(&self, vote: &Arc<Vote>, results: &HashMap<BlockHash, VoteCode>) {
        let processed = results.iter().any(|(_, code)| *code == VoteCode::Vote);
        if processed {
            self.enqueue(vote.clone());
        }
    }

    pub fn enqueue(&self, vote: Arc<Vote>) {
        let added = {
            let mut queue = self.queue.lock().unwrap();
            if queue.len() < Self::MAX_QUEUE && !self.stopped() {
                queue.push_back(vote);
                true
            } else {
                false
            }
        };

        if added {
            self.enqueued.notify_all();
        }
    }

    /// This will wait for a vote to be enqueued or for the
    /// queue to be stopped.
    pub fn dequeue(&self) -> Option<Arc<Vote>> {
        let mut queue = self.queue.lock().unwrap();

        queue = self
            .enqueued
            .wait_while(queue, |q| q.len() == 0 && !self.stopped())
            .unwrap();

        return queue.pop_front();
    }

    pub fn stopped(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
    }

    pub fn stop(&self) {
        {
            let _guard = self.queue.lock().unwrap();
            self.stopped.store(true, Ordering::SeqCst);
        }
        self.enqueued.notify_all();
    }
}
