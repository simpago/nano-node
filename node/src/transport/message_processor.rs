use super::{InboundMessageQueue, RealtimeMessageHandler};
use crate::config::NodeConfig;
use rsnano_messages::Message;
use rsnano_network::{Channel, ChannelId};
use std::{
    cmp::{max, min},
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::Instant,
};
use tracing::debug;

#[derive(Clone, Debug, PartialEq)]
pub struct MessageProcessorConfig {
    pub threads: usize,
    pub max_queue: usize,
}

impl MessageProcessorConfig {
    pub fn new(parallelism: usize) -> Self {
        Self {
            threads: min(2, max(parallelism / 4, 1)),
            max_queue: 64,
        }
    }
}

/// Process inbound messages from other nodes
pub struct MessageProcessor {
    config: NodeConfig,
    processing_threads: Vec<JoinHandle<()>>,
    state: Arc<State>,
}

impl MessageProcessor {
    pub fn new(
        config: NodeConfig,
        inbound_queue: Arc<InboundMessageQueue>,
        realtime_handler: Arc<RealtimeMessageHandler>,
    ) -> Self {
        Self {
            config,
            processing_threads: Vec::new(),
            state: Arc::new(State {
                inbound_queue,
                realtime_handler,
                stopped: AtomicBool::new(false),
            }),
        }
    }

    pub fn start(&mut self) {
        for _ in 0..self.config.network_threads {
            let state = self.state.clone();
            self.processing_threads.push(
                std::thread::Builder::new()
                    .name("Msg processing".to_string())
                    .spawn(move || {
                        state.run();
                    })
                    .unwrap(),
            );
        }
    }

    pub fn stop(&mut self) {
        self.state.stopped.store(true, Ordering::SeqCst);
        self.state.inbound_queue.stop();
        for t in self.processing_threads.drain(..) {
            t.join().unwrap();
        }
    }
}

impl Drop for MessageProcessor {
    fn drop(&mut self) {
        // All threads must be stopped before this destructor
        debug_assert!(self.processing_threads.is_empty());
    }
}

struct State {
    stopped: AtomicBool,
    realtime_handler: Arc<RealtimeMessageHandler>,
    inbound_queue: Arc<InboundMessageQueue>,
}

impl State {
    const MAX_BATCH_SIZE: usize = 1024 * 4;

    fn run(&self) {
        while !self.stopped.load(Ordering::SeqCst) {
            let batch = self.inbound_queue.next_batch(Self::MAX_BATCH_SIZE);
            if !batch.is_empty() {
                self.handle_batch(batch);
            } else {
                self.inbound_queue.wait_for_messages();
            }
        }
    }

    fn handle_batch(&self, batch: VecDeque<(ChannelId, (Message, Arc<Channel>))>) {
        let start = Instant::now();
        let batch_size = batch.len();
        for (_, (message, channel)) in batch {
            self.realtime_handler.process(message, &channel);
        }

        let elapsed_millis = start.elapsed().as_millis();
        if elapsed_millis > 100 {
            debug!(
                "Processed {} messages in {} milliseconds (rate of {} messages per second)",
                batch_size,
                elapsed_millis,
                batch_size as u128 * 1000 / elapsed_millis
            );
        }
    }
}
