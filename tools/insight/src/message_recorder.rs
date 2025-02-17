use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, RwLock,
};

use rsnano_nullable_clock::Timestamp;

use crate::{
    message_collection::{MessageCollection, RecordedMessage},
    message_rate_calculator::{MessageRates, MessageRatesCalculator},
};

pub(crate) struct MessageRecorder {
    pub rates: MessageRates,
    rate_calc: RwLock<MessageRatesCalculator>,
    messages: Arc<RwLock<MessageCollection>>,
    is_recording: AtomicBool,
}

impl MessageRecorder {
    pub(crate) fn new(messages: Arc<RwLock<MessageCollection>>) -> Self {
        Self {
            rates: Default::default(),
            rate_calc: RwLock::new(Default::default()),
            messages,
            is_recording: AtomicBool::new(false),
        }
    }

    pub fn is_recording(&self) -> bool {
        self.is_recording.load(Ordering::SeqCst)
    }

    pub fn start_recording(&self) {
        self.is_recording.store(true, Ordering::SeqCst);
    }

    pub fn stop_recording(&self) {
        self.is_recording.store(false, Ordering::SeqCst);
    }

    pub fn clear(&self) {
        self.messages.write().unwrap().clear();
    }

    pub fn record(&self, message: RecordedMessage, now: Timestamp) {
        {
            let mut rates = self.rate_calc.write().unwrap();
            rates.add(&message, now, &self.rates);
        }

        if self.is_recording() {
            let mut messages = self.messages.write().unwrap();
            messages.add(message);
        }
    }
}

impl Default for MessageRecorder {
    fn default() -> Self {
        Self::new(Arc::new(RwLock::new(MessageCollection::default())))
    }
}
