use crate::{message_collection::RecordedMessage, rate_calculator::RateCalculator};
use rsnano_network::ChannelDirection;
use rsnano_nullable_clock::Timestamp;
use std::{
    sync::atomic::{AtomicI64, Ordering},
    time::Duration,
};

#[derive(Default)]
pub(crate) struct MessageRates {
    pub send_rate: AtomicI64,
    pub receive_rate: AtomicI64,
}

#[derive(Default)]
pub(crate) struct MessageRatesCalculator {
    sent: MessageRateCalculator,
    received: MessageRateCalculator,
    last_sample: Option<Timestamp>,
}

impl MessageRatesCalculator {
    pub fn add(&mut self, message: &RecordedMessage, now: Timestamp, result: &MessageRates) {
        let should_sample = self.should_sample(now);

        match message.direction {
            ChannelDirection::Inbound => {
                self.received
                    .message_processed(should_sample, now, &result.receive_rate);
            }
            ChannelDirection::Outbound => {
                self.sent
                    .message_processed(should_sample, now, &result.send_rate);
            }
        };

        if should_sample {
            self.last_sample = Some(now);
        }
    }

    fn should_sample(&self, now: Timestamp) -> bool {
        if let Some(ts) = self.last_sample {
            (now - ts) >= Duration::from_millis(500)
        } else {
            true
        }
    }
}

#[derive(Default)]
struct MessageRateCalculator {
    processed: u64,
    rate_calculator: RateCalculator,
}

impl MessageRateCalculator {
    fn message_processed(&mut self, should_sample: bool, now: Timestamp, result: &AtomicI64) {
        self.processed += 1;
        if should_sample {
            self.rate_calculator.sample(self.processed, now);
            result.store(self.rate_calculator.rate(), Ordering::Relaxed);
        }
    }
}
