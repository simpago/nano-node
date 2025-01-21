use crate::message_recorder::MessageRecorder;
use num_format::{Locale, ToFormattedString};
use std::sync::atomic::{AtomicI64, Ordering};

pub(crate) struct MessageStatsViewModel<'a> {
    msg_recorder: &'a MessageRecorder,
}

impl<'a> MessageStatsViewModel<'a> {
    pub fn new(msg_recorder: &'a MessageRecorder) -> Self {
        Self { msg_recorder }
    }

    pub(crate) fn send_rate(&self) -> String {
        Self::to_string(&self.msg_recorder.rates.send_rate)
    }

    pub(crate) fn receive_rate(&self) -> String {
        Self::to_string(&self.msg_recorder.rates.receive_rate)
    }

    fn to_string(value: &AtomicI64) -> String {
        value
            .load(Ordering::SeqCst)
            .to_formatted_string(&Locale::en)
    }
}
