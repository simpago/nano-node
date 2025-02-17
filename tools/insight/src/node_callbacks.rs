use std::sync::Arc;

use chrono::Utc;
use rsnano_network::ChannelDirection;
use rsnano_node::NodeCallbacks;
use rsnano_nullable_clock::SteadyClock;

use crate::{message_collection::RecordedMessage, message_recorder::MessageRecorder};

pub(crate) struct NodeCallbackFactory {
    recorder: Arc<MessageRecorder>,
    clock: Arc<SteadyClock>,
}

impl NodeCallbackFactory {
    pub(crate) fn new(recorder: Arc<MessageRecorder>, clock: Arc<SteadyClock>) -> Self {
        Self { recorder, clock }
    }

    pub fn new_null() -> Self {
        Self::new(
            Arc::new(MessageRecorder::default()),
            Arc::new(SteadyClock::new_null()),
        )
    }

    pub fn make_node_callbacks(&self) -> NodeCallbacks {
        let recorder1 = self.recorder.clone();
        let recorder2 = self.recorder.clone();
        let recorder3 = self.recorder.clone();
        let clock1 = self.clock.clone();
        let clock2 = self.clock.clone();
        let clock3 = self.clock.clone();

        NodeCallbacks::builder()
            .on_publish(move |channel_id, message| {
                let recorded = RecordedMessage {
                    channel_id,
                    message: message.clone(),
                    direction: ChannelDirection::Outbound,
                    date: Utc::now(),
                };
                recorder1.record(recorded, clock1.now());
            })
            .on_inbound(move |channel_id, message| {
                let recorded = RecordedMessage {
                    channel_id,
                    message: message.clone(),
                    direction: ChannelDirection::Inbound,
                    date: Utc::now(),
                };
                recorder2.record(recorded, clock2.now());
            })
            .on_inbound_dropped(move |channel_id, message| {
                let recorded = RecordedMessage {
                    channel_id,
                    message: message.clone(),
                    direction: ChannelDirection::Inbound,
                    date: Utc::now(),
                };
                recorder3.record(recorded, clock3.now());
            })
            .finish()
    }
}
