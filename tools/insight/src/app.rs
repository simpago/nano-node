use std::sync::{Arc, RwLock};

use rsnano_nullable_clock::SteadyClock;

use crate::{
    channels::Channels, explorer::Explorer, message_collection::MessageCollection,
    message_recorder::MessageRecorder, node_callbacks::NodeCallbackFactory,
    node_runner::NodeRunner,
};

pub(crate) struct InsightApp {
    pub clock: Arc<SteadyClock>,
    pub messages: Arc<RwLock<MessageCollection>>,
    pub msg_recorder: Arc<MessageRecorder>,
    pub node_runner: NodeRunner,
    pub channels: Channels,
    pub explorer: Explorer,
}

impl InsightApp {
    pub fn new() -> Self {
        let clock = Arc::new(SteadyClock::default());
        let messages = Arc::new(RwLock::new(MessageCollection::default()));
        let msg_recorder = Arc::new(MessageRecorder::new(messages.clone()));
        let callback_factory = NodeCallbackFactory::new(msg_recorder.clone(), clock.clone());
        let channels = Channels::new(messages.clone());
        Self {
            clock,
            messages,
            msg_recorder,
            node_runner: NodeRunner::new(callback_factory),
            channels,
            explorer: Explorer::new(),
        }
    }
}
