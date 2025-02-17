use std::{
    sync::{Arc, RwLock},
    time::Duration,
};

use rsnano_core::utils::FairQueueInfo;
use rsnano_node::{
    block_processing::BlockSource,
    cementation::ConfirmingSetInfo,
    consensus::{ActiveElectionsInfo, RepTier},
};
use rsnano_nullable_clock::{SteadyClock, Timestamp};

use crate::{
    channels::Channels,
    explorer::Explorer,
    ledger_stats::LedgerStats,
    message_collection::MessageCollection,
    message_recorder::MessageRecorder,
    navigator::{NavItem, Navigator},
    node_callbacks::NodeCallbackFactory,
    node_runner::NodeRunner,
};

pub(crate) struct InsightApp {
    last_update: Option<Timestamp>,
    pub clock: Arc<SteadyClock>,
    pub messages: Arc<RwLock<MessageCollection>>,
    pub msg_recorder: Arc<MessageRecorder>,
    pub node_runner: NodeRunner,
    pub channels: Channels,
    pub explorer: Explorer,
    pub navigator: Navigator,
    pub ledger_stats: LedgerStats,
    pub aec_info: ActiveElectionsInfo,
    pub confirming_set: ConfirmingSetInfo,
    pub block_processor_info: FairQueueInfo<BlockSource>,
    pub vote_processor_info: FairQueueInfo<RepTier>,
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
            navigator: Navigator::new(),
            ledger_stats: LedgerStats::new(),
            aec_info: Default::default(),
            confirming_set: Default::default(),
            block_processor_info: Default::default(),
            vote_processor_info: Default::default(),
            last_update: None,
        }
    }

    pub fn search(&mut self, input: &str) {
        if let Some(node) = self.node_runner.node() {
            let has_result = self.explorer.search(&node.ledger, input);
            if has_result {
                self.navigator.current = NavItem::Explorer;
            }
        }
    }

    pub(crate) fn update(&mut self) -> bool {
        let now = self.clock.now();
        if let Some(last_update) = self.last_update {
            if now - last_update < Duration::from_millis(500) {
                return false;
            }
        }

        if let Some(node) = self.node_runner.node() {
            self.ledger_stats.update(&node, now);
            let channels = node.network.read().unwrap().sorted_channels();
            let telemetries = node.telemetry.get_all_telemetries();
            let (peered_reps, min_rep_weight) = {
                let guard = node.online_reps.lock().unwrap();
                (guard.peered_reps(), guard.minimum_principal_weight())
            };
            self.channels
                .update(channels, telemetries, peered_reps, min_rep_weight);
            self.aec_info = node.active.info();
            self.confirming_set = node.confirming_set.info();
            self.block_processor_info = node.block_processor.info();
            self.vote_processor_info = node.vote_processor_queue.info();
        }

        self.last_update = Some(now);
        true
    }
}
