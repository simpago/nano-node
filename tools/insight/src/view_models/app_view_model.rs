use super::{
    ChannelsViewModel, LedgerStatsViewModel, MessageStatsViewModel, MessageTableViewModel,
    NodeRunnerViewModel, QueueGroupViewModel, TabBarViewModel,
};
use crate::{
    channels::Channels, ledger_stats::LedgerStats, message_collection::MessageCollection,
    message_recorder::MessageRecorder, node_runner::NodeRunner, nullable_runtime::NullableRuntime,
    view_models::QueueViewModel,
};
use rsnano_node::{
    block_processing::BlockSource,
    cementation::ConfirmingSetInfo,
    consensus::{ActiveElectionsInfo, RepTier},
    transport::FairQueueInfo,
};
use rsnano_nullable_clock::{SteadyClock, Timestamp};
use std::{
    sync::{Arc, RwLock},
    time::Duration,
};

pub(crate) struct AppViewModel {
    pub msg_recorder: Arc<MessageRecorder>,
    pub node_runner: NodeRunnerViewModel,
    pub message_table: MessageTableViewModel,
    pub tabs: TabBarViewModel,
    ledger_stats: LedgerStats,
    channels: Channels,
    clock: Arc<SteadyClock>,
    last_update: Option<Timestamp>,
    pub aec_info: ActiveElectionsInfo,
    pub confirming_set: ConfirmingSetInfo,
    pub block_processor_info: FairQueueInfo<BlockSource>,
    pub vote_processor_info: FairQueueInfo<RepTier>,
}

impl AppViewModel {
    pub(crate) fn new(runtime: Arc<NullableRuntime>) -> Self {
        let node_runner = NodeRunner::new(runtime);
        let messages = Arc::new(RwLock::new(MessageCollection::default()));
        let msg_recorder = Arc::new(MessageRecorder::new(messages.clone()));
        let clock = Arc::new(SteadyClock::default());
        Self {
            node_runner: NodeRunnerViewModel::new(node_runner, msg_recorder.clone(), clock.clone()),
            message_table: MessageTableViewModel::new(messages.clone()),
            tabs: TabBarViewModel::new(),
            msg_recorder,
            channels: Channels::new(messages),
            clock,
            ledger_stats: LedgerStats::new(),
            last_update: None,
            aec_info: Default::default(),
            confirming_set: Default::default(),
            block_processor_info: Default::default(),
            vote_processor_info: Default::default(),
        }
    }

    pub(crate) fn with_runtime(runtime: tokio::runtime::Handle) -> Self {
        Self::new(Arc::new(NullableRuntime::new(runtime.clone())))
    }

    pub(crate) fn update(&mut self) {
        let now = self.clock.now();
        if let Some(last_update) = self.last_update {
            if now - last_update < Duration::from_millis(500) {
                return;
            }
        }

        if let Some(node) = self.node_runner.node() {
            self.ledger_stats.update(&node, now);
            let channels = node.network_info.read().unwrap().list_realtime_channels(0);
            let telemetries = node.telemetry.get_all_telemetries();
            let (peered_reps, min_rep_weight) = {
                let guard = node.online_reps.lock().unwrap();
                (guard.peered_reps(), guard.minimum_principal_weight())
            };
            let rep_weights = node.ledger.rep_weights.clone();
            self.channels.update(
                channels,
                telemetries,
                peered_reps,
                &rep_weights,
                min_rep_weight,
            );
            self.aec_info = node.active.info();
            self.confirming_set = node.confirming_set.info();
            self.block_processor_info = node.block_processor.info();
            self.vote_processor_info = node.vote_processor_queue.info();
        }

        self.message_table.update_message_counts();

        self.last_update = Some(now);
    }

    pub(crate) fn message_stats(&self) -> MessageStatsViewModel {
        MessageStatsViewModel::new(&self.msg_recorder)
    }

    pub(crate) fn ledger_stats(&self) -> LedgerStatsViewModel {
        LedgerStatsViewModel::new(&self.ledger_stats)
    }

    pub(crate) fn channels(&mut self) -> ChannelsViewModel {
        ChannelsViewModel::new(&mut self.channels)
    }

    pub(crate) fn queue_groups(&self) -> Vec<QueueGroupViewModel> {
        vec![
            QueueGroupViewModel {
                heading: "Active Elections".to_string(),
                queues: vec![
                    QueueViewModel::new(
                        "Priority",
                        self.aec_info.priority,
                        self.aec_info.max_queue,
                    ),
                    QueueViewModel::new("Hinted", self.aec_info.hinted, self.aec_info.max_queue),
                    QueueViewModel::new(
                        "Optimistic",
                        self.aec_info.optimistic,
                        self.aec_info.max_queue,
                    ),
                    QueueViewModel::new("Total", self.aec_info.total, self.aec_info.max_queue),
                ],
            },
            QueueGroupViewModel::for_fair_queue("Block Processor", &self.block_processor_info),
            QueueGroupViewModel::for_fair_queue("Vote Processor", &self.vote_processor_info),
            QueueGroupViewModel {
                heading: "Miscellaneous".to_string(),
                queues: vec![QueueViewModel::new(
                    "Confirming",
                    self.confirming_set.size,
                    self.confirming_set.max_size,
                )],
            },
        ]
    }
}

impl Default for AppViewModel {
    fn default() -> Self {
        Self::new(Arc::new(NullableRuntime::default()))
    }
}
