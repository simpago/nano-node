use std::time::Duration;

use rsnano_core::utils::FairQueueInfo;
use rsnano_node::{
    block_processing::BlockSource,
    cementation::ConfirmingSetInfo,
    consensus::{ActiveElectionsInfo, RepTier},
};
use rsnano_nullable_clock::Timestamp;

use super::{
    BlockViewModel, BootstrapViewModel, ChannelsViewModel, LedgerStatsViewModel,
    MessageStatsViewModel, MessageTableViewModel, QueueGroupViewModel, SearchBarViewModel, Tab,
    TabBarViewModel,
};
use crate::{
    app::InsightApp,
    explorer::{Explorer, ExplorerState},
    ledger_stats::LedgerStats,
    view_models::QueueViewModel,
};

pub(crate) struct AppViewModel {
    pub app: InsightApp,
    pub message_table: MessageTableViewModel,
    pub tabs: TabBarViewModel,
    ledger_stats: LedgerStats,
    last_update: Option<Timestamp>,
    pub aec_info: ActiveElectionsInfo,
    pub confirming_set: ConfirmingSetInfo,
    pub block_processor_info: FairQueueInfo<BlockSource>,
    pub vote_processor_info: FairQueueInfo<RepTier>,
    pub bootstrap: BootstrapViewModel,
    pub search_bar: SearchBarViewModel,
    pub explorer: Explorer,
}

impl AppViewModel {
    pub(crate) fn new() -> Self {
        let app = InsightApp::new();
        let message_table = MessageTableViewModel::new(app.messages.clone());

        Self {
            app,
            message_table,
            tabs: TabBarViewModel::new(),
            ledger_stats: LedgerStats::new(),
            last_update: None,
            aec_info: Default::default(),
            confirming_set: Default::default(),
            block_processor_info: Default::default(),
            vote_processor_info: Default::default(),
            bootstrap: Default::default(),
            search_bar: Default::default(),
            explorer: Explorer::new(),
        }
    }

    pub(crate) fn update(&mut self) {
        let now = self.app.clock.now();
        if let Some(last_update) = self.last_update {
            if now - last_update < Duration::from_millis(500) {
                return;
            }
        }

        if let Some(node) = self.app.node_runner.node() {
            self.ledger_stats.update(&node, now);
            let channels = node.network.read().unwrap().sorted_channels();
            let telemetries = node.telemetry.get_all_telemetries();
            let (peered_reps, min_rep_weight) = {
                let guard = node.online_reps.lock().unwrap();
                (guard.peered_reps(), guard.minimum_principal_weight())
            };
            self.app
                .channels
                .update(channels, telemetries, peered_reps, min_rep_weight);
            self.aec_info = node.active.info();
            self.confirming_set = node.confirming_set.info();
            self.block_processor_info = node.block_processor.info();
            self.vote_processor_info = node.vote_processor_queue.info();
            self.bootstrap.update(&node.bootstrapper, now);
        }

        self.message_table.update_message_counts();

        self.last_update = Some(now);
    }

    pub(crate) fn message_stats(&self) -> MessageStatsViewModel {
        MessageStatsViewModel::new(&self.app.msg_recorder)
    }

    pub(crate) fn ledger_stats(&self) -> LedgerStatsViewModel {
        LedgerStatsViewModel::new(&self.ledger_stats)
    }

    pub(crate) fn channels(&mut self) -> ChannelsViewModel {
        ChannelsViewModel::new(&mut self.app.channels)
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

    pub fn search(&mut self) {
        if let Some(node) = self.app.node_runner.node() {
            let has_result = self.explorer.search(&node.ledger, &self.search_bar.input);
            if has_result {
                self.tabs.select(Tab::Explorer);
            }
        }
    }

    pub fn explorer(&self) -> BlockViewModel {
        let mut view_model = BlockViewModel::default();
        if let ExplorerState::Block(b) = self.explorer.state() {
            view_model.show(b);
        }
        view_model
    }
}
