use super::{
    BlockViewModel, BootstrapViewModel, ChannelsViewModel, MessageStatsViewModel,
    MessageTableViewModel, QueueGroupViewModel, TabViewModel,
};
use crate::{app::InsightApp, explorer::ExplorerState, view_models::QueueViewModel};

pub(crate) struct AppViewModel {
    pub app: InsightApp,
    pub message_table: MessageTableViewModel,
    pub bootstrap: BootstrapViewModel,
    pub search_input: String,
}

impl AppViewModel {
    pub(crate) fn new() -> Self {
        let app = InsightApp::new();
        let message_table = MessageTableViewModel::new(app.messages.clone());

        Self {
            app,
            message_table,
            bootstrap: Default::default(),
            search_input: String::new(),
        }
    }

    pub(crate) fn update(&mut self) {
        if !self.app.update() {
            return;
        }

        let now = self.app.clock.now();

        if let Some(node) = self.app.node_runner.node() {
            self.bootstrap.update(&node.bootstrapper, now);
        }

        self.message_table.update_message_counts();
    }

    pub(crate) fn tabs(&self) -> Vec<TabViewModel> {
        self.app
            .navigator
            .all
            .iter()
            .map(|i| TabViewModel {
                selected: *i == self.app.navigator.current,
                label: i.name(),
                value: *i,
            })
            .collect()
    }

    pub(crate) fn message_stats(&self) -> MessageStatsViewModel {
        MessageStatsViewModel::new(&self.app.msg_recorder)
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
                        self.app.aec_info.priority,
                        self.app.aec_info.max_queue,
                    ),
                    QueueViewModel::new(
                        "Hinted",
                        self.app.aec_info.hinted,
                        self.app.aec_info.max_queue,
                    ),
                    QueueViewModel::new(
                        "Optimistic",
                        self.app.aec_info.optimistic,
                        self.app.aec_info.max_queue,
                    ),
                    QueueViewModel::new(
                        "Total",
                        self.app.aec_info.total,
                        self.app.aec_info.max_queue,
                    ),
                ],
            },
            QueueGroupViewModel::for_fair_queue("Block Processor", &self.app.block_processor_info),
            QueueGroupViewModel::for_fair_queue("Vote Processor", &self.app.vote_processor_info),
            QueueGroupViewModel {
                heading: "Miscellaneous".to_string(),
                queues: vec![QueueViewModel::new(
                    "Confirming",
                    self.app.confirming_set.size,
                    self.app.confirming_set.max_size,
                )],
            },
        ]
    }

    pub fn explorer(&self) -> BlockViewModel {
        let mut view_model = BlockViewModel::default();
        if let ExplorerState::Block(b) = self.app.explorer.state() {
            view_model.show(b);
        }
        view_model
    }
}
