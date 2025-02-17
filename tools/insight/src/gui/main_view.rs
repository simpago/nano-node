use eframe::egui::{
    self, global_theme_preference_switch, warn_if_debug_build, CentralPanel, TopBottomPanel,
};

use super::{
    view_frontier_scan, view_ledger_stats, view_message_recorder_controls, view_message_tab,
    view_node_runner, view_peers, view_queue_group, view_search_bar, view_tabs, BlockViewModel,
    ChannelsViewModel, ExplorerView, FrontierScanViewModel, MessageStatsView,
    MessageStatsViewModel, MessageTableViewModel, QueueGroupViewModel, TabViewModel,
};
use crate::{app::InsightApp, explorer::ExplorerState, gui::QueueViewModel, navigator::NavItem};

pub(crate) struct MainView {
    model: AppViewModel,
}

impl MainView {
    pub(crate) fn new() -> Self {
        let model = AppViewModel::new();
        Self { model }
    }
}

impl MainView {
    fn view_controls_panel(&mut self, ctx: &egui::Context) {
        TopBottomPanel::top("controls_panel").show(ctx, |ui| {
            ui.add_space(1.0);
            ui.horizontal(|ui| {
                view_node_runner(ui, &mut self.model.app.node_runner);
                ui.separator();
                view_message_recorder_controls(ui, &self.model.app.msg_recorder);
                ui.separator();
                view_search_bar(ui, &mut self.model.search_input, &mut self.model.app);
            });
            ui.add_space(1.0);
        });
    }

    fn view_tabs(&mut self, ctx: &egui::Context) {
        TopBottomPanel::top("tabs_panel").show(ctx, |ui| {
            view_tabs(ui, &self.model.tabs(), &mut self.model.app.navigator);
        });
    }

    fn view_stats(&mut self, ctx: &egui::Context) {
        TopBottomPanel::bottom("bottom_panel").show(ctx, |ui| {
            ui.horizontal(|ui| {
                global_theme_preference_switch(ui);
                ui.separator();
                MessageStatsView::new(self.model.message_stats()).view(ui);
                ui.separator();
                view_ledger_stats(ui, &self.model.app.ledger_stats);
                warn_if_debug_build(ui);
            });
        });
    }
}

impl eframe::App for MainView {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.model.update();
        self.view_controls_panel(ctx);
        self.view_tabs(ctx);
        self.view_stats(ctx);

        match self.model.app.navigator.current {
            NavItem::Peers => view_peers(ctx, self.model.channels()),
            NavItem::Messages => view_message_tab(ctx, &mut self.model),
            NavItem::Queues => view_queues(ctx, self.model.queue_groups()),
            NavItem::Bootstrap => {
                view_frontier_scan(ctx, self.model.frontier_scan(), &mut self.model.app)
            }
            NavItem::Explorer => ExplorerView::new(&self.model.explorer()).show(ctx),
        }

        // Repaint to show the continuously increasing current block and message counters
        ctx.request_repaint();
    }
}

fn view_queues(ctx: &egui::Context, groups: Vec<QueueGroupViewModel>) {
    CentralPanel::default().show(ctx, |ui| {
        for group in groups {
            view_queue_group(ui, group);
            ui.add_space(10.0);
        }
    });
}

pub(crate) struct AppViewModel {
    pub app: InsightApp,
    pub message_table: MessageTableViewModel,
    pub search_input: String,
}

impl AppViewModel {
    pub(crate) fn new() -> Self {
        let app = InsightApp::new();
        let message_table = MessageTableViewModel::new(app.messages.clone());

        Self {
            app,
            message_table,
            search_input: String::new(),
        }
    }

    pub(crate) fn update(&mut self) {
        if !self.app.update() {
            return;
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

    pub fn frontier_scan(&self) -> FrontierScanViewModel {
        FrontierScanViewModel::new(&self.app.frontier_scan)
    }
}
