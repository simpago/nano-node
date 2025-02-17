use eframe::egui::{
    self, global_theme_preference_switch, warn_if_debug_build, CentralPanel, TopBottomPanel,
};

use super::{
    frontier_scan::view_frontier_scan, queue_group_view::show_queue_group, view_ledger_stats,
    view_node_runner, view_peers, view_search_bar, view_tabs, ExplorerView,
    MessageRecorderControlsView, MessageStatsView, MessageTabView,
};
use crate::{
    navigator::NavItem,
    view_models::{AppViewModel, QueueGroupViewModel},
};

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
                MessageRecorderControlsView::new(&self.model.app.msg_recorder).show(ui);
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
            NavItem::Messages => MessageTabView::new(&mut self.model).show(ctx),
            NavItem::Queues => view_queues(ctx, self.model.queue_groups()),
            NavItem::Bootstrap => view_frontier_scan(ctx, self.model.frontier_scan()),
            NavItem::Explorer => ExplorerView::new(&self.model.explorer()).show(ctx),
        }

        // Repaint to show the continuously increasing current block and message counters
        ctx.request_repaint();
    }
}

fn view_queues(ctx: &egui::Context, groups: Vec<QueueGroupViewModel>) {
    CentralPanel::default().show(ctx, |ui| {
        for group in groups {
            show_queue_group(ui, group);
            ui.add_space(10.0);
        }
    });
}
