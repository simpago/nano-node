use super::{
    bootstrap_view::BootstrapView, queue_group_view::show_queue_group, show_peers, view_search_bar,
    view_tabs, ExplorerView, LedgerStatsView, MessageRecorderControlsView, MessageStatsView,
    MessageTabView, NodeRunnerView,
};
use crate::{
    navigator::NavItem,
    view_models::{AppViewModel, QueueGroupViewModel},
};
use eframe::egui::{
    self, global_theme_preference_switch, warn_if_debug_build, CentralPanel, TopBottomPanel,
};

pub(crate) struct AppView {
    model: AppViewModel,
}

impl AppView {
    pub(crate) fn new() -> Self {
        let model = AppViewModel::new();
        Self { model }
    }
}

impl AppView {
    fn show_controls_panel(&mut self, ctx: &egui::Context) {
        TopBottomPanel::top("controls_panel").show(ctx, |ui| {
            ui.add_space(1.0);
            ui.horizontal(|ui| {
                NodeRunnerView::new(&mut self.model.app.node_runner).show(ui);
                ui.separator();
                MessageRecorderControlsView::new(&self.model.app.msg_recorder).show(ui);
                ui.separator();
                let changed = view_search_bar(ui, &mut self.model.search_input);
                if changed {
                    self.model.search();
                }
            });
            ui.add_space(1.0);
        });
    }

    fn show_tabs(&mut self, ctx: &egui::Context) {
        TopBottomPanel::top("tabs_panel").show(ctx, |ui| {
            view_tabs(ui, &self.model.tabs(), &mut self.model.app.navigator);
        });
    }

    fn show_stats(&mut self, ctx: &egui::Context) {
        TopBottomPanel::bottom("bottom_panel").show(ctx, |ui| {
            ui.horizontal(|ui| {
                global_theme_preference_switch(ui);
                ui.separator();
                MessageStatsView::new(self.model.message_stats()).view(ui);
                ui.separator();
                LedgerStatsView::new(self.model.ledger_stats()).view(ui);
                warn_if_debug_build(ui);
            });
        });
    }
}

impl eframe::App for AppView {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.model.update();
        self.show_controls_panel(ctx);
        self.show_tabs(ctx);
        self.show_stats(ctx);

        match self.model.app.navigator.current {
            NavItem::Peers => show_peers(ctx, self.model.channels()),
            NavItem::Messages => MessageTabView::new(&mut self.model).show(ctx),
            NavItem::Queues => show_queues(ctx, self.model.queue_groups()),
            NavItem::Bootstrap => BootstrapView::new(&self.model.bootstrap).show(ctx),
            NavItem::Explorer => ExplorerView::new(&self.model.explorer()).show(ctx),
        }

        // Repaint to show the continuously increasing current block and message counters
        ctx.request_repaint();
    }
}

fn show_queues(ctx: &egui::Context, groups: Vec<QueueGroupViewModel>) {
    CentralPanel::default().show(ctx, |ui| {
        for group in groups {
            show_queue_group(ui, group);
            ui.add_space(10.0);
        }
    });
}
