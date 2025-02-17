mod app;
mod channels;
mod explorer;
mod frontier_scan;
mod ledger_stats;
mod message_collection;
mod message_rate_calculator;
mod message_recorder;
mod navigator;
mod node_callbacks;
mod node_runner;
mod nullable_runtime;
mod rate_calculator;
mod view_models;
mod views;

use eframe::egui;
use views::MainView;

fn main() -> eframe::Result {
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([1024.0, 768.0]),
        ..Default::default()
    };
    eframe::run_native(
        "RsNano Insight",
        options,
        Box::new(|_| Ok(Box::new(MainView::new()))),
    )
}
