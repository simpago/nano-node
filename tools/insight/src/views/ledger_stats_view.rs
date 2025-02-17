use eframe::egui::Ui;
use egui_extras::{Size, StripBuilder};

use crate::{ledger_stats::LedgerStats, view_models::formatted_number};

pub(crate) fn view_ledger_stats(ui: &mut Ui, stats: &LedgerStats) {
    ui.label("Blocks");

    ui.label("bps:");
    StripBuilder::new(ui)
        .size(Size::exact(35.0))
        .horizontal(|mut strip| {
            strip.cell(|ui| {
                ui.label(formatted_number(stats.blocks_per_second()));
            })
        });

    ui.label("cps:");
    StripBuilder::new(ui)
        .size(Size::exact(35.0))
        .horizontal(|mut strip| {
            strip.cell(|ui| {
                ui.label(formatted_number(stats.confirmations_per_second()));
            })
        });

    ui.label("blocks:");
    ui.label(formatted_number(stats.total_blocks));
    ui.add_space(10.0);
    ui.label("cemented:");
    ui.label(formatted_number(stats.cemented_blocks));
}
