use crate::view_models::FrontierScanViewModel;
use eframe::egui::{self, CentralPanel, ProgressBar, ScrollArea};
use egui_extras::{Size, StripBuilder};

pub(crate) struct BootstrapView {
    model: FrontierScanViewModel,
}

impl BootstrapView {
    pub(crate) fn new(model: FrontierScanViewModel) -> Self {
        Self { model }
    }

    pub fn show(self, ctx: &egui::Context) {
        CentralPanel::default().show(ctx, |ui| {
            ScrollArea::both().auto_shrink(false).show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.heading(self.model.frontiers_rate);
                    ui.add_space(100.0);
                    ui.heading(self.model.outdated_rate);
                    ui.add_space(50.0);
                    ui.label(self.model.frontiers_total);
                    ui.add_space(50.0);
                    ui.label(self.model.outdated_total);
                });

                for heads in self.model.frontier_heads.chunks(4) {
                    ui.horizontal(|ui| {
                        for head in heads {
                            StripBuilder::new(ui)
                                .size(Size::exact(50.0))
                                .size(Size::exact(120.0))
                                .size(Size::exact(100.0))
                                .horizontal(|mut strip| {
                                    strip.cell(|ui| {
                                        ui.label(&head.start);
                                    });
                                    strip.cell(|ui| {
                                        ui.add(
                                            ProgressBar::new(head.done_normalized)
                                                .text(&head.current),
                                        );
                                    });
                                    strip.cell(|ui| {
                                        ui.label(&head.end);
                                    });
                                });
                        }
                    });
                }
                ui.add_space(20.0);

                ui.heading("Outdated accounts found:");
                for account in self.model.outdated_accounts {
                    ui.label(account);
                }
            });
        });
    }
}
