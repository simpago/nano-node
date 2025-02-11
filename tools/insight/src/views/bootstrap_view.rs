use crate::view_models::BootstrapViewModel;
use eframe::egui::{self, CentralPanel, ProgressBar, ScrollArea};
use egui_extras::{Size, StripBuilder};
use num_format::{Locale, ToFormattedString};

pub(crate) struct BootstrapView<'a> {
    model: &'a BootstrapViewModel,
}

impl<'a> BootstrapView<'a> {
    pub(crate) fn new(model: &'a BootstrapViewModel) -> Self {
        Self { model }
    }

    pub fn show(&mut self, ctx: &egui::Context) {
        CentralPanel::default().show(ctx, |ui| {
            ScrollArea::both().auto_shrink(false).show(ui, |ui| {
                ui.horizontal(|ui| {
                    ui.heading(format!("{} frontiers/s", self.model.frontiers_rate()));
                    ui.add_space(100.0);
                    ui.heading(format!("{} outdated/s", self.model.outdated_rate()));
                    ui.add_space(50.0);
                    ui.label(format!(
                        "{} frontiers total",
                        self.model.frontiers_total.to_formatted_string(&Locale::en)
                    ));
                    ui.add_space(50.0);
                    ui.label(format!(
                        "{} outdated total",
                        self.model.outdated_total.to_formatted_string(&Locale::en)
                    ));
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
                for account in &self.model.outdated_accounts {
                    ui.label(account);
                }
            });
        });
    }
}
