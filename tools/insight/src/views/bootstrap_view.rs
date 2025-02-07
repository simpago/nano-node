use crate::view_models::BootstrapViewModel;
use eframe::egui::{self, CentralPanel, ProgressBar, ScrollArea};
use egui_extras::{Size, StripBuilder};

pub(crate) struct BootstrapView<'a> {
    model: &'a BootstrapViewModel,
}

impl<'a> BootstrapView<'a> {
    pub(crate) fn new(model: &'a BootstrapViewModel) -> Self {
        Self { model }
    }

    pub fn show(&mut self, ctx: &egui::Context) {
        CentralPanel::default().show(ctx, |ui| {
            ScrollArea::both().show(ui, |ui| {
                ui.heading("Frontier heads");
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
            });
        });
    }
}
