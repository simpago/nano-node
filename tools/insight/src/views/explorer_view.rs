use crate::view_models::ExplorerViewModel;
use eframe::egui::{self, CentralPanel};

pub(crate) struct ExplorerView<'a> {
    model: &'a ExplorerViewModel,
}

impl<'a> ExplorerView<'a> {
    pub(crate) fn new(model: &'a ExplorerViewModel) -> Self {
        Self { model }
    }

    pub fn show(&mut self, ctx: &egui::Context) {
        CentralPanel::default().show(ctx, |ui| {
            ui.heading("Explorer");
            ui.add_space(50.0);
            ui.label(&self.model.block)
        });
    }
}
