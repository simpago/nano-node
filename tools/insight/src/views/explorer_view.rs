use crate::view_models::BlockViewModel;
use eframe::egui::{self, CentralPanel, Grid};

pub(crate) struct ExplorerView<'a> {
    model: &'a BlockViewModel,
}

impl<'a> ExplorerView<'a> {
    pub(crate) fn new(model: &'a BlockViewModel) -> Self {
        Self { model }
    }

    pub fn show(&mut self, ctx: &egui::Context) {
        CentralPanel::default().show(ctx, |ui| {
            ui.heading(format!("Block {}", self.model.hash));
            ui.add_space(20.0);
            Grid::new("block_grid").num_columns(2).show(ui, |ui| {
                ui.label("Raw data: ");
                ui.label(&self.model.block);
                ui.end_row();

                ui.label("Subtype: ");
                ui.label(self.model.subtype);
                ui.end_row();

                ui.label("Amount: ");
                ui.label(&self.model.amount);
                ui.end_row();

                ui.label("Balance: ");
                ui.label(&self.model.balance);
                ui.end_row();

                ui.label("Height: ");
                ui.label(&self.model.height);
                ui.end_row();

                ui.label("Successor: ");
                ui.label(&self.model.successor);
                ui.end_row();

                ui.label("Timestamp: ");
                ui.label(&self.model.timestamp);
                ui.end_row();

                ui.label("confirmed: ");
                ui.label(&self.model.confirmed);
                ui.end_row();
            });
        });
    }
}
