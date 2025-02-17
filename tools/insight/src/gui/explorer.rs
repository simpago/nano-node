use eframe::egui::{self, CentralPanel, Grid};
use rsnano_core::DetailedBlock;

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

#[derive(Default)]
pub(crate) struct BlockViewModel {
    pub hash: String,
    pub block: String,
    pub amount: String,
    pub confirmed: String,
    pub balance: String,
    pub height: String,
    pub timestamp: String,
    pub subtype: &'static str,
    pub successor: String,
}

impl BlockViewModel {
    pub fn show(&mut self, block: &DetailedBlock) {
        self.hash = block.block.hash().to_string();
        self.block = serde_json::to_string_pretty(&block.block.json_representation()).unwrap();
        self.balance = block.block.balance().to_string_dec();
        self.height = block.block.height().to_string();
        self.amount = block.amount.unwrap_or_default().to_string_dec();
        self.confirmed = block.confirmed.to_string();
        self.timestamp = block.block.timestamp().utc().to_string();
        self.subtype = block.block.subtype().as_str();
        self.successor = block.block.successor().unwrap_or_default().to_string();
    }
}
