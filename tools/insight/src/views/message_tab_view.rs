use super::{channels::ChannelsView, MessageTableView, MessageView};
use crate::view_models::AppViewModel;
use eframe::egui::{self, CentralPanel, SidePanel};

pub(crate) struct MessageTabView<'a> {
    model: &'a mut AppViewModel,
}

impl<'a> MessageTabView<'a> {
    pub(crate) fn new(model: &'a mut AppViewModel) -> Self {
        Self { model }
    }

    pub fn show(&mut self, ctx: &egui::Context) {
        self.show_channels(ctx);
        self.show_message_overview(ctx);
        self.show_message_details(ctx);
    }

    fn show_channels(&mut self, ctx: &egui::Context) {
        SidePanel::left("channels_panel")
            .min_width(350.0)
            .resizable(false)
            .show(ctx, |ui| {
                ChannelsView::new(self.model.channels()).view(ui);
            });
    }

    fn show_message_overview(&mut self, ctx: &egui::Context) {
        SidePanel::left("messages_panel")
            .min_width(250.0)
            .resizable(true)
            .show(ctx, |ui| {
                MessageTableView::new(&mut self.model.message_table).view(ui);
            });
    }

    fn show_message_details(&mut self, ctx: &egui::Context) {
        CentralPanel::default().show(ctx, |ui| {
            ui.heading("Message details");
            if let Some(details) = self.model.message_table.selected_message() {
                MessageView::new(&details).view(ui);
            }
        });
    }
}
