use super::{channels::ChannelsView, AppViewModel, MessageTableView, MessageView};
use eframe::egui::{self, CentralPanel, SidePanel};

pub(crate) fn view_message_tab(ctx: &egui::Context, model: &mut AppViewModel) {
    MessageTabView::new(model).show(ctx);
}

struct MessageTabView<'a> {
    model: &'a mut AppViewModel,
}

impl<'a> MessageTabView<'a> {
    fn new(model: &'a mut AppViewModel) -> Self {
        Self { model }
    }

    fn show(&mut self, ctx: &egui::Context) {
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
