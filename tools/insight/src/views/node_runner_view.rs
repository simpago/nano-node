use crate::view_models::NodeRunnerViewModel;
use eframe::egui::{Button, RadioButton, TextEdit, Ui};
use rsnano_core::Networks;

pub(crate) struct NodeRunnerView<'a> {
    model: NodeRunnerViewModel<'a>,
}

impl<'a> NodeRunnerView<'a> {
    pub(crate) fn new(model: NodeRunnerViewModel<'a>) -> Self {
        Self { model }
    }

    pub fn show(&mut self, ui: &mut Ui) {
        ui.horizontal(|ui| {
            self.network_radio_button(ui, Networks::NanoLiveNetwork);
            self.network_radio_button(ui, Networks::NanoBetaNetwork);
            self.network_radio_button(ui, Networks::NanoTestNetwork);
            let mut path = self.model.data_path().to_owned();
            let response =
                ui.add_enabled(self.model.can_start_node(), TextEdit::singleline(&mut path));
            if response.changed() {
                self.model.set_data_path(path);
            }
            self.start_node_button(ui);
            self.stop_button(ui);
            ui.label(self.model.status());
        });
    }

    fn start_node_button(&mut self, ui: &mut Ui) {
        if ui
            .add_enabled(self.model.can_start_node(), Button::new("Start node"))
            .clicked()
        {
            self.model.start_node();
        }
    }

    fn stop_button(&mut self, ui: &mut Ui) {
        if ui
            .add_enabled(self.model.can_stop_node(), Button::new("Stop node"))
            .clicked()
        {
            self.model.stop_node();
        }
    }

    fn network_radio_button(&mut self, ui: &mut Ui, network: Networks) {
        if ui
            .add_enabled(
                self.model.can_start_node(),
                RadioButton::new(self.model.network() == network, network.as_str()),
            )
            .clicked()
        {
            self.model.set_network(network);
        }
    }
}
