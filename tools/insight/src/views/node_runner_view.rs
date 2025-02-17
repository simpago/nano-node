use crate::node_runner::NodeRunner;
use eframe::egui::{Button, RadioButton, TextEdit, Ui};
use rsnano_core::Networks;

pub(crate) struct NodeRunnerView<'a> {
    runner: &'a mut NodeRunner,
}

impl<'a> NodeRunnerView<'a> {
    pub(crate) fn new(runner: &'a mut NodeRunner) -> Self {
        Self { runner }
    }

    pub fn show(&mut self, ui: &mut Ui) {
        ui.horizontal(|ui| {
            self.network_radio_button(ui, Networks::NanoLiveNetwork);
            self.network_radio_button(ui, Networks::NanoBetaNetwork);
            self.network_radio_button(ui, Networks::NanoTestNetwork);
            ui.add_enabled(
                self.runner.can_start_node(),
                TextEdit::singleline(&mut self.runner.data_path),
            );
            self.start_node_button(ui);
            self.stop_button(ui);
            ui.label(self.runner.status());
        });
    }

    fn start_node_button(&mut self, ui: &mut Ui) {
        if ui
            .add_enabled(self.runner.can_start_node(), Button::new("Start node"))
            .clicked()
        {
            self.runner.start_node();
        }
    }

    fn stop_button(&mut self, ui: &mut Ui) {
        if ui
            .add_enabled(self.runner.can_stop_node(), Button::new("Stop node"))
            .clicked()
        {
            self.runner.stop();
        }
    }

    fn network_radio_button(&mut self, ui: &mut Ui, network: Networks) {
        if ui
            .add_enabled(
                self.runner.can_start_node(),
                RadioButton::new(self.runner.network() == network, network.as_str()),
            )
            .clicked()
        {
            self.runner.set_network(network);
        }
    }
}
