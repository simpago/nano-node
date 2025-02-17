use std::sync::atomic::{AtomicI64, Ordering};

use eframe::egui::Ui;
use egui_extras::{Size, StripBuilder};
use num_format::{Locale, ToFormattedString};

use crate::message_recorder::MessageRecorder;

pub(crate) struct MessageStatsView<'a>(MessageStatsViewModel<'a>);

impl<'a> MessageStatsView<'a> {
    pub fn new(model: MessageStatsViewModel<'a>) -> Self {
        Self(model)
    }

    pub fn view(&self, ui: &mut Ui) {
        ui.label("Messages");
        ui.label("out/s:");
        StripBuilder::new(ui)
            .size(Size::exact(35.0))
            .horizontal(|mut strip| {
                strip.cell(|ui| {
                    ui.label(self.0.send_rate());
                })
            });

        ui.label("in/s:");
        StripBuilder::new(ui)
            .size(Size::exact(35.0))
            .horizontal(|mut strip| {
                strip.cell(|ui| {
                    ui.label(self.0.receive_rate());
                })
            });
    }
}

pub(crate) struct MessageStatsViewModel<'a> {
    msg_recorder: &'a MessageRecorder,
}

impl<'a> MessageStatsViewModel<'a> {
    pub fn new(msg_recorder: &'a MessageRecorder) -> Self {
        Self { msg_recorder }
    }

    pub(crate) fn send_rate(&self) -> String {
        Self::to_string(&self.msg_recorder.rates.send_rate)
    }

    pub(crate) fn receive_rate(&self) -> String {
        Self::to_string(&self.msg_recorder.rates.receive_rate)
    }

    fn to_string(value: &AtomicI64) -> String {
        value
            .load(Ordering::SeqCst)
            .to_formatted_string(&Locale::en)
    }
}
