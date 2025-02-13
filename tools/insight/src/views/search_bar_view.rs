use crate::view_models::SearchBarViewModel;
use eframe::egui::{TextEdit, Ui};

pub(crate) struct SearchBarView<'a> {
    model: &'a mut SearchBarViewModel,
}

impl<'a> SearchBarView<'a> {
    pub(crate) fn new(model: &'a mut SearchBarViewModel) -> Self {
        Self { model }
    }

    pub fn show(&mut self, ui: &mut Ui) {
        if ui
            .add(
                TextEdit::singleline(&mut self.model.input)
                    .hint_text("account / block hash ...")
                    .desired_width(450.0),
            )
            .changed()
        {
            self.model.input_changed();
        };
    }
}
