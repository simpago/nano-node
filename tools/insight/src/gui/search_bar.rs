use eframe::egui::{TextEdit, Ui};

use crate::app::InsightApp;

pub(crate) fn view_search_bar(ui: &mut Ui, input: &mut String, app: &mut InsightApp) {
    let response = ui.add(
        TextEdit::singleline(input)
            .hint_text("account / block hash ...")
            .desired_width(450.0),
    );
    if response.changed() {
        app.search(input);
    }
}
