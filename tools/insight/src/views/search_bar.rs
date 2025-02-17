use eframe::egui::{TextEdit, Ui};

pub(crate) fn view_search_bar(ui: &mut Ui, input: &mut String) -> bool {
    ui.add(
        TextEdit::singleline(input)
            .hint_text("account / block hash ...")
            .desired_width(450.0),
    )
    .changed()
}
