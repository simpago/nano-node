use eframe::egui::Ui;

use crate::{navigator::Navigator, view_models::TabViewModel};

pub(crate) fn view_tabs(ui: &mut Ui, tabs: &[TabViewModel], navigator: &mut Navigator) {
    ui.horizontal(|ui| {
        for tab in tabs {
            if ui.selectable_label(tab.selected, tab.label).clicked() {
                navigator.current = tab.value;
            }
        }
    });
}
