use eframe::egui::Ui;

use crate::navigator::{NavItem, Navigator};

pub(crate) fn view_tabs(ui: &mut Ui, tabs: &[TabViewModel], navigator: &mut Navigator) {
    ui.horizontal(|ui| {
        for tab in tabs {
            if ui.selectable_label(tab.selected, tab.label).clicked() {
                navigator.current = tab.value;
            }
        }
    });
}

pub(crate) struct TabViewModel {
    pub selected: bool,
    pub label: &'static str,
    pub value: NavItem,
}
