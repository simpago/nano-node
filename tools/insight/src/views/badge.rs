use eframe::egui::{Color32, NumExt, Sense, TextStyle, Ui, Widget, WidgetText};

use super::PaletteColor;

pub(crate) struct Badge {
    text: WidgetText,
    color: PaletteColor,
}

impl Badge {
    pub(crate) fn new(text: impl Into<WidgetText>, color: PaletteColor) -> Self {
        Self {
            text: text.into(),
            color,
        }
    }

    fn colors(&self, ui: &Ui) -> (Color32, Color32) {
        if ui.visuals().dark_mode {
            self.color.as_dark_colors()
        } else {
            self.color.as_light_colors()
        }
    }
}

impl Widget for Badge {
    fn ui(self, ui: &mut Ui) -> eframe::egui::Response {
        let (text_color, background_color) = self.colors(ui);
        let button_padding = ui.spacing().button_padding;
        let total_extra = button_padding + button_padding;

        let wrap_width = ui.available_width() - total_extra.x;
        let galley = self
            .text
            .into_galley(ui, None, wrap_width, TextStyle::Button);

        let mut desired_size = total_extra + galley.size();
        desired_size.y = desired_size.y.at_least(ui.spacing().interact_size.y);
        let (rect, response) = ui.allocate_at_least(desired_size, Sense::hover());
        if ui.is_rect_visible(response.rect) {
            let text_pos = ui
                .layout()
                .align_size_within_rect(galley.size(), rect.shrink2(button_padding))
                .min;

            ui.painter().rect_filled(rect, 5.0, background_color);
            ui.painter().galley(text_pos, galley, text_color);
        }

        response
    }
}
