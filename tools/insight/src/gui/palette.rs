use eframe::egui::Color32;

#[allow(dead_code)]
pub(crate) enum PaletteColor {
    Red1,
    Red2,

    Purple1,
    Purple2,

    Blue1,
    Blue2,

    Orange1,
    Orange2,

    Neutral1,
    Neutral2,
    Neutral3,
    Neutral4,
}

/// Colors picked from https://atlassian.design/foundations/color-new/color-palette-new
impl PaletteColor {
    pub fn as_light_colors(&self) -> (Color32, Color32) {
        match self {
            PaletteColor::Red1 => (Color32::BLACK, Color32::from_rgb(253, 152, 145)), // Red300
            PaletteColor::Red2 => (Color32::BLACK, Color32::from_rgb(255, 213, 210)), // Red200
            PaletteColor::Purple1 => (Color32::BLACK, Color32::from_rgb(184, 172, 246)), // Purple300
            PaletteColor::Purple2 => (Color32::BLACK, Color32::from_rgb(223, 216, 253)), // Purple200
            PaletteColor::Blue1 => (Color32::BLACK, Color32::from_rgb(133, 184, 255)),   // Blue300
            PaletteColor::Blue2 => (Color32::BLACK, Color32::from_rgb(204, 224, 255)),   // Blue200
            PaletteColor::Orange1 => (Color32::BLACK, Color32::from_rgb(254, 163, 98)), // Orange400
            PaletteColor::Orange2 => (Color32::BLACK, Color32::from_rgb(254, 193, 149)), // Orange300
            PaletteColor::Neutral1 => (Color32::WHITE, Color32::from_rgb(23, 43, 77)), // Neutral1000
            PaletteColor::Neutral2 => (Color32::WHITE, Color32::from_rgb(98, 111, 134)), // Neutral700
            PaletteColor::Neutral3 => (Color32::BLACK, Color32::from_rgb(220, 223, 228)), // Neutral300
            PaletteColor::Neutral4 => (Color32::BLACK, Color32::from_rgb(247, 248, 249)), // Neutral100
        }
    }

    pub fn as_dark_colors(&self) -> (Color32, Color32) {
        match self {
            PaletteColor::Red1 => (Color32::WHITE, Color32::from_rgb(174, 46, 36)), // Red800
            PaletteColor::Red2 => (Color32::WHITE, Color32::from_rgb(93, 31, 26)),  // Red900
            PaletteColor::Purple1 => (Color32::WHITE, Color32::from_rgb(94, 77, 178)), // Purple800
            PaletteColor::Purple2 => (Color32::WHITE, Color32::from_rgb(53, 44, 99)), // Purple900
            PaletteColor::Blue1 => (Color32::WHITE, Color32::from_rgb(0, 85, 204)), // Blue800
            PaletteColor::Blue2 => (Color32::WHITE, Color32::from_rgb(9, 50, 108)), // Blue900
            PaletteColor::Orange1 => (Color32::WHITE, Color32::from_rgb(165, 72, 0)), // Orange800
            PaletteColor::Orange2 => (Color32::WHITE, Color32::from_rgb(112, 46, 0)), // Orange900
            PaletteColor::Neutral1 => (Color32::BLACK, Color32::from_rgb(222, 228, 234)), // DarkNeutral1100
            PaletteColor::Neutral2 => (Color32::BLACK, Color32::from_rgb(140, 155, 171)), // DarkNeutral700
            PaletteColor::Neutral3 => (Color32::WHITE, Color32::from_rgb(69, 79, 89)), // DarkNeutral400
            PaletteColor::Neutral4 => (Color32::WHITE, Color32::from_rgb(29, 33, 37)), // DarkNeutral100
        }
    }
}
