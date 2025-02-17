mod app_view_model;
mod bootstrap_view_model;
mod channels_view_model;
mod explorer_view_model;
mod message_stats_view_model;
mod message_table_view_model;
mod message_view_model;
mod palette;
mod queue_group_view_model;
mod tab_bar;

pub(crate) use app_view_model::*;
pub(crate) use bootstrap_view_model::*;
pub(crate) use channels_view_model::*;
pub(crate) use explorer_view_model::*;
pub(crate) use message_stats_view_model::*;
pub(crate) use message_table_view_model::*;
pub(crate) use message_view_model::*;
use num_format::{Locale, ToFormattedString};
pub(crate) use palette::PaletteColor;
pub(crate) use queue_group_view_model::*;
pub(crate) use tab_bar::*;

pub(crate) fn formatted_number(i: impl ToFormattedString) -> String {
    i.to_formatted_string(&Locale::en)
}
