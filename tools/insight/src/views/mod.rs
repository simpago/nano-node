use num_format::{Locale, ToFormattedString};

mod badge;
mod channels;
mod explorer;
mod frontier_scan;
mod ledger_stats;
mod main_view;
mod message;
mod message_recorder_controls_view;
mod message_stats;
mod message_tab_view;
mod message_table;
mod node_runner;
mod palette;
mod peers_view;
mod queue_group;
mod search_bar;
mod tab_bar;

pub(crate) use channels::*;
pub(crate) use explorer::*;
pub(crate) use frontier_scan::*;
pub(crate) use ledger_stats::*;
pub(crate) use main_view::*;
pub(crate) use message::*;
pub(crate) use message_recorder_controls_view::*;
pub(crate) use message_stats::*;
pub(crate) use message_tab_view::*;
pub(crate) use message_table::*;
pub(crate) use node_runner::*;
pub(crate) use palette::PaletteColor;
pub(crate) use peers_view::*;
pub(crate) use queue_group::*;
pub(crate) use search_bar::*;
pub(crate) use tab_bar::*;

pub(crate) fn formatted_number(i: impl ToFormattedString) -> String {
    i.to_formatted_string(&Locale::en)
}
