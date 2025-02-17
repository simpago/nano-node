use eframe::egui::{Align, Label, Layout, Sense, Ui};
use egui_extras::{Column, TableBuilder};
use rsnano_network::ChannelDirection;

use crate::{
    channels::{Channels, RepState},
    view_models::formatted_number,
};

use super::view_rep_state;

pub(crate) struct ChannelsView<'a> {
    model: ChannelsViewModel<'a>,
}

impl<'a> ChannelsView<'a> {
    pub(crate) fn new(model: ChannelsViewModel<'a>) -> Self {
        Self { model }
    }

    pub(crate) fn view(&mut self, ui: &mut Ui) {
        ui.add_space(5.0);
        ui.heading(self.model.heading());
        TableBuilder::new(ui)
            .striped(true)
            .resizable(false)
            .auto_shrink(false)
            .cell_layout(Layout::left_to_right(Align::Center))
            .sense(Sense::click())
            .column(Column::auto())
            .column(Column::auto()) // rep state
            .column(Column::remainder())
            .header(20.0, |mut header| {
                header.col(|ui| {
                    ui.strong("Channel");
                });
                header.col(|ui| {
                    ui.strong("Rep");
                });
                header.col(|ui| {
                    ui.strong("Remote Addr");
                });
            })
            .body(|body| {
                body.rows(20.0, self.model.channel_count(), |mut row| {
                    let Some(row_model) = self.model.get_row(row.index()) else {
                        return;
                    };
                    if row_model.is_selected {
                        row.set_selected(true);
                    }
                    row.col(|ui| {
                        ui.add(Label::new(row_model.channel_id).selectable(false));
                    });
                    row.col(|ui| {
                        view_rep_state(ui, row_model.rep_state);
                    });
                    row.col(|ui| {
                        ui.add(Label::new(row_model.remote_addr).selectable(false));
                    });
                    if row.response().clicked() {
                        self.model.select(row.index());
                    }
                })
            });
    }
}

pub(crate) struct ChannelsViewModel<'a>(&'a mut Channels);

impl<'a> ChannelsViewModel<'a> {
    pub(crate) fn new(channels: &'a mut Channels) -> Self {
        Self(channels)
    }

    pub(crate) fn get_row(&self, index: usize) -> Option<ChannelViewModel> {
        let channel = self.0.get(index)?;
        let mut result = ChannelViewModel {
            channel_id: channel.channel_id.to_string(),
            remote_addr: channel.remote_addr.to_string(),
            direction: match channel.direction {
                ChannelDirection::Inbound => "in",
                ChannelDirection::Outbound => "out",
            },
            is_selected: self.0.selected_index() == Some(index),
            block_count: String::new(),
            cemented_count: String::new(),
            unchecked_count: String::new(),
            maker: "",
            version: String::new(),
            bandwidth_cap: String::new(),
            rep_weight: channel.rep_weight.format_balance(0),
            rep_state: channel.rep_state,
        };

        if let Some(telemetry) = &channel.telemetry {
            result.block_count = formatted_number(telemetry.block_count);
            result.cemented_count = formatted_number(telemetry.cemented_count);
            result.unchecked_count = formatted_number(telemetry.unchecked_count);
            result.maker = match telemetry.maker {
                0 | 1 => "NF",
                3 => "RsNano",
                _ => "unknown",
            };
            result.version = format!(
                "v{}.{}.{}",
                telemetry.major_version, telemetry.minor_version, telemetry.patch_version
            );
            result.bandwidth_cap = format!("{}mb/s", telemetry.bandwidth_cap / (1024 * 1024))
        }

        Some(result)
    }

    pub(crate) fn channel_count(&self) -> usize {
        self.0.len()
    }

    pub(crate) fn select(&mut self, index: usize) {
        self.0.select_index(index);
    }

    pub(crate) fn heading(&self) -> String {
        format!("Channels ({})", self.0.len())
    }
}

pub(crate) struct ChannelViewModel {
    pub channel_id: String,
    pub remote_addr: String,
    pub direction: &'static str,
    pub is_selected: bool,
    pub block_count: String,
    pub cemented_count: String,
    pub unchecked_count: String,
    pub maker: &'static str,
    pub version: String,
    pub bandwidth_cap: String,
    pub rep_weight: String,
    pub rep_state: RepState,
}
