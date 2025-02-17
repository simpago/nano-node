use crate::channels::RepState;
use eframe::egui::{self, Align, CentralPanel, Label, Layout, Sense, Ui};
use egui_extras::{Column, Size, StripBuilder, TableBuilder};

use super::{badge::Badge, ChannelsViewModel, PaletteColor};

pub(crate) fn view_peers(ctx: &egui::Context, model: ChannelsViewModel) {
    CentralPanel::default().show(ctx, |ui| {
        if model.channel_count() == 0 {
            view_no_connected_peers(ui);
        } else {
            view_peers_table(ui, model);
        }
    });
}

fn view_no_connected_peers(ui: &mut Ui) {
    StripBuilder::new(ui)
        .size(Size::remainder())
        .size(Size::exact(50.0))
        .size(Size::remainder())
        .vertical(|mut strip| {
            strip.empty();
            strip.cell(|ui| {
                ui.vertical_centered_justified(|ui| ui.heading("not connected to any peers"));
            });
            strip.empty();
        });
}

fn view_peers_table(ui: &mut Ui, mut model: ChannelsViewModel) {
    TableBuilder::new(ui)
        .striped(true)
        .resizable(false)
        .cell_layout(Layout::left_to_right(Align::Center))
        .auto_shrink(false)
        .sense(Sense::click())
        .column(Column::auto()) // channel
        .column(Column::auto()) // rep state
        .column(Column::auto()) // in/out
        .column(Column::exact(300.0)) // addr
        .column(Column::exact(80.0)) //rep weight
        .column(Column::exact(80.0))
        .column(Column::exact(80.0))
        .column(Column::exact(70.0))
        .column(Column::exact(70.0)) // maker
        .column(Column::auto())
        .column(Column::exact(80.0))
        .header(20.0, |mut header| {
            header.col(|ui| {
                ui.strong("Channel");
            });
            header.col(|ui| {
                ui.strong("Rep");
            });
            header.col(|ui| {
                ui.strong("in/out");
            });
            header.col(|ui| {
                ui.strong("Remote Addr");
            });
            header.col(|ui| {
                ui.strong("Rep Weight");
            });
            header.col(|ui| {
                ui.strong("Blocks");
            });
            header.col(|ui| {
                ui.strong("Cemented");
            });
            header.col(|ui| {
                ui.strong("Unchecked");
            });
            header.col(|ui| {
                ui.strong("Maker");
            });
            header.col(|ui| {
                ui.strong("Version");
            });
            header.col(|ui| {
                ui.strong("Bandwidth");
            });
        })
        .body(|body| {
            body.rows(20.0, model.channel_count(), |mut row| {
                let Some(row_model) = model.get_row(row.index()) else {
                    return;
                };
                if row_model.is_selected {
                    row.set_selected(true);
                }
                row.col(|ui| {
                    ui.add(Label::new(row_model.channel_id).selectable(false));
                });
                row.col(|ui| view_rep_state(ui, row_model.rep_state));
                row.col(|ui| {
                    ui.add(Label::new(row_model.direction).selectable(false));
                });
                row.col(|ui| {
                    ui.add(Label::new(row_model.remote_addr).selectable(false));
                });
                row.col(|ui| {
                    ui.add(Label::new(row_model.rep_weight).selectable(false));
                });
                row.col(|ui| {
                    ui.add(Label::new(row_model.block_count).selectable(false));
                });
                row.col(|ui| {
                    ui.add(Label::new(row_model.cemented_count).selectable(false));
                });
                row.col(|ui| {
                    ui.add(Label::new(row_model.unchecked_count).selectable(false));
                });
                row.col(|ui| {
                    ui.add(Label::new(row_model.maker).selectable(false));
                });
                row.col(|ui| {
                    ui.add(Label::new(row_model.version).selectable(false));
                });
                row.col(|ui| {
                    ui.add(Label::new(row_model.bandwidth_cap).selectable(false));
                });
                if row.response().clicked() {
                    model.select(row.index());
                }
            })
        });
}

pub(crate) fn view_rep_state(ui: &mut Ui, rep_state: RepState) {
    match rep_state {
        RepState::PrincipalRep => {
            ui.add(Badge::new("PR", PaletteColor::Purple1));
        }
        RepState::Rep => {
            ui.add(Badge::new("R", PaletteColor::Neutral2));
        }
        RepState::NoRep => {}
    }
}
