use std::{fmt::Debug, hash::Hash};

use eframe::egui::{Align, Layout, ProgressBar, RichText, Ui};
use egui_extras::{Size, StripBuilder};
use num_format::{Locale, ToFormattedString};
use strum::IntoEnumIterator;

use rsnano_core::utils::{FairQueueInfo, QueueInfo};

use crate::gui::PaletteColor;

pub(crate) fn show_queue_group(ui: &mut Ui, model: QueueGroupViewModel) {
    ui.heading(model.heading);

    for queue in model.queues {
        ui.horizontal(|ui| {
            StripBuilder::new(ui)
                .size(Size::exact(100.0))
                .size(Size::exact(300.0))
                .size(Size::remainder())
                .horizontal(|mut strip| {
                    strip.cell(|ui| {
                        ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                            ui.label(queue.label);
                        });
                    });
                    strip.cell(|ui| {
                        let visuals = ui.visuals();
                        let (foreground, background) = if visuals.dark_mode {
                            queue.color.as_dark_colors()
                        } else {
                            queue.color.as_light_colors()
                        };
                        ui.add(
                            ProgressBar::new(queue.progress)
                                .text(RichText::new(queue.value).color(foreground))
                                .fill(background),
                        );
                    });
                    strip.cell(|ui| {
                        ui.label(queue.max);
                    });
                });
        });
    }
}

pub(crate) struct QueueGroupViewModel {
    pub heading: String,
    pub queues: Vec<QueueViewModel>,
}

impl QueueGroupViewModel {
    pub fn for_fair_queue<T, H>(heading: H, info: &FairQueueInfo<T>) -> Self
    where
        T: Clone + Hash + Eq + IntoEnumIterator + Debug,
        H: Into<String>,
    {
        let mut queues = Vec::new();

        for source in T::iter() {
            let info = info
                .queues
                .get(&source)
                .cloned()
                .unwrap_or_else(|| QueueInfo {
                    source,
                    size: 0,
                    max_size: 0,
                });
            let label = format!("{:?}", info.source);
            queues.push(QueueViewModel::new(label, info.size, info.max_size));
        }

        queues.push(QueueViewModel::new(
            "Total",
            info.total_size,
            info.total_max_size,
        ));

        QueueGroupViewModel {
            heading: heading.into(),
            queues,
        }
    }
}

pub(crate) struct QueueViewModel {
    pub label: String,
    pub value: String,
    pub max: String,
    pub progress: f32,
    pub color: PaletteColor,
}

impl QueueViewModel {
    pub fn new(label: impl Into<String>, value: usize, max: usize) -> Self {
        let progress = value as f32 / max as f32;
        QueueViewModel {
            label: label.into(),
            value: value.to_formatted_string(&Locale::en),
            max: max.to_formatted_string(&Locale::en),
            progress,
            color: match progress {
                0.75.. => PaletteColor::Red1,
                0.5..0.75 => PaletteColor::Orange1,
                _ => PaletteColor::Blue1,
            },
        }
    }
}
