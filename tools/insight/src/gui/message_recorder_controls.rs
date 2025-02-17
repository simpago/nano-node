use crate::message_recorder::MessageRecorder;
use eframe::egui::Ui;

pub(crate) fn view_message_recorder_controls(ui: &mut Ui, recorder: &MessageRecorder) {
    MessageRecorderControlsView::new(recorder).view(ui);
}

struct MessageRecorderControlsView<'a> {
    recorder: &'a MessageRecorder,
}

impl<'a> MessageRecorderControlsView<'a> {
    fn new(recorder: &'a MessageRecorder) -> Self {
        Self { recorder }
    }

    fn view(&self, ui: &mut Ui) {
        self.capture_check_box(ui);
        self.clear_button(ui);
    }

    fn capture_check_box(&self, ui: &mut Ui) {
        let mut checked = self.recorder.is_recording();
        ui.checkbox(&mut checked, "capture");
        if checked {
            self.recorder.start_recording()
        } else {
            self.recorder.stop_recording()
        }
    }

    fn clear_button(&self, ui: &mut Ui) {
        if ui.button("clear").clicked() {
            self.recorder.clear();
        }
    }
}
