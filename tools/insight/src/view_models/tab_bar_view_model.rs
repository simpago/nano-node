#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Tab {
    Peers,
    Messages,
    Queues,
    Bootstrap,
}

impl Tab {
    pub fn name(&self) -> &'static str {
        match self {
            Tab::Peers => "Peers",
            Tab::Messages => "Messages",
            Tab::Queues => "Queues",
            Tab::Bootstrap => "Frontier Scan",
        }
    }
}

pub static TAB_ORDER: [Tab; 4] = [Tab::Peers, Tab::Messages, Tab::Queues, Tab::Bootstrap];

pub(crate) struct TabBarViewModel {
    pub selected: Tab,
    pub tabs: Vec<TabViewModel>,
}

impl TabBarViewModel {
    pub(crate) fn new() -> Self {
        let mut tabs = create_tab_view_models();
        tabs[0].selected = true;
        let selected = tabs[0].value;
        Self { tabs, selected }
    }

    pub fn select(&mut self, tab: Tab) {
        for t in &mut self.tabs {
            t.selected = t.value == tab;
        }
        self.selected = tab;
    }

    pub fn selected_tab(&self) -> Tab {
        self.selected
    }
}

fn create_tab_view_models() -> Vec<TabViewModel> {
    TAB_ORDER.iter().map(|t| TabViewModel::from(*t)).collect()
}

pub(crate) struct TabViewModel {
    pub selected: bool,
    pub label: &'static str,
    pub value: Tab,
}

impl From<Tab> for TabViewModel {
    fn from(value: Tab) -> Self {
        Self {
            selected: false,
            label: value.name(),
            value,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tab_names() {
        let model = TabBarViewModel::new();
        assert_eq!(model.tabs.len(), 4);
        assert_eq!(model.tabs[0].label, "Peers");
        assert_eq!(model.tabs[0].selected, true);
        assert_eq!(model.tabs[1].label, "Messages");
        assert_eq!(model.tabs[1].selected, false);
        assert_eq!(model.tabs[2].label, "Queues");
    }
}
