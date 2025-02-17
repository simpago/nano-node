use rsnano_core::Networks;

use crate::node_runner::{NodeRunner, NodeState};

pub(crate) struct NodeRunnerViewModel<'a> {
    pub node_runner: &'a mut NodeRunner,
}
impl<'a> NodeRunnerViewModel<'a> {
    pub(crate) fn new(node_runner: &'a mut NodeRunner) -> Self {
        Self { node_runner }
    }

    pub(crate) fn can_start_node(&self) -> bool {
        self.node_runner.state() == NodeState::Stopped
    }

    pub(crate) fn can_stop_node(&self) -> bool {
        self.node_runner.state() == NodeState::Started
    }

    pub(crate) fn start_node(&mut self) {
        self.node_runner.start_node();
    }

    pub(crate) fn stop_node(&mut self) {
        self.node_runner.stop();
    }

    pub(crate) fn status(&self) -> &'static str {
        match self.node_runner.state() {
            NodeState::Starting => "starting...",
            NodeState::Started => "running",
            NodeState::Stopping => "stopping...",
            NodeState::Stopped => "not running",
        }
    }

    pub(crate) fn network(&self) -> Networks {
        self.node_runner.network()
    }

    pub(crate) fn set_network(&mut self, network: Networks) {
        self.node_runner.set_network(network);
    }

    pub(crate) fn data_path(&self) -> &str {
        self.node_runner.data_path()
    }

    pub(crate) fn set_data_path(&mut self, path: String) {
        self.node_runner.set_data_path(path)
    }
}
