use num::FromPrimitive;
use num_derive::FromPrimitive;
use rsnano_core::Networks;
use rsnano_daemon::DaemonBuilder;
use rsnano_node::{working_path_for, Node, NodeCallbacks};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc, Mutex,
    },
};
use tracing::error;

#[derive(FromPrimitive, PartialEq, Eq, Debug)]
pub enum NodeState {
    Starting,
    Started,
    Stopping,
    Stopped,
}

pub(crate) struct NodeRunner {
    data_path: String,
    network: Networks,
    node: Arc<Mutex<Option<Arc<Node>>>>,
    state: Arc<AtomicU8>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
}

impl NodeRunner {
    pub(crate) fn new() -> Self {
        let mut runner = Self {
            network: Networks::Invalid,
            data_path: String::new(),
            node: Arc::new(Mutex::new(None)),
            state: Arc::new(AtomicU8::new(NodeState::Stopped as u8)),
            stop: None,
        };
        runner.set_network(Networks::NanoLiveNetwork);
        runner
    }

    pub fn new_null_with(node: Arc<Node>) -> Self {
        let runner = Self::new();
        *runner.node.lock().unwrap() = Some(node);
        runner.set_state(NodeState::Started);
        runner
    }

    pub fn start_node(&mut self, callbacks: NodeCallbacks) {
        self.set_state(NodeState::Starting);

        let (tx_stop, rx_stop) = tokio::sync::oneshot::channel::<()>();
        self.stop = Some(tx_stop);

        let node = self.node.clone();
        let state1 = self.state.clone();
        let state2 = self.state.clone();
        let data_path: PathBuf = self.data_path.clone().into();
        let network = self.network;

        std::thread::spawn(move || {
            let on_started = move |n| {
                *node.lock().unwrap() = Some(n);
                state1.store(NodeState::Started as u8, Ordering::SeqCst);
            };

            let shutdown_signal = async move {
                let _ = rx_stop.await;
            };

            if let Err(e) = DaemonBuilder::new(network)
                .data_path(data_path)
                .callbacks(callbacks)
                .on_node_started(on_started)
                .run(shutdown_signal)
            {
                error!("Error running node: {:?}", e);
            }

            state2.store(NodeState::Stopped as u8, Ordering::SeqCst);
        });
    }

    fn set_state(&self, state: NodeState) {
        self.state.store(state as u8, Ordering::SeqCst);
    }

    pub(crate) fn stop(&mut self) {
        if let Some(tx) = self.stop.take() {
            self.state
                .store(NodeState::Stopping as u8, Ordering::SeqCst);
            let _ = tx.send(());
        }
    }

    pub(crate) fn state(&self) -> NodeState {
        FromPrimitive::from_u8(self.state.load(Ordering::SeqCst)).unwrap()
    }

    pub(crate) fn node(&self) -> Option<Arc<Node>> {
        self.node.lock().unwrap().clone()
    }

    pub(crate) fn data_path(&self) -> &str {
        &self.data_path
    }

    pub fn set_data_path(&mut self, path: String) {
        self.data_path = path;
    }

    pub fn network(&self) -> Networks {
        self.network
    }

    pub(crate) fn set_network(&mut self, network: Networks) {
        self.network = network;
        self.data_path = working_path_for(network)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();
    }
}

impl Drop for NodeRunner {
    fn drop(&mut self) {
        if let Some(tx_stop) = self.stop.take() {
            let _ = tx_stop.send(());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn can_be_nulled() {
        let node = Arc::new(Node::new_null());
        let runner = NodeRunner::new_null_with(node.clone());
        assert!(Arc::ptr_eq(&node, &runner.node().unwrap()));
        assert_eq!(runner.state(), NodeState::Started);
    }
}
