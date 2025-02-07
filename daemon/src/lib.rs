use rsnano_core::utils::get_cpu_count;
use rsnano_node::{
    config::{DaemonConfig, Networks, NodeFlags},
    Node, NodeBuilder, NodeCallbacks,
};
use rsnano_rpc_server::{run_rpc_server, RpcServerConfig};
use rsnano_websocket_server::{create_websocket_server, WebsocketListenerExt};
use std::{future::Future, path::PathBuf, sync::Arc};
use tokio::{net::TcpListener, sync::oneshot};

pub struct DaemonBuilder {
    network: Networks,
    node_builder: NodeBuilder,
    node_started: Option<Box<dyn FnMut(Arc<Node>) + Send>>,
}

impl DaemonBuilder {
    pub fn new(network: Networks) -> Self {
        Self {
            network,
            node_builder: NodeBuilder::new(network),
            node_started: None,
        }
    }

    pub fn data_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.node_builder = self.node_builder.data_path(path);
        self
    }

    pub fn flags(mut self, flags: NodeFlags) -> Self {
        self.node_builder = self.node_builder.flags(flags);
        self
    }

    pub fn callbacks(mut self, callbacks: NodeCallbacks) -> Self {
        self.node_builder = self.node_builder.callbacks(callbacks);
        self
    }

    pub fn on_node_started(mut self, callback: impl FnMut(Arc<Node>) + Send + 'static) -> Self {
        self.node_started = Some(Box::new(callback));
        self
    }

    pub fn run<F>(self, shutdown: F) -> anyhow::Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let data_path = self.node_builder.get_data_path()?;
        let parallelism = get_cpu_count();
        let daemon_config =
            DaemonConfig::load_from_data_path(self.network, parallelism, &data_path)?;
        let rpc_config =
            RpcServerConfig::load_from_data_path(self.network, parallelism, &data_path)?;
        let mut node = self.node_builder.finish()?;

        let websocket_server = if daemon_config.node.websocket_config.enabled {
            Some(
                create_websocket_server(daemon_config.node.websocket_config.clone(), &node)
                    .unwrap(),
            )
        } else {
            None
        };

        if let Some(ref websocket) = websocket_server {
            websocket.start();
        }

        node.start();
        let mut node = Arc::new(node);

        if let Some(mut started_callback) = self.node_started {
            started_callback(node.clone());
        }
        let (tx_stop, rx_stop) = oneshot::channel();
        let wait_for_shutdown = async move {
            tokio::select! {
                _ = rx_stop =>{}
                _ = shutdown => {}
            }
        };

        node.runtime.block_on(run_rpc(
            daemon_config,
            rpc_config,
            node.clone(),
            tx_stop,
            wait_for_shutdown,
        ))?;

        if let Some(ref websocket) = websocket_server {
            websocket.stop();
        }

        let node = Arc::get_mut(&mut node).expect("No exclusive access to node!");
        node.stop();
        Ok(())
    }
}

async fn run_rpc(
    daemon_config: DaemonConfig,
    rpc_config: RpcServerConfig,
    node: Arc<Node>,
    tx_stop: oneshot::Sender<()>,
    wait_for_shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    if daemon_config.rpc_enable {
        let socket_addr = rpc_config.listening_addr()?;
        let listener = TcpListener::bind(socket_addr).await?;
        run_rpc_server(
            node.clone(),
            listener,
            rpc_config.enable_control,
            tx_stop,
            wait_for_shutdown,
        )
        .await?;
    } else {
        wait_for_shutdown.await;
    };
    Ok(())
}
