use crate::{
    config::{get_node_toml_config_path, DaemonConfig, DaemonToml, NodeConfig, NodeFlags},
    consensus::{ElectionEndCallback, ElectionStatus, VoteProcessedCallback2},
    transport::MessageCallback,
    working_path_for, NetworkParams, Node, NodeArgs,
};
use rsnano_core::{
    utils::get_cpu_count, work::WorkPoolImpl, Account, Amount, Networks, SavedBlock, Vote,
    VoteCode, VoteSource, VoteWithWeightInfo,
};
use rsnano_messages::Message;
use rsnano_network::{Channel, ChannelId};
use std::{path::PathBuf, sync::Arc, time::Duration};

#[derive(Default)]
pub struct NodeCallbacks {
    pub on_election_end: Option<ElectionEndCallback>,
    pub on_vote: Option<VoteProcessedCallback2>,
    pub on_publish: Option<MessageCallback>,
    pub on_inbound: Option<MessageCallback>,
    pub on_inbound_dropped: Option<MessageCallback>,
}

impl NodeCallbacks {
    pub fn builder() -> NodeCallbacksBuilder {
        NodeCallbacksBuilder::new()
    }
}

pub struct NodeCallbacksBuilder(NodeCallbacks);

impl NodeCallbacksBuilder {
    fn new() -> Self {
        Self(NodeCallbacks::default())
    }

    pub fn on_election_end(
        mut self,
        callback: impl Fn(&ElectionStatus, &Vec<VoteWithWeightInfo>, Account, &SavedBlock, Amount, bool, bool)
            + Send
            + Sync
            + 'static,
    ) -> Self {
        self.0.on_election_end = Some(Box::new(callback));
        self
    }

    pub fn on_vote(
        mut self,
        callback: impl Fn(&Arc<Vote>, Option<&Arc<Channel>>, VoteSource, VoteCode)
            + Send
            + Sync
            + 'static,
    ) -> Self {
        self.0.on_vote = Some(Box::new(callback));
        self
    }

    pub fn on_publish(
        mut self,
        callback: impl Fn(ChannelId, &Message) + Send + Sync + 'static,
    ) -> Self {
        self.0.on_publish = Some(Arc::new(callback));
        self
    }

    pub fn on_inbound(
        mut self,
        callback: impl Fn(ChannelId, &Message) + Send + Sync + 'static,
    ) -> Self {
        self.0.on_inbound = Some(Arc::new(callback));
        self
    }

    pub fn on_inbound_dropped(
        mut self,
        callback: impl Fn(ChannelId, &Message) + Send + Sync + 'static,
    ) -> Self {
        self.0.on_inbound_dropped = Some(Arc::new(callback));
        self
    }

    pub fn finish(self) -> NodeCallbacks {
        self.0
    }
}

pub struct NodeBuilder {
    network: Networks,
    runtime: Option<tokio::runtime::Handle>,
    data_path: Option<PathBuf>,
    config: Option<NodeConfig>,
    network_params: Option<NetworkParams>,
    flags: Option<NodeFlags>,
    work: Option<Arc<WorkPoolImpl>>,
    callbacks: Option<NodeCallbacks>,
}

impl NodeBuilder {
    pub fn new(network: Networks) -> Self {
        Self {
            network,
            runtime: None,
            data_path: None,
            config: None,
            network_params: None,
            flags: None,
            work: None,
            callbacks: None,
        }
    }

    pub fn runtime(mut self, runtime: tokio::runtime::Handle) -> Self {
        self.runtime = Some(runtime);
        self
    }

    pub fn data_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.data_path = Some(path.into());
        self
    }

    pub fn config(mut self, config: NodeConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub fn network_params(mut self, network_params: NetworkParams) -> Self {
        self.network_params = Some(network_params);
        self
    }

    pub fn flags(mut self, flags: NodeFlags) -> Self {
        self.flags = Some(flags);
        self
    }

    pub fn work(mut self, work: Arc<WorkPoolImpl>) -> Self {
        self.work = Some(work);
        self
    }

    pub fn callbacks(mut self, callbacks: NodeCallbacks) -> Self {
        self.callbacks = Some(callbacks);
        self
    }

    pub fn get_data_path(&self) -> anyhow::Result<PathBuf> {
        match &self.data_path {
            Some(path) => Ok(path.clone()),
            None => working_path_for(self.network).ok_or_else(|| anyhow!("working path not found")),
        }
    }

    pub fn finish(self) -> anyhow::Result<Node> {
        let data_path = self.get_data_path()?;
        let runtime = self
            .runtime
            .unwrap_or_else(|| tokio::runtime::Handle::current());

        let network_params = self
            .network_params
            .unwrap_or_else(|| NetworkParams::new(self.network));

        let config = match self.config {
            Some(c) => c,
            None => {
                let cpu_count = get_cpu_count();
                let mut daemon_config = DaemonConfig::new(&network_params, cpu_count);
                let config_path = get_node_toml_config_path(&data_path);
                if config_path.exists() {
                    let toml_str = std::fs::read_to_string(config_path)?;
                    let daemon_toml: DaemonToml = toml::de::from_str(&toml_str)?;
                    daemon_config.merge_toml(&daemon_toml);
                }
                daemon_config.node
            }
        };

        let flags = self.flags.unwrap_or_default();
        let work = self.work.unwrap_or_else(|| {
            Arc::new(WorkPoolImpl::new(
                network_params.work.clone(),
                config.work_threads as usize,
                Duration::from_nanos(config.pow_sleep_interval_ns as u64),
            ))
        });

        let callbacks = self.callbacks.unwrap_or_default();

        let args = NodeArgs {
            runtime,
            data_path,
            config,
            network_params,
            flags,
            work,
            callbacks,
        };

        Ok(Node::new_with_args(args))
    }
}
