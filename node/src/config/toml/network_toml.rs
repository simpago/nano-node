use serde::{Deserialize, Serialize};

use crate::config::NodeConfig;

#[derive(Deserialize, Serialize)]
pub struct NetworkToml {
    pub peer_reachout: Option<usize>,
    pub cached_peer_reachout: Option<usize>,
    pub max_peers_per_ip: Option<u16>,
    pub max_peers_per_subnetwork: Option<u16>,
    pub duplicate_filter_size: Option<usize>,
    pub duplicate_filter_cutoff: Option<usize>,
    pub minimum_fanout: Option<usize>,
}

impl From<&NodeConfig> for NetworkToml {
    fn from(value: &NodeConfig) -> Self {
        Self {
            peer_reachout: Some(value.network.peer_reachout.as_millis() as usize),
            cached_peer_reachout: Some(value.network.cached_peer_reachout.as_millis() as usize),
            max_peers_per_ip: Some(value.network.max_peers_per_ip),
            max_peers_per_subnetwork: Some(value.network.max_peers_per_subnetwork),
            duplicate_filter_size: Some(value.network_duplicate_filter_size),
            duplicate_filter_cutoff: Some(value.network_duplicate_filter_cutoff as usize),
            minimum_fanout: Some(value.network.minimum_fanout),
        }
    }
}
