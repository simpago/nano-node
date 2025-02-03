use super::{peer_score::PeerScore, peer_score_container::PeerScoreContainer};
use crate::bootstrap::BootstrapConfig;
use rsnano_core::utils::ContainerInfo;
use rsnano_network::{Channel, ChannelId, Network, TrafficType};
use std::sync::{Arc, RwLock};

/// Container for tracking and scoring peers with respect to bootstrapping
pub(crate) struct PeerScoring {
    scoring: PeerScoreContainer,
    config: BootstrapConfig,
    network: Arc<RwLock<Network>>,
}

impl PeerScoring {
    pub fn new(config: BootstrapConfig, network: Arc<RwLock<Network>>) -> Self {
        Self {
            scoring: PeerScoreContainer::default(),
            config,
            network,
        }
    }

    #[cfg(test)]
    pub fn add_test_channel(&mut self) -> Arc<Channel> {
        self.network.write().unwrap().add_test_channel()
    }

    pub fn received_message(&mut self, channel_id: ChannelId) {
        self.scoring.modify(channel_id, |i| {
            if i.outstanding > 1 {
                i.outstanding -= 1;
                i.response_count_total += 1;
            }
        });
    }

    pub fn channel(&mut self) -> Option<Arc<Channel>> {
        self.network
            .read()
            .unwrap()
            .random_list(usize::MAX, 0)
            .iter()
            .find(|c| {
                if !c.should_drop(TrafficType::BootstrapRequests) {
                    if !Self::try_send_message(&mut self.scoring, c, &self.config) {
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            })
            .cloned()
    }

    fn try_send_message(
        scoring: &mut PeerScoreContainer,
        channel: &Arc<Channel>,
        config: &BootstrapConfig,
    ) -> bool {
        let mut result = false;
        let modified = scoring.modify(channel.channel_id(), |i| {
            if i.outstanding < config.channel_limit {
                i.outstanding += 1;
                i.request_count_total += 1;
            } else {
                result = true;
            }
        });
        if !modified {
            scoring.insert(PeerScore::new(channel));
        }
        result
    }

    pub fn len(&self) -> usize {
        self.scoring.len()
    }

    pub fn available(&self) -> usize {
        self.network
            .read()
            .unwrap()
            .list_channels(0)
            .iter()
            .filter(|c| !self.limit_exceeded(c))
            .count()
    }

    fn limit_exceeded(&self, channel: &Channel) -> bool {
        if let Some(existing) = self.scoring.get(channel.channel_id()) {
            existing.outstanding >= self.config.channel_limit
        } else {
            false
        }
    }

    pub fn timeout(&mut self) {
        self.scoring.retain(|i| i.is_alive());
        self.scoring.modify_all(|i| i.decay());
    }

    pub fn container_info(&self) -> ContainerInfo {
        [
            ("scores", self.len(), 0),
            ("available", self.available(), 0),
        ]
        .into()
    }
}
