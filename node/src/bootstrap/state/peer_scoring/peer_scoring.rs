use super::{peer_score::PeerScore, peer_score_container::PeerScoreContainer};
use crate::bootstrap::BootstrapConfig;
use rsnano_core::utils::ContainerInfo;
use rsnano_network::{Channel, ChannelId, TrafficType};
use std::sync::Arc;

/// Container for tracking and scoring peers with respect to bootstrapping
pub(crate) struct PeerScoring {
    scoring: PeerScoreContainer,
    config: BootstrapConfig,
}

impl PeerScoring {
    pub fn new(config: BootstrapConfig) -> Self {
        Self {
            scoring: PeerScoreContainer::default(),
            config,
        }
    }

    pub fn received_message(&mut self, channel_id: ChannelId) {
        self.scoring.modify(channel_id, |i| {
            if i.outstanding > 1 {
                i.outstanding -= 1;
                i.response_count_total += 1;
            }
        });
    }

    pub fn channel(&mut self, channels: Vec<Arc<Channel>>) -> Option<Arc<Channel>> {
        channels
            .iter()
            .find(|c| {
                if !c.should_drop(TrafficType::BootstrapRequests) {
                    Self::try_send_message(&mut self.scoring, c, &self.config)
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
        let mut success = true;
        let modified = scoring.modify(channel.channel_id(), |i| {
            if i.outstanding < config.channel_limit {
                i.outstanding += 1;
                i.request_count_total += 1;
            } else {
                success = false;
            }
        });
        if !modified {
            scoring.insert(PeerScore::new(channel));
        }
        success
    }

    pub fn len(&self) -> usize {
        self.scoring.len()
    }

    pub fn timeout(&mut self) {
        self.scoring.retain(|i| i.is_alive());
        self.scoring.modify_all(|i| i.decay());
    }

    pub fn container_info(&self) -> ContainerInfo {
        [("scores", self.len(), 0)].into()
    }
}
