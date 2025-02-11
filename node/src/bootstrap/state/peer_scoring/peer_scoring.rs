use super::{peer_score::PeerScore, peer_score_container::PeerScoreContainer};
use rand::{seq::SliceRandom, thread_rng};
use rsnano_core::utils::ContainerInfo;
use rsnano_network::{Channel, ChannelId, TrafficType};
use std::sync::Arc;

/// Container for tracking and scoring peers with respect to bootstrapping
pub(crate) struct PeerScoring {
    scoring: PeerScoreContainer,
    channel_limit: usize,
}

impl PeerScoring {
    pub fn new() -> Self {
        Self {
            scoring: PeerScoreContainer::default(),
            channel_limit: 16,
        }
    }

    pub fn set_channel_limit(&mut self, limit: usize) {
        self.channel_limit = limit;
    }

    pub fn received_message(&mut self, channel_id: ChannelId) {
        self.scoring.modify(channel_id, |i| {
            if i.outstanding > 1 {
                i.outstanding -= 1;
                i.response_count_total += 1;
            }
        });
    }

    pub fn channel(&mut self, mut candidates: Vec<ChannelId>) -> Option<ChannelId> {
        candidates.shuffle(&mut thread_rng());
        candidates
            .iter()
            .find(|channel_id| {
                Self::try_send_message(&mut self.scoring, **channel_id, self.channel_limit)
            })
            .cloned()
    }

    fn try_send_message(
        scoring: &mut PeerScoreContainer,
        channel_id: ChannelId,
        channel_limit: usize,
    ) -> bool {
        let mut success = true;
        let modified = scoring.modify(channel_id, |i| {
            if i.outstanding < channel_limit {
                i.outstanding += 1;
                i.request_count_total += 1;
            } else {
                success = false;
            }
        });
        if !modified {
            scoring.insert(PeerScore::new(channel_id));
        }
        success
    }

    pub fn len(&self) -> usize {
        self.scoring.len()
    }

    pub fn decay(&mut self) {
        self.scoring.modify_all(|i| i.decay());
    }

    pub fn clean_up_dead_channels(&mut self, dead_channel_ids: &[ChannelId]) {
        for channel_id in dead_channel_ids {
            self.scoring.remove(*channel_id);
        }
    }

    pub fn container_info(&self) -> ContainerInfo {
        [("scores", self.len(), 0)].into()
    }
}
