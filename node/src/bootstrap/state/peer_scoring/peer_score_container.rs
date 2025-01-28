use rsnano_network::ChannelId;
use std::collections::{BTreeMap, HashMap};

use super::peer_score::PeerScore;

#[derive(Default)]
pub(super) struct PeerScoreContainer {
    by_channel: HashMap<ChannelId, PeerScore>,
    by_outstanding: BTreeMap<usize, Vec<ChannelId>>,
}

impl PeerScoreContainer {
    pub fn len(&self) -> usize {
        self.by_channel.len()
    }

    #[allow(dead_code)]
    pub fn get(&self, channel_id: ChannelId) -> Option<&PeerScore> {
        self.by_channel.get(&channel_id)
    }

    pub fn insert(&mut self, score: PeerScore) -> Option<PeerScore> {
        let outstanding = score.outstanding;
        let channel_id = score.channel_id;

        let old = self.by_channel.insert(score.channel_id, score);

        if let Some(old) = &old {
            self.remove_outstanding(old.channel_id, old.outstanding);
        }

        self.insert_outstanding(channel_id, outstanding);
        old
    }

    pub fn modify(&mut self, channel_id: ChannelId, mut f: impl FnMut(&mut PeerScore)) -> bool {
        if let Some(scoring) = self.by_channel.get_mut(&channel_id) {
            let old_outstanding = scoring.outstanding;
            f(scoring);
            let new_outstanding = scoring.outstanding;
            if new_outstanding != old_outstanding {
                self.remove_outstanding(channel_id, old_outstanding);
                self.insert_outstanding(channel_id, new_outstanding);
            }
            true
        } else {
            false
        }
    }

    pub fn modify_all(&mut self, mut f: impl FnMut(&mut PeerScore)) {
        let channel_ids: Vec<ChannelId> = self.by_channel.keys().cloned().collect();
        for id in channel_ids {
            self.modify(id, &mut f);
        }
    }

    pub fn retain(&mut self, mut f: impl FnMut(&PeerScore) -> bool) {
        let to_delete = self
            .by_channel
            .values()
            .filter(|i| !f(i))
            .map(|i| i.channel_id)
            .collect::<Vec<_>>();

        for channel_id in to_delete {
            self.remove(channel_id);
        }
    }

    pub fn remove(&mut self, channel_id: ChannelId) {
        if let Some(scoring) = self.by_channel.remove(&channel_id) {
            self.remove_outstanding(channel_id, scoring.outstanding);
        }
    }

    fn insert_outstanding(&mut self, channel_id: ChannelId, outstanding: usize) {
        self.by_outstanding
            .entry(outstanding)
            .or_default()
            .push(channel_id);
    }

    fn remove_outstanding(&mut self, channel_id: ChannelId, outstanding: usize) {
        let channel_ids = self.by_outstanding.get_mut(&outstanding).unwrap();
        if channel_ids.len() > 1 {
            channel_ids.retain(|i| *i != channel_id);
        } else {
            self.by_outstanding.remove(&outstanding);
        }
    }
}
