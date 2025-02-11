use super::peer_score::PeerScore;
use rsnano_network::ChannelId;
use std::collections::HashMap;

#[derive(Default)]
pub(super) struct PeerScoreContainer {
    by_channel: HashMap<ChannelId, PeerScore>,
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
        self.by_channel.insert(score.channel_id, score)
    }

    pub fn modify(&mut self, channel_id: ChannelId, mut f: impl FnMut(&mut PeerScore)) -> bool {
        if let Some(scoring) = self.by_channel.get_mut(&channel_id) {
            f(scoring);
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

    pub fn remove(&mut self, channel_id: ChannelId) {
        self.by_channel.remove(&channel_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty() {
        let container = PeerScoreContainer::default();
        assert_eq!(container.len(), 0);
    }

    #[test]
    fn insert() {
        let mut container = PeerScoreContainer::default();
        let channel_id = ChannelId::from(42);
        container.insert(PeerScore::new(channel_id));
        assert_eq!(container.len(), 1);
        assert!(container.get(channel_id).is_some())
    }

    #[test]
    fn remove() {
        let mut container = PeerScoreContainer::default();
        let channel_id = ChannelId::from(42);
        let another_channel_id = ChannelId::from(100);
        container.insert(PeerScore::new(channel_id));
        container.insert(PeerScore::new(another_channel_id));

        container.remove(channel_id);

        assert_eq!(container.len(), 1);
        assert!(container.get(channel_id).is_none());
        assert!(container.get(another_channel_id).is_some());
    }

    #[test]
    fn remove_non_existing() {
        let mut container = PeerScoreContainer::default();
        container.remove(ChannelId::from(42));
        assert_eq!(container.len(), 0);
    }

    #[test]
    fn modify_nothing() {
        let mut container = PeerScoreContainer::default();
        let modified = container.modify(ChannelId::from(42), |_| unreachable!());
        assert!(!modified);
        assert_eq!(container.len(), 0);
    }

    #[test]
    fn modify() {
        let mut container = PeerScoreContainer::default();
        let channel_id = ChannelId::from(42);
        let another_channel_id = ChannelId::from(100);
        container.insert(PeerScore::new(channel_id));
        container.insert(PeerScore::new(another_channel_id));
        let modified = container.modify(channel_id, |i| {
            i.outstanding = 1000;
        });
        assert!(modified);
        assert_eq!(container.get(channel_id).unwrap().outstanding, 1000);
        assert_ne!(container.get(another_channel_id).unwrap().outstanding, 1000);
    }

    #[test]
    fn modify_all() {
        let mut container = PeerScoreContainer::default();
        let channel_id = ChannelId::from(42);
        let another_channel_id = ChannelId::from(100);
        container.insert(PeerScore::new(channel_id));
        container.insert(PeerScore::new(another_channel_id));
        container.modify_all(|i| {
            i.outstanding = 1000;
        });
        assert_eq!(container.get(channel_id).unwrap().outstanding, 1000);
        assert_eq!(container.get(another_channel_id).unwrap().outstanding, 1000);
    }
}
