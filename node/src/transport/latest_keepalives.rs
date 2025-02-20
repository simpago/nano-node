use rand::seq::IteratorRandom;
use rsnano_messages::Keepalive;
use rsnano_network::{ChannelId, DeadChannelCleanupStep};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

/// Keeps the last keepalive message per channel in memory, so that we can
/// later use that information, when we want to connect to more nodes
pub struct LatestKeepalives {
    entries: HashMap<ChannelId, Keepalive>,
    max_len: usize,
}

impl Default for LatestKeepalives {
    fn default() -> Self {
        Self::with_max_len(1000)
    }
}

impl LatestKeepalives {
    pub fn with_max_len(max_len: usize) -> Self {
        Self {
            entries: HashMap::new(),
            max_len,
        }
    }

    pub fn insert(&mut self, channel_id: ChannelId, keepalive: Keepalive) {
        while self.len() >= self.max_len {
            self.pop_random();
        }
        self.entries.insert(channel_id, keepalive);
    }

    pub fn get(&self, channel_id: ChannelId) -> Option<&Keepalive> {
        self.entries.get(&channel_id)
    }

    //TODO: randomize
    pub fn pop_random(&mut self) -> Option<Keepalive> {
        let mut rng = rand::rng();
        if let Some(&channel_id) = self.entries.keys().choose(&mut rng) {
            self.entries.remove(&channel_id)
        } else {
            None
        }
    }

    pub fn remove(&mut self, channel_id: ChannelId) {
        self.entries.remove(&channel_id);
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn max_len(&self) -> usize {
        self.max_len
    }
}

pub(crate) struct LatestKeepalivesCleanup {
    keepalives: Arc<Mutex<LatestKeepalives>>,
}

impl LatestKeepalivesCleanup {
    pub(crate) fn new(keepalives: Arc<Mutex<LatestKeepalives>>) -> Self {
        Self { keepalives }
    }
}

impl DeadChannelCleanupStep for LatestKeepalivesCleanup {
    fn clean_up_dead_channels(&self, dead_channel_ids: &[ChannelId]) {
        let mut keepalives = self.keepalives.lock().unwrap();
        for channel_id in dead_channel_ids {
            keepalives.remove(*channel_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rsnano_core::utils::{TEST_ENDPOINT_2, TEST_ENDPOINT_3};

    #[test]
    fn empty() {
        let cache = LatestKeepalives::default();
        assert_eq!(cache.len(), 0);
        assert_eq!(cache.get(ChannelId::from(1)), None);
        assert_eq!(cache.max_len(), 1000);
    }

    #[test]
    fn insert_one() {
        let mut cache = LatestKeepalives::default();
        let channel_id = ChannelId::from(1);
        cache.insert(channel_id, KEEPALIVE_1);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(channel_id).cloned(), Some(KEEPALIVE_1));
    }

    #[test]
    fn insert_two_for_different_channel() {
        let mut cache = LatestKeepalives::default();
        cache.insert(ChannelId::from(1), KEEPALIVE_1);
        cache.insert(ChannelId::from(2), KEEPALIVE_2);
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn inserting_for_same_channel_should_replace_previous_entry() {
        let mut cache = LatestKeepalives::default();
        let channel_id = ChannelId::from(1);
        cache.insert(channel_id, KEEPALIVE_1);
        cache.insert(channel_id, KEEPALIVE_2);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(channel_id).cloned(), Some(KEEPALIVE_2))
    }

    #[test]
    fn pop_the_only_entry() {
        let mut cache = LatestKeepalives::default();
        cache.insert(ChannelId::from(1), KEEPALIVE_1);

        let popped = cache.pop_random().unwrap();

        assert_eq!(popped, KEEPALIVE_1);
        assert_eq!(cache.len(), 0);
    }

    #[test]
    fn pop_multiple() {
        let mut cache = LatestKeepalives::default();
        cache.insert(ChannelId::from(1), KEEPALIVE_1);
        cache.insert(ChannelId::from(2), KEEPALIVE_2);
        cache.insert(ChannelId::from(3), KEEPALIVE_3);

        assert!(cache.pop_random().is_some());
        assert!(cache.pop_random().is_some());
        assert!(cache.pop_random().is_some());
        assert_eq!(cache.pop_random(), None);
    }

    #[test]
    fn max_len() {
        let mut cache = LatestKeepalives::with_max_len(2);
        assert_eq!(cache.max_len(), 2);
        cache.insert(ChannelId::from(1), KEEPALIVE_1);
        cache.insert(ChannelId::from(2), KEEPALIVE_2);
        cache.insert(ChannelId::from(3), KEEPALIVE_3);

        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn remove() {
        let mut cache = LatestKeepalives::default();
        cache.insert(ChannelId::from(1), KEEPALIVE_1);
        cache.insert(ChannelId::from(2), KEEPALIVE_2);
        cache.insert(ChannelId::from(3), KEEPALIVE_3);

        cache.remove(ChannelId::from(2));

        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get(ChannelId::from(2)), None);
    }

    const KEEPALIVE_1: Keepalive = Keepalive::new_test_instance();
    const KEEPALIVE_2: Keepalive = Keepalive {
        peers: [TEST_ENDPOINT_2; 8],
    };
    const KEEPALIVE_3: Keepalive = Keepalive {
        peers: [TEST_ENDPOINT_3; 8],
    };
}
