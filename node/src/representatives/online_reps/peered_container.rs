use super::PeeredRep;
use rsnano_core::{Account, PublicKey};
use rsnano_network::{Channel, ChannelId};
use rsnano_nullable_clock::Timestamp;
use std::{collections::HashMap, mem::size_of, sync::Arc};

#[derive(Debug, PartialEq, Eq)]
pub enum InsertResult {
    Inserted,
    Updated,
    /// Returns the old channel id
    // TODO return old peer address
    ChannelChanged(ChannelId),
}

/// Collection of all representatives that we have a direct connection to
pub(super) struct PeeredContainer {
    by_account: HashMap<PublicKey, PeeredRep>,
    by_channel_id: HashMap<ChannelId, Vec<PublicKey>>,
}

impl PeeredContainer {
    pub const ELEMENT_SIZE: usize =
        size_of::<PeeredRep>() + size_of::<Account>() + size_of::<usize>() + size_of::<Account>();

    pub fn new() -> Self {
        Self {
            by_account: HashMap::new(),
            by_channel_id: HashMap::new(),
        }
    }

    pub fn update_or_insert2(
        &mut self,
        account: PublicKey,
        channel: Arc<Channel>,
        now: Timestamp,
    ) -> InsertResult {
        self.update_or_insert(account, channel.channel_id(), now)
    }

    pub fn update_or_insert(
        &mut self,
        account: PublicKey,
        channel_id: ChannelId,
        now: Timestamp,
    ) -> InsertResult {
        if let Some(rep) = self.by_account.get_mut(&account) {
            // Update if representative channel was changed
            if rep.channel_id != channel_id {
                let old_channel_id = rep.channel_id;
                let new_channel_id = channel_id;
                rep.channel_id = new_channel_id;
                self.remove_channel_id(&account, old_channel_id);
                self.by_channel_id
                    .entry(new_channel_id)
                    .or_default()
                    .push(account);
                InsertResult::ChannelChanged(old_channel_id)
            } else {
                InsertResult::Updated
            }
        } else {
            self.by_account
                .insert(account, PeeredRep::new(account, channel_id, now));

            let by_id = self.by_channel_id.entry(channel_id).or_default();
            by_id.push(account);
            InsertResult::Inserted
        }
    }

    fn remove_channel_id(&mut self, account: &PublicKey, channel_id: ChannelId) {
        let accounts = self.by_channel_id.get_mut(&channel_id).unwrap();

        if accounts.len() == 1 {
            self.by_channel_id.remove(&channel_id);
        } else {
            accounts.retain(|acc| acc != account);
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &PeeredRep> {
        self.by_account.values()
    }

    pub fn iter_by_channel(&self, channel_id: ChannelId) -> impl Iterator<Item = &PeeredRep> {
        self.accounts_by_channel(channel_id)
            .map(|account| self.by_account.get(account).unwrap())
    }

    pub fn accounts_by_channel(&self, channel_id: ChannelId) -> impl Iterator<Item = &PublicKey> {
        self.by_channel_id.get(&channel_id).into_iter().flatten()
    }

    pub fn accounts(&self) -> impl Iterator<Item = &PublicKey> {
        self.by_account.keys()
    }

    pub fn modify_by_channel(
        &mut self,
        channel_id: ChannelId,
        mut modify: impl FnMut(&mut PeeredRep),
    ) {
        if let Some(rep_accounts) = self.by_channel_id.get(&channel_id) {
            for rep in rep_accounts {
                modify(self.by_account.get_mut(rep).unwrap());
            }
        }
    }

    pub fn len(&self) -> usize {
        self.by_account.len()
    }

    pub fn remove(&mut self, channel_id: ChannelId) -> Vec<PublicKey> {
        let Some(accounts) = self.by_channel_id.remove(&channel_id) else {
            return Vec::new();
        };
        for account in &accounts {
            self.by_account.remove(account);
        }
        accounts
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn empty() {
        let container = PeeredContainer::new();
        assert_eq!(container.len(), 0);
        assert_eq!(container.iter().count(), 0);
        assert_eq!(container.iter_by_channel(42.into()).count(), 0);
        assert_eq!(container.accounts_by_channel(42.into()).count(), 0);
        assert_eq!(container.accounts().count(), 0);
    }

    #[test]
    fn insert_one() {
        let mut container = PeeredContainer::new();
        let account = PublicKey::from(1);
        let channel = Arc::new(Channel::new_test_instance());
        let channel_id = channel.channel_id();
        let now = Timestamp::new_test_instance();
        assert_eq!(
            container.update_or_insert2(account, channel, now),
            InsertResult::Inserted
        );
        assert_eq!(container.len(), 1);

        assert_eq!(
            container.iter().cloned().collect::<Vec<_>>(),
            vec![PeeredRep::new(account, channel_id, now)]
        );
        assert_eq!(
            container
                .iter_by_channel(channel_id)
                .cloned()
                .collect::<Vec<_>>(),
            vec![PeeredRep::new(account, channel_id, now)]
        );
        assert_eq!(
            container
                .accounts_by_channel(channel_id)
                .cloned()
                .collect::<Vec<_>>(),
            vec![account]
        );
        assert_eq!(
            container.accounts().cloned().collect::<Vec<_>>(),
            vec![account]
        );
    }

    #[test]
    fn insert_two() {
        let mut container = PeeredContainer::new();
        let now = Timestamp::new_test_instance();
        let channel1 = Arc::new(Channel::new_test_instance_with_id(101));
        let channel2 = Arc::new(Channel::new_test_instance_with_id(102));
        assert_eq!(
            container.update_or_insert2(PublicKey::from(100), channel1, now,),
            InsertResult::Inserted
        );
        assert_eq!(
            container.update_or_insert2(
                PublicKey::from(200),
                channel2,
                now + Duration::from_secs(1),
            ),
            InsertResult::Inserted
        );
        assert_eq!(container.len(), 2);
        assert_eq!(container.iter().count(), 2);
        assert_eq!(container.accounts().count(), 2);
    }

    #[test]
    fn remove_one() {
        let mut container = PeeredContainer::new();

        let channel = Arc::new(Channel::new_test_instance());
        let channel_id = channel.channel_id();
        let now = Timestamp::new_test_instance();
        container.update_or_insert2(PublicKey::from(100), channel, now);

        container.remove(channel_id);
        assert_eq!(container.len(), 0);
        assert_eq!(container.iter().count(), 0);
    }

    #[test]
    fn remove_from_container_with_multiple_entries() {
        let mut container = PeeredContainer::new();

        let now = Timestamp::new_test_instance();
        let channel1 = Arc::new(Channel::new_test_instance_with_id(1));
        let channel2 = Arc::new(Channel::new_test_instance_with_id(2));
        let channel3 = Arc::new(Channel::new_test_instance_with_id(3));
        let channel_id = channel2.channel_id();
        container.update_or_insert2(PublicKey::from(100), channel1, now);
        container.update_or_insert2(PublicKey::from(200), channel2, now + Duration::from_secs(1));
        container.update_or_insert2(PublicKey::from(300), channel3, now + Duration::from_secs(2));

        container.remove(channel_id);
        assert_eq!(container.len(), 2);
        assert_eq!(container.iter_by_channel(channel_id).count(), 0);
    }

    #[test]
    fn modify_by_channel() {
        let mut container = PeeredContainer::new();
        let now = Timestamp::new_test_instance();

        let channel1 = Arc::new(Channel::new_test_instance_with_id(1));
        let channel2 = Arc::new(Channel::new_test_instance_with_id(2));
        let channel_id = channel2.channel_id();
        container.update_or_insert2(PublicKey::from(100), channel1, now);
        container.update_or_insert2(PublicKey::from(200), channel2, now + Duration::from_secs(1));

        let new_value = now + Duration::from_secs(1234);
        container.modify_by_channel(channel_id, |rep| {
            rep.last_request = new_value;
        });
        assert_eq!(
            container
                .iter_by_channel(channel_id)
                .next()
                .unwrap()
                .last_request,
            new_value
        );
    }

    #[test]
    fn update_entry() {
        let mut container = PeeredContainer::new();
        let now = Timestamp::new_test_instance();

        let account = PublicKey::from(1);
        let channel = Arc::new(Channel::new_test_instance());
        container.update_or_insert2(account, channel.clone(), now);
        assert_eq!(
            container.update_or_insert2(account, channel, now + Duration::from_secs(2)),
            InsertResult::Updated
        );
        assert_eq!(container.len(), 1);
    }

    #[test]
    fn channel_changed() {
        let mut container = PeeredContainer::new();
        let now = Timestamp::new_test_instance();

        let account = PublicKey::from(1);
        let channel_a = Arc::new(Channel::new_test_instance_with_id(2));
        let channel_b = Arc::new(Channel::new_test_instance_with_id(3));
        container.update_or_insert2(account, channel_a.clone(), now);
        assert_eq!(
            container.update_or_insert2(account, channel_b.clone(), now + Duration::from_secs(2)),
            InsertResult::ChannelChanged(channel_a.channel_id())
        );
        assert_eq!(container.len(), 1);
        assert_eq!(container.iter_by_channel(channel_a.channel_id()).count(), 0);
        assert_eq!(container.iter_by_channel(channel_b.channel_id()).count(), 1);
    }

    #[test]
    fn two_reps_in_same_channel() {
        let mut container = PeeredContainer::new();
        let now = Timestamp::new_test_instance();

        let account_a = PublicKey::from(1);
        let account_b = PublicKey::from(2);
        let channel = Arc::new(Channel::new_test_instance());
        assert_eq!(
            container.update_or_insert2(account_a, channel.clone(), now),
            InsertResult::Inserted,
        );
        assert_eq!(
            container.update_or_insert2(account_b, channel.clone(), now),
            InsertResult::Inserted,
        );

        assert_eq!(container.len(), 2);
        assert_eq!(container.iter_by_channel(channel.channel_id()).count(), 2);
    }
}
