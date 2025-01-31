use super::frontier_head::{FrontierHead, FrontierHeadsConfig};
use primitive_types::U256;
use rsnano_core::Account;
use rsnano_nullable_clock::Timestamp;
use std::collections::BTreeMap;

#[derive(Default)]
pub(super) struct HeadsContainer {
    config: FrontierHeadsConfig,
    sequenced: Vec<Account>,
    by_start: BTreeMap<Account, FrontierHead>,
    by_timestamp: BTreeMap<Timestamp, Vec<Account>>,
}

impl HeadsContainer {
    /// Divide account numeric range into consecutive and equal ranges
    pub fn with_heads(config: FrontierHeadsConfig) -> Self {
        let mut heads = Self {
            config,
            ..Default::default()
        };

        for i in 0..heads.config.parallelism {
            heads.push_back(Self::create_head(heads.config.clone(), i));
        }

        heads
    }

    #[cfg(test)]
    pub fn add_head(&mut self, start: impl Into<Account>, end: impl Into<Account>) {
        self.push_back(FrontierHead::new(start, end, self.config.clone()));
    }

    fn create_head(config: FrontierHeadsConfig, index: usize) -> FrontierHead {
        let range_size = Account::MAX.number() / config.parallelism;

        // Start at 1 to avoid the burn account
        let start = if index == 0 {
            U256::from(1)
        } else {
            range_size * index
        };
        let end = if index == config.parallelism - 1 {
            Account::MAX.number()
        } else {
            start + range_size
        };
        FrontierHead::new(start, end, config)
    }

    fn push_back(&mut self, head: FrontierHead) {
        let start = head.start;
        let timestamp = head.timestamp;
        let mut inserted = true;
        self.by_start
            .entry(start)
            .and_modify(|_| inserted = false)
            .or_insert(head);

        if !inserted {
            return;
        }
        self.sequenced.push(start);
        self.by_timestamp.entry(timestamp).or_default().push(start);
    }

    pub fn ordered_by_timestamp(&self) -> impl Iterator<Item = &FrontierHead> {
        self.by_timestamp
            .values()
            .flatten()
            .map(|start| self.by_start.get(start).unwrap())
    }

    pub fn modify<F>(&mut self, start: impl Into<Account>, mut f: F)
    where
        F: FnMut(&mut FrontierHead),
    {
        let start = start.into();
        if let Some(head) = self.by_start.get_mut(&start) {
            let old_timestamp = head.timestamp;
            f(head);
            if head.timestamp != old_timestamp {
                let accounts = self.by_timestamp.get_mut(&old_timestamp).unwrap();
                if accounts.len() == 1 {
                    self.by_timestamp.remove(&old_timestamp);
                } else {
                    accounts.retain(|a| *a != start);
                }
                self.by_timestamp
                    .entry(head.timestamp)
                    .or_default()
                    .push(start);
            }
        } else {
            panic!("head not found: {}", start.encode_account());
        }
    }

    pub fn find_first_less_than_or_equal_to(&self, account: impl Into<Account>) -> Option<Account> {
        self.by_start
            .range(..=account.into())
            .last()
            .map(|(start, _)| *start)
    }

    pub fn iter(&self) -> impl Iterator<Item = &FrontierHead> {
        self.by_start.values()
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.sequenced.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn empty() {
        let heads = HeadsContainer::default();

        assert_eq!(heads.len(), 0);
        assert!(heads.iter().next().is_none());
        assert!(heads.ordered_by_timestamp().next().is_none());
    }

    #[test]
    fn push_back_one_head() {
        let heads = container_with_heads([(1, 10)]);

        assert_eq!(heads.len(), 1);
        assert_eq!(heads.iter().count(), 1);
        assert_eq!(heads.ordered_by_timestamp().count(), 1);
    }

    #[test]
    fn push_back_multiple() {
        let heads = container_with_heads([(1, 10), (10, 20), (20, 30)]);

        assert_eq!(heads.len(), 3);
        assert_eq!(heads.iter().count(), 3);
        assert_eq!(heads.ordered_by_timestamp().count(), 3);
    }

    #[test]
    fn order_by_timestamp() {
        let mut heads = container_with_heads([(1, 10), (10, 20), (20, 30)]);
        let now = Timestamp::new_test_instance();
        heads.modify(1, |h| h.timestamp = now + Duration::from_secs(100));
        heads.modify(10, |h| h.timestamp = now + Duration::from_secs(99));
        heads.modify(20, |h| h.timestamp = now + Duration::from_secs(101));

        let ordered: Vec<_> = heads.ordered_by_timestamp().collect();
        assert_eq!(ordered[0].start, 10.into());
        assert_eq!(ordered[1].start, 1.into());
        assert_eq!(ordered[2].start, 20.into());
    }

    #[test]
    #[should_panic = "head not found"]
    fn modify_unknown_start_panics() {
        let mut heads = HeadsContainer::default();
        heads.modify(123, |_| {});
    }

    #[test]
    fn modify_nothing() {
        let mut heads = container_with_heads([(1, 10)]);

        heads.modify(1, |_| {});

        assert_eq!(heads.iter().next().unwrap().timestamp, Default::default());
        assert_eq!(heads.sequenced.len(), 1);
        assert_eq!(heads.by_timestamp.len(), 1);
        assert_eq!(heads.by_start.len(), 1);
    }

    #[test]
    fn modify_timestamp() {
        let mut heads = container_with_heads([(1, 10)]);

        let now = Timestamp::new_test_instance();
        heads.modify(1, |head| head.timestamp = now);

        assert_eq!(heads.iter().next().unwrap().timestamp, now);
        assert_eq!(heads.sequenced.len(), 1);
        assert_eq!(heads.by_start.len(), 1);
        assert_eq!(heads.by_timestamp.len(), 1);
        assert_eq!(*heads.by_timestamp.first_key_value().unwrap().0, now);
    }

    #[test]
    fn modify_duplicate_timestamp() {
        let mut heads = container_with_heads([(1, 10), (10, 20)]);

        let now = Timestamp::new_test_instance();
        heads.modify(1, |head| head.timestamp = now);

        assert_eq!(heads.by_timestamp.len(), 2);
        assert_eq!(
            heads.by_timestamp.get(&Default::default()).unwrap(),
            &vec![Account::from(10)]
        );
        assert_eq!(
            heads.by_timestamp.get(&now).unwrap(),
            &vec![Account::from(1)]
        );
    }

    #[test]
    fn find_first_less_than_or_equal_to() {
        let heads = container_with_heads([(1, 10), (10, 20)]);

        assert_eq!(heads.find_first_less_than_or_equal_to(0), None);
        assert_eq!(
            heads.find_first_less_than_or_equal_to(1),
            Some(Account::from(1))
        );
        assert_eq!(
            heads.find_first_less_than_or_equal_to(2),
            Some(Account::from(1))
        );
        assert_eq!(
            heads.find_first_less_than_or_equal_to(9),
            Some(Account::from(1))
        );
        assert_eq!(
            heads.find_first_less_than_or_equal_to(10),
            Some(Account::from(10))
        );
        assert_eq!(
            heads.find_first_less_than_or_equal_to(10),
            Some(Account::from(10))
        );
        assert_eq!(
            heads.find_first_less_than_or_equal_to(20),
            Some(Account::from(10))
        );
        assert_eq!(
            heads.find_first_less_than_or_equal_to(30),
            Some(Account::from(10))
        );
    }

    #[test]
    fn ignore_duplicate_insert() {
        let heads = container_with_heads([(1, 10), (1, 10)]);
        assert_eq!(heads.len(), 1);
        assert_eq!(heads.sequenced.len(), 1);
        assert_eq!(heads.by_start.len(), 1);
    }

    fn container_with_heads(ranges: impl IntoIterator<Item = (u64, u64)>) -> HeadsContainer {
        let mut heads = HeadsContainer::default();

        for (start, end) in ranges.into_iter() {
            heads.add_head(start, end);
        }

        heads
    }
}
