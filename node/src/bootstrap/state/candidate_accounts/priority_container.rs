use rsnano_core::Account;
use rsnano_nullable_clock::Timestamp;
use std::collections::BTreeMap;
use std::mem::size_of;

use super::priority::{Priority, PriorityKeyDesc};

#[derive(Clone, Default)]
pub(super) struct PriorityEntry {
    pub account: Account,
    pub priority: Priority,
    pub fails: usize,
    pub timestamp: Option<Timestamp>,
}

impl PriorityEntry {
    pub fn new(account: Account, priority: Priority) -> Self {
        Self {
            account,
            priority,
            fails: 0,
            timestamp: None,
        }
    }

    #[allow(dead_code)]
    pub fn new_test_instance() -> Self {
        Self {
            account: Account::from(7),
            priority: Priority::new(3.0),
            fails: 0,
            timestamp: None,
        }
    }
}

/// Tracks the ongoing account priorities
/// This only stores account priorities > 1.0f.
#[derive(Default)]
pub(super) struct PriorityContainer {
    by_account: BTreeMap<Account, PriorityEntry>,
    by_priority: BTreeMap<PriorityKeyDesc, Vec<Account>>, // descending
}

pub(crate) enum ChangePriorityResult {
    Updated,
    Deleted,
    NotFound,
}

impl PriorityContainer {
    pub const ELEMENT_SIZE: usize =
        size_of::<PriorityEntry>() + size_of::<Account>() + size_of::<f32>() + size_of::<u64>() * 4;

    pub fn len(&self) -> usize {
        self.by_account.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_account.is_empty()
    }

    pub fn get(&self, account: &Account) -> Option<&PriorityEntry> {
        self.by_account.get(account)
    }

    pub fn contains(&self, account: &Account) -> bool {
        self.by_account.contains_key(account)
    }

    pub fn insert(&mut self, entry: PriorityEntry) -> bool {
        let account = entry.account;
        let priority = entry.priority;

        if self.by_account.contains_key(&account) {
            return false;
        }

        self.by_account.insert(account, entry);
        self.by_priority
            .entry(priority.into())
            .or_default()
            .push(account);
        true
    }

    pub fn pop_lowest_prio(&mut self) -> Option<PriorityEntry> {
        let lowest_prio_account = {
            let (_, v) = self.by_priority.last_key_value()?;
            v.first().unwrap().clone()
        };
        Some(self.remove_account(&lowest_prio_account))
    }

    pub fn change_timestamp(&mut self, account: &Account, timestamp: Option<Timestamp>) {
        if let Some(entry) = self.by_account.get_mut(account) {
            entry.timestamp = timestamp;
        }
    }

    pub fn modify(
        &mut self,
        account: &Account,
        mut f: impl FnMut(&mut PriorityEntry) -> bool,
    ) -> ChangePriorityResult {
        if let Some(entry) = self.by_account.get_mut(account) {
            let old_prio = entry.priority;
            if f(entry) {
                if entry.priority != old_prio {
                    let new_prio = entry.priority;
                    self.change_priority_internal(account, old_prio, new_prio)
                }
                ChangePriorityResult::Updated
            } else {
                self.remove_account(account);
                ChangePriorityResult::Deleted
            }
        } else {
            ChangePriorityResult::NotFound
        }
    }

    pub fn next_priority(
        &self,
        cutoff: Timestamp,
        filter: impl Fn(&Account) -> bool,
    ) -> Option<&PriorityEntry> {
        self.by_priority
            .values()
            .flatten()
            .map(|account| self.by_account.get(account).unwrap())
            .find(|entry| {
                if let Some(ts) = entry.timestamp {
                    if ts > cutoff {
                        return false;
                    }
                }
                filter(&entry.account)
            })
    }

    pub fn remove(&mut self, account: &Account) -> Option<PriorityEntry> {
        if let Some(entry) = self.by_account.remove(account) {
            self.remove_priority(account, entry.priority);
            Some(entry)
        } else {
            None
        }
    }

    fn change_priority_internal(
        &mut self,
        account: &Account,
        old_prio: Priority,
        new_prio: Priority,
    ) {
        self.remove_priority(account, old_prio);
        self.by_priority
            .entry(new_prio.into())
            .or_default()
            .push(*account);
    }

    fn remove_account(&mut self, account: &Account) -> PriorityEntry {
        let entry = self.by_account.remove(account).unwrap();
        self.remove_priority(account, entry.priority);
        entry
    }

    fn remove_priority(&mut self, account: &Account, priority: Priority) {
        let ids = self.by_priority.get_mut(&priority.into()).unwrap();
        if ids.len() > 1 {
            ids.retain(|i| i != account);
        } else {
            self.by_priority.remove(&priority.into());
        }
    }

    pub fn clear(&mut self) {
        self.by_account.clear();
        self.by_priority.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty() {
        let mut priorities = PriorityContainer::default();
        assert_eq!(priorities.len(), 0);
        assert!(priorities.is_empty());
        assert!(priorities.get(&Account::from(1)).is_none());
        assert_eq!(priorities.contains(&Account::from(1)), false);
        assert!(priorities.pop_lowest_prio().is_none());
        assert!(priorities.remove(&Account::from(1)).is_none());
    }

    #[test]
    fn insert_one() {
        let mut priorities = PriorityContainer::default();
        let entry = PriorityEntry::new_test_instance();
        assert!(priorities.insert(entry.clone()));
        assert_eq!(priorities.len(), 1);
        assert_eq!(priorities.is_empty(), false);
        assert_eq!(priorities.contains(&entry.account), true);
        assert!(priorities.get(&entry.account).is_some());
    }

    #[test]
    fn insert_two() {
        let mut priorities = PriorityContainer::default();
        assert!(priorities.insert(PriorityEntry::new(Account::from(1), Priority::new(2.5))));
        assert!(priorities.insert(PriorityEntry::new(Account::from(2), Priority::new(3.5))));
        assert_eq!(priorities.len(), 2);
        assert_eq!(priorities.is_empty(), false);
        assert_eq!(priorities.contains(&Account::from(1)), true);
        assert_eq!(priorities.contains(&Account::from(2)), true);
    }

    #[test]
    fn dont_insert_when_account_already_present() {
        let mut priorities = PriorityContainer::default();
        priorities.insert(PriorityEntry::new(Account::from(1), Priority::new(2.5)));
        let inserted = priorities.insert(PriorityEntry::new(Account::from(1), Priority::new(3.5)));
        assert_eq!(inserted, false);
        assert_eq!(priorities.len(), 1);
    }

    #[test]
    fn pop_front() {
        let mut priorities = PriorityContainer::default();
        priorities.insert(PriorityEntry::new(Account::from(1), Priority::new(2.5)));
        priorities.insert(PriorityEntry::new(Account::from(2), Priority::new(2.5)));
        priorities.insert(PriorityEntry::new(Account::from(3), Priority::new(2.5)));

        assert_eq!(
            priorities.pop_lowest_prio().unwrap().account,
            Account::from(1)
        );
        assert_eq!(
            priorities.pop_lowest_prio().unwrap().account,
            Account::from(2)
        );
        assert_eq!(
            priorities.pop_lowest_prio().unwrap().account,
            Account::from(3)
        );
        assert!(priorities.pop_lowest_prio().is_none());
    }

    #[test]
    fn change_timestamp() {
        let account = Account::from(1);
        let mut priorities = PriorityContainer::default();
        priorities.insert(PriorityEntry::new(account, Priority::new(2.5)));
        let now = Timestamp::new_test_instance();

        priorities.change_timestamp(&account, Some(now));

        assert_eq!(priorities.get(&account).unwrap().timestamp, Some(now));
    }

    mod next_priority {
        use super::*;
        use std::time::Duration;

        #[test]
        fn empty() {
            let priorities = PriorityContainer::default();
            let next = priorities.next_priority(Timestamp::new_test_instance(), |_account| true);
            assert!(next.is_none());
        }

        #[test]
        fn one_item() {
            let mut priorities = PriorityContainer::default();
            let account = Account::from(1);
            priorities.insert(PriorityEntry::new(account, Priority::new(2.5)));

            let next = priorities
                .next_priority(Timestamp::new_test_instance(), |_account| true)
                .unwrap();

            assert_eq!(next.account, account);
        }

        #[test]
        fn ordered_by_priority_desc() {
            let mut priorities = PriorityContainer::default();
            priorities.insert(PriorityEntry::new(Account::from(1), Priority::new(2.5)));
            priorities.insert(PriorityEntry::new(Account::from(2), Priority::new(10.0)));
            priorities.insert(PriorityEntry::new(Account::from(3), Priority::new(3.5)));

            let next = priorities
                .next_priority(Timestamp::new_test_instance(), |_account| true)
                .unwrap();

            assert_eq!(next.account, Account::from(2));
        }

        #[test]
        fn cutoff() {
            let now = Timestamp::new_test_instance();
            let a = PriorityEntry::new(Account::from(1), Priority::new(2.5));
            let mut b = PriorityEntry::new(Account::from(2), Priority::new(10.0));
            b.timestamp = Some(now);
            let mut c = PriorityEntry::new(Account::from(3), Priority::new(3.5));
            c.timestamp = Some(now - Duration::from_secs(60));
            let mut priorities = PriorityContainer::default();
            priorities.insert(a);
            priorities.insert(b);
            priorities.insert(c);

            let next = priorities
                .next_priority(now - Duration::from_secs(30), |_account| true)
                .unwrap();

            assert_eq!(next.account, Account::from(3));
        }

        #[test]
        fn filter() {
            let a = PriorityEntry::new(Account::from(1), Priority::new(2.5));
            let b = PriorityEntry::new(Account::from(2), Priority::new(10.0));
            let c = PriorityEntry::new(Account::from(3), Priority::new(3.5));
            let mut priorities = PriorityContainer::default();
            priorities.insert(a);
            priorities.insert(b);
            priorities.insert(c);

            let next = priorities
                .next_priority(Timestamp::new_test_instance(), |account| {
                    *account == Account::from(1)
                })
                .unwrap();

            assert_eq!(next.account, Account::from(1));
        }
    }

    #[test]
    fn change_priority() {
        let mut priorities = PriorityContainer::default();
        priorities.insert(PriorityEntry::new(Account::from(1), Priority::new(2.5)));
        priorities.insert(PriorityEntry::new(Account::from(2), Priority::new(3.0)));
        priorities.insert(PriorityEntry::new(Account::from(3), Priority::new(3.5)));

        let mut old_priority = Priority::ZERO;
        let new_priority = Priority::new(10.0);

        priorities.modify(&Account::from(2), |entry| {
            old_priority = entry.priority;
            entry.priority = new_priority;
            true
        });

        assert_eq!(old_priority, Priority::new(3.0));
        assert_eq!(
            priorities.get(&Account::from(2)).unwrap().priority,
            new_priority
        );

        let next = priorities
            .next_priority(Timestamp::new_test_instance(), |_| true)
            .unwrap();
        assert_eq!(next.account, Account::from(2));
    }

    #[test]
    fn remove_by_priority_change() {
        let mut priorities = PriorityContainer::default();
        let account = Account::from(1);
        priorities.insert(PriorityEntry::new(account, Priority::new(2.5)));

        priorities.modify(&account, |_| false);

        assert_eq!(priorities.len(), 0);
    }

    #[test]
    fn remove() {
        let mut priorities = PriorityContainer::default();
        priorities.insert(PriorityEntry::new(Account::from(1), Priority::new(2.5)));
        priorities.insert(PriorityEntry::new(Account::from(2), Priority::new(3.0)));
        priorities.insert(PriorityEntry::new(Account::from(3), Priority::new(3.5)));

        let removed = priorities.remove(&Account::from(2)).unwrap();

        assert_eq!(removed.account, Account::from(2));
        assert_eq!(priorities.len(), 2);
    }
}
