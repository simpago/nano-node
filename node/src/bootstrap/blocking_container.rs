use rsnano_core::{Account, BlockHash};
use std::{
    collections::{BTreeMap, VecDeque},
    mem::size_of,
};

#[derive(Clone)]
pub(crate) struct BlockingEntry {
    pub account: Account,
    pub dependency: BlockHash,
    pub dependency_account: Account,
}

/// A blocked account is an account that has failed to insert a new block because the source block is not currently present in the ledger
/// An account is unblocked once it has a block successfully inserted
#[derive(Default)]
pub(crate) struct OrderedBlocking {
    by_account: BTreeMap<Account, BlockingEntry>,
    sequenced: VecDeque<Account>,
    by_dependency: BTreeMap<BlockHash, Vec<Account>>,
    by_dependency_account: BTreeMap<Account, Vec<Account>>,
}

impl OrderedBlocking {
    pub const ELEMENT_SIZE: usize =
        size_of::<BlockingEntry>() + size_of::<Account>() * 3 + size_of::<f32>();

    pub fn len(&self) -> usize {
        self.sequenced.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn insert(&mut self, entry: BlockingEntry) -> bool {
        let account = entry.account;
        let dependency = entry.dependency;
        let dependency_account = entry.dependency_account;
        if self.by_account.contains_key(&account) {
            return false;
        }

        self.by_account.insert(account, entry);
        self.sequenced.push_back(account);
        self.by_dependency
            .entry(dependency)
            .or_default()
            .push(account);
        self.by_dependency_account
            .entry(dependency_account)
            .or_default()
            .push(account);
        true
    }

    pub fn contains(&self, account: &Account) -> bool {
        self.by_account.contains_key(account)
    }

    pub fn count_by_dependency_account(&self, dep_account: &Account) -> usize {
        self.by_dependency_account
            .get(dep_account)
            .map(|accs| accs.len())
            .unwrap_or_default()
    }

    pub fn next(&self, filter: impl Fn(&BlockHash) -> bool) -> Option<BlockHash> {
        // Scan all entries with unknown dependency account
        let accounts = self.by_dependency_account.get(&Account::zero())?;
        accounts
            .iter()
            .map(|a| self.by_account.get(a).unwrap())
            .find(|e| filter(&e.dependency))
            .map(|e| e.dependency)
    }

    pub fn iter_start_dep_account(&self, start: Account) -> impl Iterator<Item = &BlockingEntry> {
        self.by_dependency_account
            .range(start..)
            .flat_map(|(_, accs)| accs)
            .map(|acc| self.by_account.get(acc).unwrap())
    }

    pub fn get(&self, account: &Account) -> Option<&BlockingEntry> {
        self.by_account.get(account)
    }

    pub fn pop_oldest(&mut self) -> Option<BlockingEntry> {
        let oldest = self.sequenced.front()?.clone();
        self.remove(&oldest)
    }

    pub fn remove(&mut self, account: &Account) -> Option<BlockingEntry> {
        let entry = self.by_account.remove(account)?;
        self.remove_indexes(&entry);
        Some(entry)
    }

    pub fn modify_dependency_account(
        &mut self,
        dependency: &BlockHash,
        new_dependency_account: Account,
    ) -> usize {
        let Some(accounts) = self.by_dependency.get(dependency) else {
            return 0;
        };

        let mut updated = 0;

        for account in accounts {
            let entry = self.by_account.get_mut(account).unwrap();
            if entry.dependency_account != new_dependency_account {
                let old_dependency_account = entry.dependency_account;
                entry.dependency_account = new_dependency_account;
                let old = self
                    .by_dependency_account
                    .get_mut(&old_dependency_account)
                    .unwrap();
                if old.len() == 1 {
                    self.by_dependency_account.remove(&old_dependency_account);
                } else {
                    old.retain(|a| *a != entry.account);
                }
                self.by_dependency_account
                    .entry(new_dependency_account)
                    .or_default()
                    .push(entry.account);

                updated += 1;
            }
        }

        updated
    }

    fn remove_indexes(&mut self, entry: &BlockingEntry) {
        self.sequenced.retain(|i| *i != entry.account);
        let accounts = self.by_dependency.get_mut(&entry.dependency).unwrap();
        if accounts.len() > 1 {
            accounts.retain(|i| *i != entry.account);
        } else {
            self.by_dependency.remove(&entry.dependency);
        }
        let accounts = self
            .by_dependency_account
            .get_mut(&entry.dependency_account)
            .unwrap();
        if accounts.len() > 1 {
            accounts.retain(|i| *i != entry.account);
        } else {
            self.by_dependency_account.remove(&entry.dependency_account);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty() {
        let mut blocking = OrderedBlocking::default();
        assert_eq!(blocking.len(), 0);
        assert_eq!(blocking.is_empty(), true);
        assert_eq!(blocking.contains(&Account::from(1)), false);
        assert_eq!(blocking.count_by_dependency_account(&Account::from(1)), 0);
        assert!(blocking.next(|_| true).is_none());
        assert!(blocking.get(&Account::from(1)).is_none());
        assert!(blocking.remove(&Account::from(1)).is_none());
        assert!(blocking.pop_oldest().is_none());
    }

    #[test]
    fn insert_one() {
        let mut blocking = OrderedBlocking::default();

        let entry = BlockingEntry {
            account: Account::from(5),
            dependency: BlockHash::from(100),
            dependency_account: Account::from(13),
        };
        let inserted = blocking.insert(entry.clone());

        assert_eq!(inserted, true);
        assert_eq!(blocking.len(), 1);
        assert_eq!(blocking.is_empty(), false);
        assert_eq!(blocking.contains(&entry.account), true);
        assert!(blocking.get(&entry.account).is_some());
        assert_eq!(
            blocking.count_by_dependency_account(&entry.dependency_account),
            1
        );
    }

    #[test]
    fn dont_insert_if_account_already_present() {
        let mut blocking = OrderedBlocking::default();

        let entry = BlockingEntry {
            account: Account::from(5),
            dependency: BlockHash::from(100),
            dependency_account: Account::from(13),
        };
        blocking.insert(entry.clone());

        let inserted = blocking.insert(entry.clone());

        assert_eq!(inserted, false);
        assert_eq!(blocking.len(), 1);
    }

    #[test]
    fn next() {
        let mut blocking = OrderedBlocking::default();

        let entry = BlockingEntry {
            account: Account::from(5),
            dependency: BlockHash::from(100),
            dependency_account: Account::zero(),
        };
        blocking.insert(entry.clone());

        assert!(blocking.next(|_| true).is_some());
    }

    #[test]
    fn next_returns_none_when_all_dependency_accounts_are_known() {
        let mut blocking = OrderedBlocking::default();

        let entry = BlockingEntry {
            account: Account::from(5),
            dependency: BlockHash::from(100),
            dependency_account: Account::from(13),
        };
        blocking.insert(entry.clone());

        assert!(blocking.next(|_| true).is_none());
    }

    #[test]
    fn next_with_filter() {
        let mut blocking = OrderedBlocking::default();

        blocking.insert(BlockingEntry {
            account: Account::from(1000),
            dependency: BlockHash::from(100),
            dependency_account: Account::zero(),
        });

        blocking.insert(BlockingEntry {
            account: Account::from(2000),
            dependency: BlockHash::from(200),
            dependency_account: Account::zero(),
        });

        blocking.insert(BlockingEntry {
            account: Account::from(3000),
            dependency: BlockHash::from(300),
            dependency_account: Account::zero(),
        });

        assert_eq!(
            blocking.next(|dep| *dep == BlockHash::from(300)),
            Some(BlockHash::from(300))
        );
    }

    #[test]
    fn pop_front() {
        let mut blocking = OrderedBlocking::default();

        blocking.insert(BlockingEntry {
            account: Account::from(1000),
            dependency: BlockHash::from(100),
            dependency_account: Account::zero(),
        });

        blocking.insert(BlockingEntry {
            account: Account::from(2000),
            dependency: BlockHash::from(200),
            dependency_account: Account::zero(),
        });

        assert_eq!(blocking.pop_oldest().unwrap().account, Account::from(1000));
        assert_eq!(blocking.pop_oldest().unwrap().account, Account::from(2000));
        assert!(blocking.pop_oldest().is_none());
    }

    #[test]
    fn modify_dependency_account() {
        let mut blocking = OrderedBlocking::default();

        let dependency = BlockHash::from(100);
        blocking.insert(BlockingEntry {
            account: Account::from(1000),
            dependency,
            dependency_account: Account::zero(),
        });

        let new_dep_account = Account::from(5000);
        let updated = blocking.modify_dependency_account(&dependency, new_dep_account);

        assert_eq!(updated, 1);
        assert_eq!(
            blocking
                .get(&Account::from(1000))
                .unwrap()
                .dependency_account,
            new_dep_account
        );
    }
}
