use super::{
    ordered_blocking::{BlockingEntry, OrderedBlocking},
    priority::Priority,
    priority_container::{ChangePriorityResult, PriorityContainer},
};
use crate::bootstrap::priority_container::PriorityEntry;
use rsnano_core::{utils::ContainerInfo, Account, BlockHash};
use rsnano_nullable_clock::Timestamp;
use std::{cmp::min, time::Duration};

#[derive(Clone, Debug, PartialEq)]
pub struct AccountSetsConfig {
    pub consideration_count: usize,
    pub priorities_max: usize,
    pub blocking_max: usize,
    pub cooldown: Duration,
}

impl Default for AccountSetsConfig {
    fn default() -> Self {
        Self {
            consideration_count: 4,
            priorities_max: 256 * 1024,
            blocking_max: 256 * 1024,
            cooldown: Duration::from_secs(3),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PriorityUpResult {
    Inserted,
    Updated,
    InvalidAccount,
    AccountBlocked,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum PriorityDownResult {
    Deprioritized,
    Erased,
    AccountNotFound,
    InvalidAccount,
}

/// This struct tracks various account sets which are shared among the multiple bootstrap threads
pub(crate) struct AccountSets {
    config: AccountSetsConfig,
    priorities: PriorityContainer,
    blocking: OrderedBlocking,
}

impl AccountSets {
    pub const PRIORITY_INITIAL: Priority = Priority::new(2.0);
    pub const PRIORITY_INCREASE: Priority = Priority::new(2.0);
    pub const PRIORITY_DIVIDE: f64 = 2.0;
    pub const PRIORITY_MAX: Priority = Priority::new(128.0);
    pub const PRIORITY_CUTOFF: Priority = Priority::new(0.15);
    pub const MAX_FAILS: usize = 3;

    pub fn new(config: AccountSetsConfig) -> Self {
        Self {
            config,
            priorities: Default::default(),
            blocking: Default::default(),
        }
    }

    pub fn priority_up(&mut self, account: &Account) -> PriorityUpResult {
        if account.is_zero() {
            return PriorityUpResult::InvalidAccount;
        }

        if !self.blocked(account) {
            let updated = self.priorities.modify(account, |entry| {
                entry.priority = Self::higher_priority(entry.priority);
                entry.fails = 0;
                true // keep this entry
            });

            match updated {
                ChangePriorityResult::Updated | ChangePriorityResult::Deleted => {
                    PriorityUpResult::Updated
                }
                ChangePriorityResult::NotFound => {
                    self.priorities
                        .insert(PriorityEntry::new(*account, Self::PRIORITY_INITIAL));

                    self.trim_overflow();
                    PriorityUpResult::Inserted
                }
            }
        } else {
            PriorityUpResult::AccountBlocked
        }
    }

    fn higher_priority(priority: Priority) -> Priority {
        min(priority + Self::PRIORITY_INCREASE, Self::PRIORITY_MAX)
    }

    pub fn priority_down(&mut self, account: &Account) -> PriorityDownResult {
        if account.is_zero() {
            return PriorityDownResult::InvalidAccount;
        }

        let change_result = self.priorities.modify(account, |entry| {
            let priority = entry.priority / Self::PRIORITY_DIVIDE;
            if entry.fails >= AccountSets::MAX_FAILS
                || entry.fails as f64 >= entry.priority.as_f64()
                || priority <= Self::PRIORITY_CUTOFF
            {
                false // delete entry
            } else {
                entry.fails += 1;
                entry.priority = priority;
                true // keep
            }
        });

        match change_result {
            ChangePriorityResult::Updated => PriorityDownResult::Deprioritized,
            ChangePriorityResult::Deleted => PriorityDownResult::Erased,
            ChangePriorityResult::NotFound => PriorityDownResult::AccountNotFound,
        }
    }

    pub fn priority_set_initial(&mut self, account: &Account) -> bool {
        self.priority_set(account, Self::PRIORITY_INITIAL)
    }

    pub fn priority_set(&mut self, account: &Account, priority: Priority) -> bool {
        let inserted =
            Self::priority_set_impl(account, priority, &self.blocking, &mut self.priorities);
        self.trim_overflow();
        inserted
    }

    fn priority_set_impl(
        account: &Account,
        priority: Priority,
        blocking: &OrderedBlocking,
        priorities: &mut PriorityContainer,
    ) -> bool {
        if account.is_zero() {
            return false;
        }

        if !blocking.contains(account) && !priorities.contains(account) {
            priorities.insert(PriorityEntry::new(*account, priority));
            true
        } else {
            false
        }
    }

    pub fn priority_erase(&mut self, account: &Account) -> bool {
        if account.is_zero() {
            return false;
        }

        self.priorities.remove(account).is_some()
    }

    pub fn block(&mut self, account: Account, dependency: BlockHash) -> bool {
        debug_assert!(!account.is_zero());

        let removed = self.priorities.remove(&account);

        if removed.is_some() {
            self.blocking.insert(BlockingEntry {
                account,
                dependency,
                dependency_account: Account::zero(),
            });

            self.trim_overflow();
            true
        } else {
            false
        }
    }

    pub fn unblock(&mut self, account: Account, hash: Option<BlockHash>) -> bool {
        if account.is_zero() {
            return false;
        }

        // Unblock only if the dependency is fulfilled
        if let Some(existing) = self.blocking.get(&account) {
            let hash_matches = if let Some(hash) = hash {
                hash == existing.dependency
            } else {
                true
            };

            if hash_matches {
                debug_assert!(!self.priorities.contains(&account));
                self.priorities
                    .insert(PriorityEntry::new(account, Self::PRIORITY_INITIAL));
                self.blocking.remove(&account);
                self.trim_overflow();
                return true;
            }
        }

        false
    }

    pub fn timestamp_set(&mut self, account: &Account, now: Timestamp) {
        debug_assert!(!account.is_zero());
        self.priorities.change_timestamp(account, Some(now));
    }

    pub fn timestamp_reset(&mut self, account: &Account) {
        debug_assert!(!account.is_zero());

        self.priorities.change_timestamp(account, None);
    }

    /// Sets information about the account chain that contains the block hash
    pub fn dependency_update(
        &mut self,
        dependency: &BlockHash,
        dependency_account: Account,
    ) -> usize {
        debug_assert!(!dependency_account.is_zero());
        let updated = self
            .blocking
            .modify_dependency_account(dependency, dependency_account);
        updated
    }

    /// Erase the oldest entries
    fn trim_overflow(&mut self) {
        while !self.priorities.is_empty() && self.priorities.len() > self.config.priorities_max {
            self.priorities.pop_lowest_prio();
        }
        while self.blocking.len() > self.config.blocking_max {
            self.blocking.pop_oldest();
        }
    }

    /// Sampling
    pub fn next_priority(
        &self,
        now: Timestamp,
        filter: impl Fn(&Account) -> bool,
    ) -> PriorityResult {
        if self.priorities.is_empty() {
            return Default::default();
        }

        let cutoff = now - self.config.cooldown;

        let Some(entry) = self.priorities.next_priority(cutoff, filter) else {
            return Default::default();
        };

        PriorityResult {
            account: entry.account,
            priority: entry.priority,
            fails: entry.fails,
        }
    }

    pub fn next_blocking(&self, filter: impl Fn(&BlockHash) -> bool) -> BlockHash {
        if self.blocking.len() == 0 {
            return BlockHash::zero();
        }

        self.blocking.next(filter).unwrap_or_default()
    }

    /// Sets information about the account chain that contains the block hash
    pub fn sync_dependencies(&mut self) -> (usize, usize) {
        let mut inserted = 0;
        let mut insert_failed = 0;

        // Sample all accounts with a known dependency account (> account 0)
        let begin = Account::zero().inc().unwrap();
        for entry in self.blocking.iter_start_dep_account(begin) {
            if self.priorities.len() >= self.config.priorities_max {
                break;
            }

            if !self.blocked(&entry.dependency_account)
                && !self.prioritized(&entry.dependency_account)
            {
                if Self::priority_set_impl(
                    &entry.dependency_account,
                    Self::PRIORITY_INITIAL,
                    &self.blocking,
                    &mut self.priorities,
                ) {
                    inserted += 1;
                } else {
                    insert_failed += 1;
                }
            }
        }

        self.trim_overflow();
        (inserted, insert_failed)
    }

    pub fn blocked(&self, account: &Account) -> bool {
        self.blocking.contains(account)
    }

    pub fn prioritized(&self, account: &Account) -> bool {
        self.priorities.contains(account)
    }

    pub fn priority_len(&self) -> usize {
        self.priorities.len()
    }

    pub fn blocked_len(&self) -> usize {
        self.blocking.len()
    }

    pub fn priority_half_full(&self) -> bool {
        self.priorities.len() > self.config.priorities_max / 2
    }

    pub fn blocked_half_full(&self) -> bool {
        self.blocking.len() > self.config.blocking_max / 2
    }

    /// Accounts in the ledger but not in priority list are assumed priority 1.0f
    /// Blocked accounts are assumed priority 0.0f
    #[allow(dead_code)]
    pub fn priority(&self, account: &Account) -> Priority {
        if !self.blocked(account) {
            if let Some(existing) = self.priorities.get(account) {
                return existing.priority;
            }
        }
        return Priority::ZERO;
    }

    pub fn container_info(&self) -> ContainerInfo {
        // Count blocking entries with their dependency account unknown
        let blocking_unknown = self.blocking.count_by_dependency_account(&Account::zero());
        [
            (
                "priorities",
                self.priorities.len(),
                PriorityContainer::ELEMENT_SIZE,
            ),
            (
                "blocking",
                self.blocking.len(),
                OrderedBlocking::ELEMENT_SIZE,
            ),
            ("blocking_unknown", blocking_unknown, 0),
        ]
        .into()
    }
}

impl Default for AccountSets {
    fn default() -> Self {
        Self::new(Default::default())
    }
}

#[derive(Default)]
pub struct PriorityResult {
    pub account: Account,
    pub priority: Priority,
    pub fails: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_blocked() {
        let sets = AccountSets::default();
        assert_eq!(sets.blocked(&Account::from(1)), false);
    }

    #[test]
    fn block() {
        let mut sets = AccountSets::default();
        let account = Account::from(1);
        let hash = BlockHash::from(2);
        sets.priority_up(&account);

        sets.block(account, hash);

        assert!(sets.blocked(&account));
        assert_eq!(sets.priority(&account), Priority::ZERO);
    }

    #[test]
    fn unblock() {
        let mut sets = AccountSets::default();
        let account = Account::from(1);
        let hash = BlockHash::from(2);
        sets.priority_up(&account);

        sets.block(account, hash);

        assert!(sets.unblock(account, None));
        assert_eq!(sets.blocked(&account), false);
    }

    #[test]
    fn priority_base() {
        let sets = AccountSets::default();
        assert_eq!(sets.priority(&Account::from(1)), Priority::ZERO);
    }

    #[test]
    fn priority_unblock() {
        let mut sets = AccountSets::default();
        let account = Account::from(1);
        let hash = BlockHash::from(2);

        assert_eq!(sets.priority_up(&account), PriorityUpResult::Inserted);
        assert_eq!(sets.priority(&account), AccountSets::PRIORITY_INITIAL);

        sets.block(account, hash);
        sets.unblock(account, None);

        assert_eq!(sets.priority(&account), AccountSets::PRIORITY_INITIAL);
    }

    #[test]
    fn priority_up_down() {
        let mut sets = AccountSets::default();
        let account = Account::from(1);

        sets.priority_up(&account);
        assert_eq!(sets.priority(&account), AccountSets::PRIORITY_INITIAL);

        sets.priority_down(&account);
        assert_eq!(
            sets.priority(&account),
            AccountSets::PRIORITY_INITIAL / AccountSets::PRIORITY_DIVIDE
        );
    }

    #[test]
    fn priority_down_empty() {
        let mut sets = AccountSets::default();
        let account = Account::from(1);

        sets.priority_down(&account);

        assert_eq!(sets.priority(&account), Priority::ZERO);
    }

    // Ensure priority value is bounded
    #[test]
    fn saturate_priority() {
        let mut sets = AccountSets::default();
        let account = Account::from(1);

        for _ in 0..100 {
            sets.priority_up(&account);
        }
        assert_eq!(sets.priority(&account), AccountSets::PRIORITY_MAX);
    }

    #[test]
    fn priority_down_saturate() {
        let mut sets = AccountSets::default();
        let account = Account::from(1);
        sets.priority_up(&account);
        assert_eq!(sets.priority(&account), AccountSets::PRIORITY_INITIAL);
        for _ in 0..10 {
            sets.priority_down(&account);
        }
        assert_eq!(sets.prioritized(&account), false);
    }

    #[test]
    fn priority_set() {
        let mut sets = AccountSets::default();
        let account = Account::from(1);
        let prio = Priority::new(10.0);
        sets.priority_set(&account, prio);
        assert_eq!(sets.priority(&account), prio);
    }
}
