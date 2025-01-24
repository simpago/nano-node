use rsnano_core::{Account, BlockHash, HashOrAccount};
use rsnano_nullable_clock::Timestamp;
use std::{
    collections::{HashMap, VecDeque},
    mem::size_of,
    time::Duration,
};

use crate::stats::DetailType;

#[derive(Default, PartialEq, Eq, Debug, Clone, Copy)]
pub(crate) enum QueryType {
    #[default]
    Invalid,
    BlocksByHash,
    BlocksByAccount,
    AccountInfoByHash,
    Frontiers,
}

impl From<QueryType> for DetailType {
    fn from(value: QueryType) -> Self {
        match value {
            QueryType::Invalid => DetailType::Invalid,
            QueryType::BlocksByHash => DetailType::BlocksByHash,
            QueryType::BlocksByAccount => DetailType::BlocksByAccount,
            QueryType::AccountInfoByHash => DetailType::AccountInfoByHash,
            QueryType::Frontiers => DetailType::Frontiers,
        }
    }
}

#[derive(Default, PartialEq, Eq, Debug, Clone, Copy)]
pub(crate) enum QuerySource {
    #[default]
    Invalid,
    Priority,
    Database,
    Dependencies,
    Frontiers,
}

/// Information about a running query that hasn't been responded yet
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RunningQuery {
    pub query_type: QueryType,
    pub source: QuerySource,
    pub start: HashOrAccount,
    pub account: Account,
    pub hash: BlockHash,
    pub count: usize,
    pub cutoff: Timestamp,
    pub timestamp: Timestamp,
    pub id: u64,
}

impl RunningQuery {
    #[allow(dead_code)]
    pub fn new_test_instance() -> Self {
        Self {
            query_type: QueryType::BlocksByHash,
            source: QuerySource::Database,
            start: HashOrAccount::from(1),
            account: Account::from(2),
            hash: BlockHash::from(3),
            count: 4,
            cutoff: Timestamp::new_test_instance() + Duration::from_secs(30),
            timestamp: Timestamp::new_test_instance(),
            id: 42,
        }
    }
}

#[derive(Default)]
pub(crate) struct RunningQueryContainer {
    by_id: HashMap<u64, RunningQuery>,
    by_account: HashMap<Account, Vec<u64>>,
    by_hash: HashMap<BlockHash, Vec<u64>>,
    sequenced: VecDeque<u64>,
}

static EMPTY_IDS: Vec<u64> = Vec::new();

impl RunningQueryContainer {
    pub const ELEMENT_SIZE: usize =
        size_of::<RunningQuery>() + size_of::<Account>() + size_of::<u64>() * 3;

    pub(crate) fn len(&self) -> usize {
        self.sequenced.len()
    }

    pub fn contains(&self, id: u64) -> bool {
        self.by_id.contains_key(&id)
    }

    #[allow(dead_code)]
    pub fn get(&self, id: u64) -> Option<&RunningQuery> {
        self.by_id.get(&id)
    }

    pub fn modify(&mut self, id: u64, mut f: impl FnMut(&mut RunningQuery)) -> bool {
        let Some(query) = self.by_id.get_mut(&id) else {
            return false;
        };

        let old_id = query.id;
        let old_account = query.account;
        let old_hash = query.hash;
        f(query);
        assert_eq!(query.id, old_id);
        assert_eq!(query.account, old_account);
        assert_eq!(query.hash, old_hash);

        true
    }

    pub fn count_by_account(&self, account: &Account, source: QuerySource) -> usize {
        self.iter_account(account)
            .filter(|i| i.source == source)
            .count()
    }

    pub fn iter_hash(&self, hash: &BlockHash) -> impl Iterator<Item = &RunningQuery> {
        self.iter_ids(self.by_hash.get(hash))
    }

    pub fn iter_account(&self, account: &Account) -> impl Iterator<Item = &RunningQuery> {
        self.iter_ids(self.by_account.get(account))
    }

    fn iter_ids<'a>(&'a self, ids: Option<&'a Vec<u64>>) -> impl Iterator<Item = &'a RunningQuery> {
        let ids = ids.unwrap_or(&EMPTY_IDS);
        ids.iter().map(|id| self.by_id.get(id).unwrap())
    }

    pub fn remove(&mut self, id: u64) -> Option<RunningQuery> {
        if let Some(tag) = self.by_id.remove(&id) {
            self.remove_by_account(id, &tag.account);
            self.remove_by_hash(id, &tag.hash);
            self.sequenced.retain(|i| *i != id);
            Some(tag)
        } else {
            None
        }
    }

    pub fn front(&self) -> Option<&RunningQuery> {
        self.sequenced.front().map(|id| self.by_id.get(id).unwrap())
    }

    pub fn pop_front(&mut self) -> Option<RunningQuery> {
        if let Some(id) = self.sequenced.pop_front() {
            let result = self.by_id.remove(&id).unwrap();
            self.remove_by_account(id, &result.account);
            self.remove_by_hash(id, &result.hash);
            Some(result)
        } else {
            None
        }
    }

    pub(crate) fn insert(&mut self, tag: RunningQuery) {
        let id = tag.id;
        let account = tag.account;
        let hash = tag.hash;
        if let Some(old) = self.by_id.insert(id, tag) {
            self.remove_internal(old.id, &old.account, &old.hash);
        }
        self.by_account.entry(account).or_default().push(id);
        self.by_hash.entry(hash).or_default().push(id);
        self.sequenced.push_back(id);
    }

    fn remove_internal(&mut self, id: u64, account: &Account, hash: &BlockHash) {
        self.by_id.remove(&id);
        self.remove_by_account(id, account);
        self.remove_by_hash(id, hash);
        self.sequenced.retain(|i| *i != id);
    }

    fn remove_by_account(&mut self, id: u64, account: &Account) {
        if let Some(ids) = self.by_account.get_mut(account) {
            if ids.len() == 1 {
                self.by_account.remove(account);
            } else {
                ids.retain(|i| *i != id)
            }
        }
    }
    fn remove_by_hash(&mut self, id: u64, hash: &BlockHash) {
        if let Some(ids) = self.by_hash.get_mut(hash) {
            if ids.len() == 1 {
                self.by_hash.remove(hash);
            } else {
                ids.retain(|i| *i != id)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty() {
        let mut container = RunningQueryContainer::default();
        assert_eq!(container.len(), 0);
        assert_eq!(container.contains(123), false);
        assert_eq!(container.get(123), None);
        assert_eq!(
            container.count_by_account(&Account::from(1), QuerySource::Priority),
            0
        );
        assert_eq!(container.iter_hash(&BlockHash::from(1)).next(), None);
        assert_eq!(container.iter_account(&Account::from(1)).next(), None);
        assert_eq!(container.front(), None);
        assert_eq!(container.pop_front(), None);
    }

    #[test]
    fn insert_one() {
        let mut container = RunningQueryContainer::default();
        let query = RunningQuery::new_test_instance();

        container.insert(query.clone());

        assert_eq!(container.len(), 1);
        assert_eq!(container.contains(query.id), true);
        assert_eq!(container.get(query.id), Some(&query));
        assert_eq!(container.count_by_account(&query.account, query.source), 1);
        assert_eq!(container.front(), Some(&query));
        assert_eq!(
            container
                .iter_hash(&query.hash)
                .cloned()
                .collect::<Vec<_>>(),
            vec![query.clone()]
        );
        assert_eq!(
            container
                .iter_account(&query.account)
                .cloned()
                .collect::<Vec<_>>(),
            vec![query]
        );
    }

    #[test]
    fn query_type_to_detail_type() {
        let expectations = [
            (QueryType::Invalid, DetailType::Invalid),
            (QueryType::BlocksByHash, DetailType::BlocksByHash),
            (QueryType::BlocksByAccount, DetailType::BlocksByAccount),
            (QueryType::AccountInfoByHash, DetailType::AccountInfoByHash),
            (QueryType::Frontiers, DetailType::Frontiers),
        ];

        for (qt, dt) in expectations {
            assert_eq!(DetailType::from(qt), dt);
        }
    }
}
