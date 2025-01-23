use rsnano_core::{Account, BlockHash, HashOrAccount};
use rsnano_nullable_clock::Timestamp;
use std::{
    collections::{HashMap, VecDeque},
    mem::size_of,
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

#[derive(Default, PartialEq, Eq, Debug, Clone)]
pub(crate) enum QuerySource {
    #[default]
    Invalid,
    Priority,
    Database,
    Dependencies,
    Frontiers,
}

/// Information about a running query that hasn't been responded yet
#[derive(Clone)]
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

#[derive(Default)]
pub(crate) struct OrderedTags {
    by_id: HashMap<u64, RunningQuery>,
    by_account: HashMap<Account, Vec<u64>>,
    by_hash: HashMap<BlockHash, Vec<u64>>,
    sequenced: VecDeque<u64>,
}

static EMPTY_IDS: Vec<u64> = Vec::new();

impl OrderedTags {
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

    pub fn get_mut(&mut self, id: u64) -> Option<&mut RunningQuery> {
        self.by_id.get_mut(&id)
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
