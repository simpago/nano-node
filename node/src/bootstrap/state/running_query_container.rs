use super::running_query::{QuerySource, RunningQuery};
use rsnano_core::{Account, BlockHash};
use std::{
    collections::{HashMap, VecDeque},
    mem::size_of,
};

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

    #[allow(dead_code)]
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
        assert_eq!(query.id, old_id, "query id must not be changed");
        assert_eq!(query.account, old_account, "account must not be changed");
        assert_eq!(query.hash, old_hash, "hash must not be changed");

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

    pub(crate) fn insert(&mut self, query: RunningQuery) {
        let id = query.id;
        let account = query.account;
        let hash = query.hash;
        if let Some(old) = self.by_id.insert(id, query) {
            self.remove_internal(old.id, &old.account, &old.hash);
        }
        self.by_account.entry(account).or_default().push(id);
        self.by_hash.entry(hash).or_default().push(id);
        self.sequenced.push_back(id);
    }

    fn remove_internal(&mut self, id: u64, account: &Account, hash: &BlockHash) {
        self.remove_by_account(id, account);
        self.remove_by_hash(id, hash);
        self.sequenced.retain(|i| *i != id);
    }

    fn remove_by_account(&mut self, id: u64, account: &Account) {
        match self.by_account.get_mut(account) {
            Some(ids) => {
                if ids.len() == 1 {
                    self.by_account.remove(account);
                } else {
                    ids.retain(|i| *i != id)
                }
            }
            None => unreachable!(), // The account entry must exist
        }
    }
    fn remove_by_hash(&mut self, id: u64, hash: &BlockHash) {
        match self.by_hash.get_mut(hash) {
            Some(ids) => {
                if ids.len() == 1 {
                    self.by_hash.remove(hash);
                } else {
                    ids.retain(|i| *i != id)
                }
            }
            None => unreachable!(), // The hash entry must exist
        }
    }

    #[allow(dead_code)]
    pub fn clear(&mut self) {
        self.by_id.clear();
        self.by_account.clear();
        self.by_hash.clear();
        self.sequenced.clear();
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rsnano_nullable_clock::Timestamp;

    use crate::{bootstrap::state::QueryType, stats::DetailType};

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
    fn insert_two() {
        let mut container = RunningQueryContainer::default();
        let query1 = RunningQuery::new_test_instance();
        let query2 = RunningQuery {
            id: 999,
            ..RunningQuery::new_test_instance()
        };

        container.insert(query1.clone());
        container.insert(query2.clone());

        assert_eq!(container.len(), 2);
        assert_eq!(container.contains(query1.id), true);
        assert_eq!(container.contains(query2.id), true);
    }

    #[test]
    fn when_same_query_inserted_twice_should_replace_first_insert() {
        let mut container = RunningQueryContainer::default();
        let query_a = RunningQuery::new_test_instance();
        let query_b = RunningQuery {
            count: 99999,
            ..query_a
        };

        container.insert(query_a);
        container.insert(query_b.clone());

        assert_eq!(container.len(), 1);
        assert_eq!(container.get(query_b.id), Some(&query_b));
    }

    #[test]
    fn modify() {
        let mut container = RunningQueryContainer::default();
        let query = RunningQuery::new_test_instance();
        container.insert(query.clone());
        let new_cutoff = Timestamp::new_test_instance() + Duration::from_secs(999);
        let modified = container.modify(query.id, |q| q.response_cutoff = new_cutoff);
        assert!(modified);
        assert_eq!(
            container.get(query.id),
            Some(&RunningQuery {
                response_cutoff: new_cutoff,
                ..query
            })
        );
    }

    #[test]
    fn modify_non_existant() {
        let mut container = RunningQueryContainer::default();
        let modified = container.modify(123, |_| unreachable!());
        assert!(!modified);
    }

    #[test]
    #[should_panic]
    fn modify_panics_when_account_changed() {
        let mut container = RunningQueryContainer::default();
        let query = RunningQuery::new_test_instance();
        container.insert(query.clone());

        container.modify(query.id, |q| q.account = Account::from(1000));
    }

    #[test]
    #[should_panic]
    fn modify_panics_when_hash_changed() {
        let mut container = RunningQueryContainer::default();
        let query = RunningQuery::new_test_instance();
        container.insert(query.clone());

        container.modify(query.id, |q| q.hash = BlockHash::from(1000));
    }

    #[test]
    #[should_panic]
    fn modify_panics_when_id_changed() {
        let mut container = RunningQueryContainer::default();
        let query = RunningQuery::new_test_instance();
        container.insert(query.clone());

        container.modify(query.id, |q| q.id = 1000);
    }

    #[test]
    fn remove() {
        let mut container = RunningQueryContainer::default();
        let query = RunningQuery::new_test_instance();
        container.insert(query.clone());

        container.remove(query.id);

        assert_eq!(container.len(), 0);
        assert_eq!(container.sequenced.len(), 0);
        assert_eq!(container.by_id.len(), 0);
        assert_eq!(container.by_hash.len(), 0);
        assert_eq!(container.by_account.len(), 0);
    }

    #[test]
    fn remove_none() {
        let mut container = RunningQueryContainer::default();
        container.remove(123);
        assert_eq!(container.len(), 0);
    }

    #[test]
    fn remove_one_of_two_queries_with_same_hash() {
        let mut container = RunningQueryContainer::default();
        let query_a = RunningQuery::new_test_instance();
        let query_b = RunningQuery {
            id: 999,
            account: Account::from(999),
            ..query_a
        };
        container.insert(query_a.clone());
        container.insert(query_b.clone());

        container.remove(query_a.id);

        assert_eq!(container.len(), 1);
        assert_eq!(
            container.iter_hash(&query_b.hash).collect::<Vec<_>>(),
            vec![&query_b]
        )
    }

    #[test]
    fn remove_one_of_two_queries_with_same_account() {
        let mut container = RunningQueryContainer::default();
        let query_a = RunningQuery::new_test_instance();
        let query_b = RunningQuery {
            id: 999,
            hash: BlockHash::from(999),
            ..query_a
        };
        container.insert(query_a.clone());
        container.insert(query_b.clone());

        container.remove(query_a.id);

        assert_eq!(container.len(), 1);
        assert_eq!(
            container.iter_account(&query_b.account).collect::<Vec<_>>(),
            vec![&query_b]
        )
    }

    #[test]
    fn pop_front_the_only_entry() {
        let mut container = RunningQueryContainer::default();
        let query = RunningQuery::new_test_instance();
        container.insert(query.clone());

        assert_eq!(container.front(), Some(&query));
        let popped = container.pop_front();

        assert_eq!(container.len(), 0);
        assert_eq!(popped, Some(query));
    }

    #[test]
    fn pop_front_with_multiple_entries() {
        let mut container = RunningQueryContainer::default();
        let query_a = RunningQuery::new_test_instance();
        let query_b = RunningQuery {
            id: 1000,
            ..RunningQuery::new_test_instance()
        };
        let query_c = RunningQuery {
            id: 2000,
            ..RunningQuery::new_test_instance()
        };
        container.insert(query_a.clone());
        container.insert(query_b.clone());
        container.insert(query_c.clone());

        assert_eq!(container.front(), Some(&query_a));
        let popped = container.pop_front();

        assert_eq!(container.len(), 2);
        assert_eq!(popped, Some(query_a));
        assert_eq!(container.front(), Some(&query_b));
    }

    #[test]
    #[should_panic]
    fn remove_by_account_panics_when_account_not_found() {
        let mut container = RunningQueryContainer::default();
        container.remove_by_account(123, &Account::from(1000));
    }

    #[test]
    #[should_panic]
    fn remove_by_hash_panics_when_hash_not_found() {
        let mut container = RunningQueryContainer::default();
        container.remove_by_hash(123, &BlockHash::from(1000));
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
