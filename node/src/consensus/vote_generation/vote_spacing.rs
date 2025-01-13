#[cfg(test)]
use mock_instant::thread_local::Instant;
use rsnano_core::{BlockHash, Root};
#[cfg(not(test))]
use std::time::Instant;
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    time::Duration,
};

pub struct VoteSpacing {
    delay: Duration,
    recent: EntryContainer,
}

impl VoteSpacing {
    pub fn new(delay: Duration) -> Self {
        Self {
            recent: EntryContainer::new(),
            delay,
        }
    }

    pub fn votable(&self, root: &Root, hash: &BlockHash) -> bool {
        self.recent
            .by_root(root)
            .all(|item| *hash == item.hash || item.time.elapsed() >= self.delay)
    }

    pub fn flag(&mut self, root: &Root, hash: &BlockHash) {
        self.trim();
        let time = Instant::now();
        if !self.recent.change_time_for_root(root, time) {
            self.recent.insert(Entry {
                root: *root,
                hash: *hash,
                time,
            });
        }
    }

    pub fn len(&self) -> usize {
        self.recent.len()
    }

    pub fn is_empty(&self) -> bool {
        self.recent.is_empty()
    }

    fn trim(&mut self) {
        self.recent.trim(self.delay);
    }
}

struct Entry {
    root: Root,
    hash: BlockHash,
    time: Instant,
}

#[derive(Default)]
struct EntryContainer {
    entries: HashMap<usize, Entry>,
    by_root: HashMap<Root, HashSet<usize>>,
    by_time: BTreeMap<Instant, Vec<usize>>,
    next_id: usize,
    empty_id_set: HashSet<usize>,
}

impl EntryContainer {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn insert(&mut self, entry: Entry) {
        let id = self.create_id();

        let by_root = self.by_root.entry(entry.root).or_default();
        by_root.insert(id);

        let by_time = self.by_time.entry(entry.time).or_default();
        by_time.push(id);

        self.entries.insert(id, entry);
    }

    fn create_id(&mut self) -> usize {
        let id = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        id
    }

    pub fn by_root(&self, root: &Root) -> impl Iterator<Item = &Entry> + '_ {
        match self.by_root.get(root) {
            Some(ids) => self.iter_entries(ids),
            None => self.iter_entries(&self.empty_id_set),
        }
    }

    fn iter_entries<'a>(&'a self, ids: &'a HashSet<usize>) -> impl Iterator<Item = &'a Entry> + 'a {
        ids.iter().map(|&id| &self.entries[&id])
    }

    fn trim(&mut self, upper_bound: Duration) {
        let mut instants_to_remove = Vec::new();
        for (&instant, ids) in self.by_time.iter() {
            if instant.elapsed() < upper_bound {
                break;
            }

            instants_to_remove.push(instant);

            for id in ids {
                let entry = self.entries.remove(id).unwrap();

                let by_root = self.by_root.get_mut(&entry.root).unwrap();
                by_root.remove(id);
                if by_root.is_empty() {
                    self.by_root.remove(&entry.root);
                }
            }
        }

        for instant in instants_to_remove {
            self.by_time.remove(&instant);
        }
    }

    fn change_time_for_root(&mut self, root: &Root, time: Instant) -> bool {
        match self.by_root.get(root) {
            Some(ids) => {
                change_time_for_entries(ids, time, &mut self.entries, &mut self.by_time);
                true
            }
            None => false,
        }
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

fn change_time_for_entries(
    ids: &HashSet<usize>,
    time: Instant,
    entries: &mut HashMap<usize, Entry>,
    by_time: &mut BTreeMap<Instant, Vec<usize>>,
) {
    for id in ids {
        change_time_for_entry(id, time, entries, by_time);
    }
}

fn change_time_for_entry(
    id: &usize,
    time: Instant,
    entries: &mut HashMap<usize, Entry>,
    by_time: &mut BTreeMap<Instant, Vec<usize>>,
) {
    if let Some(entry) = entries.get_mut(id) {
        let old_time = entry.time;
        entry.time = time;
        remove_from_time_index(old_time, id, by_time);
        by_time.entry(time).or_default().push(*id);
    }
}

fn remove_from_time_index(
    time: Instant,
    id: &usize,
    ids_by_time: &mut BTreeMap<Instant, Vec<usize>>,
) {
    if let Some(ids) = ids_by_time.get_mut(&time) {
        if ids.len() == 1 {
            ids_by_time.remove(&time);
        } else {
            ids.retain(|x| x != id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mock_instant::thread_local::MockClock;

    #[test]
    fn empty() {
        let spacing = VoteSpacing::new(Duration::from_millis(100));
        assert_eq!(spacing.len(), 0);
        assert!(spacing.votable(&Root::from(1), &BlockHash::from(2)));
    }

    #[test]
    fn flag() {
        let mut spacing = VoteSpacing::new(Duration::from_millis(100));
        let root1 = Root::from(1);
        let root2 = Root::from(2);
        let hash1 = BlockHash::from(3);
        let hash2 = BlockHash::from(4);
        let hash3 = BlockHash::from(5);

        spacing.flag(&root1, &hash1);
        assert_eq!(spacing.len(), 1);
        assert!(spacing.votable(&root1, &hash1));
        assert!(!spacing.votable(&root1, &hash2));

        spacing.flag(&root2, &hash3);
        assert_eq!(spacing.len(), 2);
    }

    #[test]
    fn prune() {
        let length = Duration::from_millis(100);
        let mut spacing = VoteSpacing::new(length);
        spacing.flag(&Root::from(1), &BlockHash::from(3));
        assert_eq!(spacing.len(), 1);

        MockClock::advance(length);
        spacing.flag(&Root::from(2), &BlockHash::from(4));
        assert_eq!(spacing.len(), 1);
    }

    mod entry_container_tests {
        use super::*;

        #[test]
        fn trim() {
            let mut container = EntryContainer::new();
            container.insert(Entry {
                root: Root::from(1),
                hash: BlockHash::from(2),
                time: Instant::now(),
            });
            MockClock::advance(Duration::from_secs(10));
            container.trim(Duration::from_secs(5));
            assert_eq!(container.len(), 0);
            assert_eq!(container.by_time.len(), 0);
            assert_eq!(container.entries.len(), 0);
            assert_eq!(container.by_root.len(), 0);
        }
    }
}
