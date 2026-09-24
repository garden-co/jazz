//! In-memory ordered index with exact ranks and path-copying snapshots.
//!
//! AVL balancing bounds both comparisons and copied nodes per edit. Entries
//! have separate shared ownership, so copying a search path does not copy its
//! record/sort-key bytes. No randomized priorities or durable encoding.

use std::cmp::Ordering;
use std::rc::Rc;

#[derive(Debug)]
pub(super) struct RankIndex<K, V> {
    root: Link<K, V>,
}

type Link<K, V> = Option<Rc<Node<K, V>>>;

#[derive(Debug)]
struct Node<K, V> {
    entry: Rc<(K, V)>,
    left: Link<K, V>,
    right: Link<K, V>,
    height: usize,
    count: usize,
}

impl<K, V> Clone for Node<K, V> {
    fn clone(&self) -> Self {
        Self {
            entry: Rc::clone(&self.entry),
            left: self.left.clone(),
            right: self.right.clone(),
            height: self.height,
            count: self.count,
        }
    }
}

impl<K, V> Clone for RankIndex<K, V> {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
        }
    }
}

impl<K, V> Default for RankIndex<K, V> {
    fn default() -> Self {
        Self { root: None }
    }
}

fn height<K, V>(link: &Link<K, V>) -> usize {
    link.as_ref().map_or(0, |node| node.height)
}

fn count<K, V>(link: &Link<K, V>) -> usize {
    link.as_ref().map_or(0, |node| node.count)
}

impl<K, V> Node<K, V> {
    fn refresh(&mut self) {
        self.height = 1 + height(&self.left).max(height(&self.right));
        self.count = 1 + count(&self.left) + count(&self.right);
    }
}

fn rotate_left<K, V>(link: &mut Link<K, V>) {
    let mut root = link.take().expect("rotation has a root");
    let mut pivot = Rc::make_mut(&mut root)
        .right
        .take()
        .expect("right-heavy root");
    Rc::make_mut(&mut root).right = Rc::make_mut(&mut pivot).left.take();
    Rc::make_mut(&mut root).refresh();
    Rc::make_mut(&mut pivot).left = Some(root);
    Rc::make_mut(&mut pivot).refresh();
    *link = Some(pivot);
}

fn rotate_right<K, V>(link: &mut Link<K, V>) {
    let mut root = link.take().expect("rotation has a root");
    let mut pivot = Rc::make_mut(&mut root)
        .left
        .take()
        .expect("left-heavy root");
    Rc::make_mut(&mut root).left = Rc::make_mut(&mut pivot).right.take();
    Rc::make_mut(&mut root).refresh();
    Rc::make_mut(&mut pivot).right = Some(root);
    Rc::make_mut(&mut pivot).refresh();
    *link = Some(pivot);
}

fn balance<K, V>(link: &mut Link<K, V>) {
    let Some(root) = link.as_mut() else { return };
    let node = Rc::make_mut(root);
    node.refresh();
    if height(&node.left) > height(&node.right) + 1 {
        let left = node.left.as_ref().unwrap();
        if height(&left.right) > height(&left.left) {
            rotate_left(&mut node.left);
        }
        rotate_right(link);
    } else if height(&node.right) > height(&node.left) + 1 {
        let right = node.right.as_ref().unwrap();
        if height(&right.left) > height(&right.right) {
            rotate_right(&mut node.right);
        }
        rotate_left(link);
    }
}

fn insert<K: Ord, V>(link: &mut Link<K, V>, entry: Rc<(K, V)>) {
    let Some(root) = link.as_mut() else {
        *link = Some(Rc::new(Node {
            entry,
            left: None,
            right: None,
            height: 1,
            count: 1,
        }));
        return;
    };
    let node = Rc::make_mut(root);
    match entry.0.cmp(&node.entry.0) {
        Ordering::Less => insert(&mut node.left, entry),
        Ordering::Greater => insert(&mut node.right, entry),
        Ordering::Equal => {
            node.entry = entry;
            return;
        }
    }
    balance(link);
}

fn pop_first<K, V>(link: &mut Link<K, V>) -> Rc<(K, V)> {
    let node = Rc::make_mut(link.as_mut().expect("nonempty successor subtree"));
    if node.left.is_none() {
        let entry = Rc::clone(&node.entry);
        *link = node.right.take();
        entry
    } else {
        let entry = pop_first(&mut node.left);
        balance(link);
        entry
    }
}

fn remove<K: Ord, V>(link: &mut Link<K, V>, key: &K) {
    let Some(root) = link.as_mut() else { return };
    let node = Rc::make_mut(root);
    match key.cmp(&node.entry.0) {
        Ordering::Less => remove(&mut node.left, key),
        Ordering::Greater => remove(&mut node.right, key),
        Ordering::Equal => {
            if node.left.is_none() {
                *link = node.right.take();
                return;
            }
            if node.right.is_none() {
                *link = node.left.take();
                return;
            }
            node.entry = pop_first(&mut node.right);
        }
    }
    balance(link);
}

impl<K: Ord, V> RankIndex<K, V> {
    pub(super) fn insert(&mut self, key: K, value: V) {
        insert(&mut self.root, Rc::new((key, value)));
    }

    pub(super) fn remove(&mut self, key: &K) {
        remove(&mut self.root, key);
    }

    pub(super) fn contains(&self, key: &K) -> bool {
        let mut next = self.root.as_deref();
        while let Some(node) = next {
            match key.cmp(&node.entry.0) {
                Ordering::Less => next = node.left.as_deref(),
                Ordering::Greater => next = node.right.as_deref(),
                Ordering::Equal => return true,
            }
        }
        false
    }

    /// Number of keys strictly before `key`, whether or not it is present.
    pub(super) fn rank(&self, key: &K) -> usize {
        let mut rank = 0;
        let mut next = self.root.as_deref();
        while let Some(node) = next {
            match key.cmp(&node.entry.0) {
                Ordering::Less => next = node.left.as_deref(),
                Ordering::Equal => return rank + count(&node.left),
                Ordering::Greater => {
                    rank += count(&node.left) + 1;
                    next = node.right.as_deref();
                }
            }
        }
        rank
    }

    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        count(&self.root)
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    #[cfg(test)]
    pub(super) fn keys(&self) -> impl Iterator<Item = &K> {
        fn visit<'a, K, V>(link: &'a Link<K, V>, keys: &mut Vec<&'a K>) {
            if let Some(node) = link {
                visit(&node.left, keys);
                keys.push(&node.entry.0);
                visit(&node.right, keys);
            }
        }
        let mut keys = Vec::new();
        visit(&self.root, &mut keys);
        keys.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, HashSet};

    // Internal tests deliberately check the balanced-tree and structural-
    // sharing mechanism; public query equality cannot detect a full-tree copy.
    fn validate(link: &Link<u64, u64>, low: Option<u64>, high: Option<u64>) -> (usize, usize) {
        let Some(node) = link else { return (0, 0) };
        let key = node.entry.0;
        assert!(low.is_none_or(|low| low < key));
        assert!(high.is_none_or(|high| key < high));
        let (lh, lc) = validate(&node.left, low, Some(key));
        let (rh, rc) = validate(&node.right, Some(key), high);
        assert!(lh.abs_diff(rh) <= 1);
        assert_eq!(node.height, 1 + lh.max(rh));
        assert_eq!(node.count, 1 + lc + rc);
        (node.height, node.count)
    }

    fn entries(link: &Link<u64, u64>) -> Vec<(u64, u64)> {
        let Some(node) = link else { return Vec::new() };
        let mut result = entries(&node.left);
        result.push(*node.entry);
        result.extend(entries(&node.right));
        result
    }

    #[test]
    fn ranks_edits_and_retained_versions_match_ordered_map() {
        let mut index = RankIndex::default();
        let mut oracle = BTreeMap::new();
        let mut random = 0x12345678_u64;
        for step in 0..3000 {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            let key = random % 257;
            let before = index.clone();
            let expected_before = oracle.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>();
            if step % 3 == 0 {
                index.remove(&key);
                oracle.remove(&key);
            } else {
                index.insert(key, step);
                oracle.insert(key, step);
            }
            validate(&index.root, None, None);
            validate(&before.root, None, None);
            assert_eq!(entries(&before.root), expected_before);
            assert_eq!(
                entries(&index.root),
                oracle.iter().map(|(k, v)| (*k, *v)).collect::<Vec<_>>()
            );
            for probe in [0, key, key + 1, 300] {
                assert_eq!(index.rank(&probe), oracle.range(..probe).count());
                assert_eq!(index.contains(&probe), oracle.contains_key(&probe));
            }
        }
        for key in (0..257).rev() {
            index.remove(&key);
            validate(&index.root, None, None);
        }
        assert!(index.is_empty());
    }

    fn addresses<K, V>(link: &Link<K, V>, out: &mut HashSet<usize>) {
        if let Some(node) = link {
            out.insert(Rc::as_ptr(node) as usize);
            addresses(&node.left, out);
            addresses(&node.right, out);
        }
    }

    fn new_nodes<K, V>(link: &Link<K, V>, old: &HashSet<usize>) -> usize {
        match link {
            Some(node) if !old.contains(&(Rc::as_ptr(node) as usize)) => {
                1 + new_nodes(&node.left, old) + new_nodes(&node.right, old)
            }
            _ => 0,
        }
    }

    #[test]
    fn monotonic_growth_and_snapshot_edits_copy_only_balanced_paths() {
        for size in [128_u64, 32_768] {
            let mut original = RankIndex::default();
            for key in 0..size {
                original.insert(key, key);
            }
            let (height, count) = validate(&original.root, None, None);
            assert_eq!(count, size as usize);
            assert!(height <= 2 * (size.ilog2() as usize + 1));
            let mut old = HashSet::new();
            addresses(&original.root, &mut old);
            for key in [0, size / 2, size - 1, size] {
                let mut staged = original.clone();
                assert!(Rc::ptr_eq(
                    staged.root.as_ref().unwrap(),
                    original.root.as_ref().unwrap()
                ));
                staged.insert(key, size + 1);
                assert!(new_nodes(&staged.root, &old) <= 3 * (height + 1));
                staged.remove(&(size / 3));
                assert!(new_nodes(&staged.root, &old) <= 6 * (height + 1));
                validate(&staged.root, None, None);
                assert_eq!(original.len(), size as usize);
                assert!(original.contains(&(size / 3)));
                assert!(!original.contains(&size));
            }
        }
    }
}
