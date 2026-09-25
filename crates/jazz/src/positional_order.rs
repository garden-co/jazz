//! A keyed sequence with logarithmic positional edits.
//!
//! Maintained terminal outputs address roots both by identity (update,
//! remove, move) and by position (insert and move targets, and the public
//! `index`/`previous_index` delta contract). A `Vec` of identities makes every
//! identity lookup a linear scan, and a `BTreeMap<K, usize>` of positions must
//! shift every later position on each insertion. This order keeps an implicit
//! treap (subtree sizes, parent links) beside a key-to-node map so that an
//! identity's position, a positional insert and a removal each cost
//! O(log n), independent of the number of unchanged neighbours.
//!
//! Priorities come from a fixed-seed generator, so the shape (and hence all
//! observable behaviour, which never depends on shape) is deterministic.

use std::borrow::Borrow;
use std::collections::BTreeMap;

const NIL: u32 = u32::MAX;

#[derive(Clone, Debug)]
struct Node<K> {
    /// `None` marks an anonymous position: it occupies a place in the
    /// sequence but cannot be addressed by key.
    key: Option<K>,
    left: u32,
    right: u32,
    parent: u32,
    size: u32,
    priority: u64,
}

/// An ordered sequence of distinct keys addressed by key or by position.
#[derive(Clone, Debug)]
pub(crate) struct PositionalOrder<K: Ord + Clone> {
    slots: BTreeMap<K, u32>,
    nodes: Vec<Node<K>>,
    root: u32,
    seed: u64,
    anonymous: usize,
}

impl<K: Ord + Clone> Default for PositionalOrder<K> {
    fn default() -> Self {
        Self {
            slots: BTreeMap::new(),
            nodes: Vec::new(),
            root: NIL,
            seed: 0x9e37_79b9_7f4a_7c15,
            anonymous: 0,
        }
    }
}

impl<K: Ord + Clone> PositionalOrder<K> {
    /// Build from keys in sequence order. Returns the first duplicate key as
    /// an error: a sequence of identities must not silently collapse one.
    pub(crate) fn from_ordered(keys: impl IntoIterator<Item = K>) -> Result<Self, K> {
        let keys = keys.into_iter();
        let mut order = Self::default();
        let (lower, _) = keys.size_hint();
        order.nodes.reserve(lower + (lower / 8).max(64));
        for key in keys {
            let position = order.len();
            if !order.insert(position, key.clone()) {
                return Err(key);
            }
        }
        Ok(order)
    }

    pub(crate) fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Current position of `key`, in O(log n).
    pub(crate) fn position<Q>(&self, key: &Q) -> Option<usize>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let mut node = *self.slots.get(key)?;
        let mut rank = self.size(self.nodes[node as usize].left);
        loop {
            let parent = self.nodes[node as usize].parent;
            if parent == NIL {
                return Some(rank);
            }
            if self.nodes[parent as usize].right == node {
                rank += self.size(self.nodes[parent as usize].left) + 1;
            }
            node = parent;
        }
    }

    /// Number of anonymous positions (see [`Self::anonymize`]).
    pub(crate) fn anonymous_len(&self) -> usize {
        self.anonymous
    }

    /// Keep `key`'s position occupied but no longer addressable by `key`.
    /// Returns the position it occupies.
    pub(crate) fn anonymize<Q>(&mut self, key: &Q) -> Option<usize>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let position = self.position(key)?;
        let node = self.slots.remove(key).expect("positioned key has a slot");
        self.nodes[node as usize].key = None;
        self.anonymous += 1;
        Some(position)
    }

    /// Key at `position`, in O(log n); `None` when out of bounds or anonymous.
    pub(crate) fn get(&self, mut position: usize) -> Option<&K> {
        let mut node = self.root;
        while node != NIL {
            let current = &self.nodes[node as usize];
            let left = self.size(current.left);
            if position < left {
                node = current.left;
            } else if position == left {
                return current.key.as_ref();
            } else {
                position -= left + 1;
                node = current.right;
            }
        }
        None
    }

    /// Insert `key` before the element at `position` (clamped to the end).
    /// Returns false, leaving the order unchanged, if `key` is present.
    pub(crate) fn insert(&mut self, position: usize, key: K) -> bool {
        if self.slots.contains_key(&key) {
            return false;
        }
        let position = position.min(self.len());
        let node = u32::try_from(self.nodes.len()).expect("positional order exceeds u32 nodes");
        assert!(node != NIL, "positional order exceeds u32 nodes");
        self.seed = self.seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let priority = splitmix64(self.seed);
        self.slots.insert(key.clone(), node);
        self.nodes.push(Node {
            key: Some(key),
            left: NIL,
            right: NIL,
            parent: NIL,
            size: 1,
            priority,
        });
        let (before, after) = self.split(self.root, position);
        let joined = self.merge(before, node);
        self.root = self.merge(joined, after);
        self.nodes[self.root as usize].parent = NIL;
        true
    }

    /// Remove `key`, returning the position it occupied.
    pub(crate) fn remove<Q>(&mut self, key: &Q) -> Option<usize>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let position = self.position(key)?;
        let node = self.slots.remove(key).expect("positioned key has a slot");
        let (before, rest) = self.split(self.root, position);
        let (removed, after) = self.split(rest, 1);
        debug_assert_eq!(removed, node);
        self.root = self.merge(before, after);
        if self.root != NIL {
            self.nodes[self.root as usize].parent = NIL;
        }
        self.release(node);
        Some(position)
    }

    /// Keys in sequence order; `None` for an anonymous position.
    pub(crate) fn iter(&self) -> Iter<'_, K> {
        let mut iter = Iter {
            order: self,
            stack: Vec::new(),
        };
        iter.descend_left(self.root);
        iter
    }

    /// Keys in key order; cheaper than [`Self::iter`] when order is irrelevant.
    pub(crate) fn keys_unordered(&self) -> impl Iterator<Item = &K> {
        self.slots.keys()
    }

    fn size(&self, node: u32) -> usize {
        if node == NIL {
            0
        } else {
            self.nodes[node as usize].size as usize
        }
    }

    fn pull(&mut self, node: u32) {
        let (left, right) = {
            let current = &self.nodes[node as usize];
            (current.left, current.right)
        };
        self.nodes[node as usize].size = (1 + self.size(left) + self.size(right)) as u32;
        if left != NIL {
            self.nodes[left as usize].parent = node;
        }
        if right != NIL {
            self.nodes[right as usize].parent = node;
        }
    }

    /// Split `node`'s subtree into its first `count` elements and the rest.
    fn split(&mut self, node: u32, count: usize) -> (u32, u32) {
        if node == NIL {
            return (NIL, NIL);
        }
        let left = self.nodes[node as usize].left;
        let left_size = self.size(left);
        if count <= left_size {
            let (before, after) = self.split(left, count);
            self.nodes[node as usize].left = after;
            self.pull(node);
            (before, node)
        } else {
            let right = self.nodes[node as usize].right;
            let (before, after) = self.split(right, count - left_size - 1);
            self.nodes[node as usize].right = before;
            self.pull(node);
            (node, after)
        }
    }

    fn merge(&mut self, left: u32, right: u32) -> u32 {
        if left == NIL {
            return right;
        }
        if right == NIL {
            return left;
        }
        if self.nodes[left as usize].priority >= self.nodes[right as usize].priority {
            let child = self.nodes[left as usize].right;
            self.nodes[left as usize].right = self.merge(child, right);
            self.pull(left);
            left
        } else {
            let child = self.nodes[right as usize].left;
            self.nodes[right as usize].left = self.merge(left, child);
            self.pull(right);
            right
        }
    }

    /// Drop a detached node, keeping the arena dense by moving the last node
    /// into its slot and repairing the links that named the moved node.
    fn release(&mut self, node: u32) {
        let last = (self.nodes.len() - 1) as u32;
        self.nodes.swap_remove(node as usize);
        if node == last {
            return;
        }
        let (parent, left, right) = {
            let moved = &self.nodes[node as usize];
            (moved.parent, moved.left, moved.right)
        };
        if parent == NIL {
            if self.root == last {
                self.root = node;
            }
        } else {
            let parent = &mut self.nodes[parent as usize];
            if parent.left == last {
                parent.left = node;
            } else if parent.right == last {
                parent.right = node;
            }
        }
        if left != NIL {
            self.nodes[left as usize].parent = node;
        }
        if right != NIL {
            self.nodes[right as usize].parent = node;
        }
        if let Some(key) = self.nodes[node as usize].key.clone() {
            *self
                .slots
                .get_mut(&key)
                .expect("moved node retains its key slot") = node;
        }
    }
}

impl<K: Ord + Clone> std::ops::Index<usize> for PositionalOrder<K> {
    type Output = K;

    fn index(&self, position: usize) -> &K {
        self.get(position)
            .unwrap_or_else(|| panic!("position {position} out of bounds (len {})", self.len()))
    }
}

impl<K: Ord + Clone + PartialEq> PartialEq<Vec<K>> for PositionalOrder<K> {
    fn eq(&self, other: &Vec<K>) -> bool {
        self.len() == other.len() && self.iter().eq(other.iter().map(Some))
    }
}

pub(crate) struct Iter<'a, K: Ord + Clone> {
    order: &'a PositionalOrder<K>,
    stack: Vec<u32>,
}

impl<K: Ord + Clone> Iter<'_, K> {
    fn descend_left(&mut self, mut node: u32) {
        while node != NIL {
            self.stack.push(node);
            node = self.order.nodes[node as usize].left;
        }
    }
}

impl<'a, K: Ord + Clone> Iterator for Iter<'a, K> {
    type Item = Option<&'a K>;

    fn next(&mut self) -> Option<Option<&'a K>> {
        let node = self.stack.pop()?;
        let current = &self.order.nodes[node as usize];
        self.descend_left(current.right);
        Some(current.key.as_ref())
    }
}

fn splitmix64(state: u64) -> u64 {
    let mut z = state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

// Internal: the order's shape and rank bookkeeping are not observable through
// the public database API, which only sees the positions it reports. A
// deterministic differential test against a plain vector pins those positions
// for every edit kind, including arena compaction on removal.
#[cfg(test)]
mod tests {
    use super::PositionalOrder;

    fn check(order: &PositionalOrder<u32>, oracle: &[u32]) {
        assert_eq!(order.len(), oracle.len());
        assert!(
            order
                .iter()
                .map(|key| *key.unwrap())
                .eq(oracle.iter().copied())
        );
        for (position, key) in oracle.iter().enumerate() {
            assert_eq!(order.position(key), Some(position));
            assert_eq!(order.get(position), Some(key));
        }
        assert_eq!(order.get(oracle.len()), None);
    }

    #[test]
    fn positional_edits_match_a_vector_oracle() {
        let mut state = 0x1234_5678_u64;
        let mut next = move |bound: usize| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state % bound as u64) as usize
        };
        let mut order = PositionalOrder::default();
        let mut oracle = Vec::new();
        let mut fresh = 0_u32;
        for step in 0..4_000 {
            match next(4) {
                0 | 1 => {
                    let position = next(oracle.len() + 2);
                    assert!(order.insert(position, fresh));
                    oracle.insert(position.min(oracle.len()), fresh);
                    fresh += 1;
                }
                2 if !oracle.is_empty() => {
                    let key = oracle[next(oracle.len())];
                    let expected = oracle.iter().position(|k| *k == key).unwrap();
                    assert_eq!(order.remove(&key), Some(expected));
                    oracle.remove(expected);
                }
                _ if !oracle.is_empty() => {
                    let key = oracle[next(oracle.len())];
                    assert!(!order.insert(0, key), "duplicate insert must be refused");
                    let from = order.remove(&key).unwrap();
                    oracle.remove(from);
                    let to = next(oracle.len() + 1);
                    order.insert(to, key);
                    oracle.insert(to, key);
                }
                _ => {}
            }
            if step % 97 == 0 {
                check(&order, &oracle);
            }
        }
        check(&order, &oracle);
        assert_eq!(order.remove(&u32::MAX), None);
        let rebuilt = PositionalOrder::from_ordered(oracle.iter().copied()).unwrap();
        check(&rebuilt, &oracle);
        assert_eq!(PositionalOrder::from_ordered([1, 2, 1]).unwrap_err(), 1);

        let mut anonymized = PositionalOrder::from_ordered([7, 8, 9]).unwrap();
        assert_eq!(anonymized.anonymize(&8), Some(1));
        assert_eq!(anonymized.anonymous_len(), 1);
        assert_eq!(anonymized.position(&9), Some(2));
        assert_eq!(anonymized.get(1), None);
        assert!(anonymized.insert(1, 8));
        assert_eq!(anonymized.remove(&7), Some(0));
        assert_eq!(
            anonymized
                .iter()
                .map(|key| key.copied())
                .collect::<Vec<_>>(),
            vec![Some(8), None, Some(9)]
        );
    }
}
