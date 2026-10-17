//! Ordered weighted map with order-statistic ranks (#3505).
//!
//! A sorted sequence of chunks holding between `MIN_CHUNK` and `MAX_CHUNK`
//! entries (a lone chunk may hold fewer). Each chunk records how many of its
//! entries carry a positive weight, and a Fenwick tree over those counts
//! gives the positive entries before any chunk in O(log chunks). So the rank
//! of a key costs O(log n) plus a scan inside one bounded chunk, instead of a
//! walk over every preceding entry. Edits touch one chunk; a split or merge
//! rebuilds the chunk list and the tree in O(n / MIN_CHUNK), which happens at
//! most once per `MIN_CHUNK` edits to that chunk. Iteration stays a contiguous
//! scan. Purely in-memory: this has no durable encoding.

use std::borrow::Borrow;

/// Upper bound on a chunk's entries; a full chunk splits in half.
const MAX_CHUNK: usize = 256;
/// A chunk below this merges into a neighbour, so heavy deletes cannot leave
/// a long run of near-empty chunks.
const MIN_CHUNK: usize = MAX_CHUNK / 4;

#[derive(Clone, Debug)]
pub(super) struct CountedMap<K> {
    chunks: Vec<Chunk<K>>,
    /// Fenwick tree over `chunks[i].positive`, one-based.
    positive_tree: Vec<usize>,
    len: usize,
}

#[derive(Clone, Debug)]
struct Chunk<K> {
    entries: Vec<(K, i64)>,
    positive: usize,
}

impl<K> Default for CountedMap<K> {
    fn default() -> Self {
        Self {
            chunks: Vec::new(),
            positive_tree: vec![0],
            len: 0,
        }
    }
}

impl<K: Ord> CountedMap<K> {
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.len
    }

    fn rebuild_positive_tree(&mut self) {
        let count = self.chunks.len();
        self.positive_tree.clear();
        self.positive_tree.resize(count + 1, 0);
        for index in 1..=count {
            self.positive_tree[index] += self.chunks[index - 1].positive;
            let parent = index + (index & index.wrapping_neg());
            if parent <= count {
                self.positive_tree[parent] += self.positive_tree[index];
            }
        }
    }

    fn adjust_positive(&mut self, chunk_index: usize, gained: bool, lost: bool) {
        if gained == lost {
            return;
        }
        let mut index = chunk_index + 1;
        while index < self.positive_tree.len() {
            if gained {
                self.positive_tree[index] += 1;
            } else {
                self.positive_tree[index] -= 1;
            }
            index += index & index.wrapping_neg();
        }
    }

    /// Positive entries in `chunks[..chunk_index]`.
    fn positive_in_chunks_before(&self, chunk_index: usize) -> usize {
        let mut total = 0;
        let mut index = chunk_index;
        while index > 0 {
            total += self.positive_tree[index];
            index &= index - 1;
        }
        total
    }

    /// The chunk that holds `key` or would receive it.
    fn chunk_for<Q: Ord + ?Sized>(&self, key: &Q) -> usize
    where
        K: Borrow<Q>,
    {
        let index = self.chunks.partition_point(|chunk| {
            chunk
                .entries
                .last()
                .expect("chunks are never empty")
                .0
                .borrow()
                < key
        });
        index.min(self.chunks.len().saturating_sub(1))
    }

    pub(super) fn get<Q: Ord + ?Sized>(&self, key: &Q) -> Option<&i64>
    where
        K: Borrow<Q>,
    {
        let chunk = self.chunks.get(self.chunk_for(key))?;
        chunk
            .entries
            .binary_search_by(|(candidate, _)| candidate.borrow().cmp(key))
            .ok()
            .map(|index| &chunk.entries[index].1)
    }

    pub(super) fn insert(&mut self, key: K, weight: i64) -> Option<i64> {
        if self.chunks.is_empty() {
            self.chunks.push(Chunk {
                entries: vec![(key, weight)],
                positive: usize::from(weight > 0),
            });
            self.len = 1;
            self.rebuild_positive_tree();
            return None;
        }
        let chunk_index = self.chunk_for(&key);
        let chunk = &mut self.chunks[chunk_index];
        match chunk
            .entries
            .binary_search_by(|(candidate, _)| candidate.cmp(&key))
        {
            Ok(index) => {
                let old = std::mem::replace(&mut chunk.entries[index].1, weight);
                chunk.positive = chunk.positive - usize::from(old > 0) + usize::from(weight > 0);
                self.adjust_positive(chunk_index, weight > 0, old > 0);
                Some(old)
            }
            Err(index) => {
                chunk.entries.insert(index, (key, weight));
                chunk.positive += usize::from(weight > 0);
                self.len += 1;
                if chunk.entries.len() > MAX_CHUNK {
                    let tail = chunk.entries.split_off(chunk.entries.len() / 2);
                    let tail_positive = tail.iter().filter(|(_, weight)| *weight > 0).count();
                    chunk.positive -= tail_positive;
                    self.chunks.insert(
                        chunk_index + 1,
                        Chunk {
                            entries: tail,
                            positive: tail_positive,
                        },
                    );
                    self.rebuild_positive_tree();
                } else {
                    self.adjust_positive(chunk_index, weight > 0, false);
                }
                None
            }
        }
    }

    pub(super) fn remove<Q: Ord + ?Sized>(&mut self, key: &Q) -> Option<i64>
    where
        K: Borrow<Q>,
    {
        if self.chunks.is_empty() {
            return None;
        }
        let chunk_index = self.chunk_for(key);
        let chunk = &mut self.chunks[chunk_index];
        let index = chunk
            .entries
            .binary_search_by(|(candidate, _)| candidate.borrow().cmp(key))
            .ok()?;
        let (_, weight) = chunk.entries.remove(index);
        chunk.positive -= usize::from(weight > 0);
        self.len -= 1;
        if chunk.entries.is_empty() {
            self.chunks.remove(chunk_index);
            self.rebuild_positive_tree();
        } else if chunk.entries.len() < MIN_CHUNK && self.chunks.len() > 1 {
            self.merge_into_neighbour(chunk_index);
            self.rebuild_positive_tree();
        } else {
            self.adjust_positive(chunk_index, false, weight > 0);
        }
        Some(weight)
    }

    /// Folds an undersized chunk into its next neighbour (or its previous one
    /// at the end), splitting the result again if it overflows.
    fn merge_into_neighbour(&mut self, chunk_index: usize) {
        let left = if chunk_index + 1 < self.chunks.len() {
            chunk_index
        } else {
            chunk_index - 1
        };
        let right = self.chunks.remove(left + 1);
        let merged = &mut self.chunks[left];
        merged.entries.extend(right.entries);
        merged.positive += right.positive;
        if merged.entries.len() > MAX_CHUNK {
            let tail = merged.entries.split_off(merged.entries.len() / 2);
            let tail_positive = tail.iter().filter(|(_, weight)| *weight > 0).count();
            merged.positive -= tail_positive;
            self.chunks.insert(
                left + 1,
                Chunk {
                    entries: tail,
                    positive: tail_positive,
                },
            );
        }
    }

    /// Entries with a positive weight whose key is strictly below `key`.
    pub(super) fn positive_before<Q: Ord + ?Sized>(&self, key: &Q) -> usize
    where
        K: Borrow<Q>,
    {
        if self.chunks.is_empty() {
            return 0;
        }
        let chunk_index = self.chunk_for(key);
        let before = self.positive_in_chunks_before(chunk_index);
        let chunk = &self.chunks[chunk_index];
        let within = chunk
            .entries
            .partition_point(|(candidate, _)| candidate.borrow() < key);
        before
            + if chunk.positive == chunk.entries.len() {
                within
            } else {
                chunk.entries[..within]
                    .iter()
                    .filter(|(_, weight)| *weight > 0)
                    .count()
            }
    }

    pub(super) fn iter(&self) -> Iter<'_, K> {
        Iter {
            chunks: self.chunks.iter(),
            current: [].iter(),
        }
    }
}

pub(super) struct Iter<'a, K> {
    chunks: std::slice::Iter<'a, Chunk<K>>,
    current: std::slice::Iter<'a, (K, i64)>,
}

impl<'a, K> Iterator for Iter<'a, K> {
    type Item = (&'a K, &'a i64);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some((key, weight)) = self.current.next() {
                return Some((key, weight));
            }
            self.current = self.chunks.next()?.entries.iter();
        }
    }
}

impl<K: Ord> FromIterator<(K, i64)> for CountedMap<K> {
    fn from_iter<I: IntoIterator<Item = (K, i64)>>(iter: I) -> Self {
        let mut map = Self::default();
        for (key, weight) in iter {
            map.insert(key, weight);
        }
        map
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn edits_and_ranks_match_an_ordered_map() {
        let mut map = CountedMap::default();
        let mut reference = BTreeMap::<u64, i64>::new();
        let mut state = 0x2545_f491_4f6c_dd1du64;
        for step in 0..20_000u64 {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let key = state % 2_000;
            if step % 5 == 0 {
                assert_eq!(map.remove(&key), reference.remove(&key));
            } else {
                let weight = (state % 7) as i64 - 2;
                if weight == 0 {
                    continue;
                }
                assert_eq!(map.insert(key, weight), reference.insert(key, weight));
            }
            if step % 97 == 0 {
                let probe = state % 2_100;
                let expected = reference
                    .range(..probe)
                    .filter(|(_, weight)| **weight > 0)
                    .count();
                assert_eq!(map.positive_before(&probe), expected);
                assert_eq!(map.get(&probe), reference.get(&probe));
            }
        }
        assert_eq!(map.len(), reference.len());
        assert!(map.iter().map(|(k, w)| (*k, *w)).eq(reference.into_iter()));
    }

    #[test]
    fn heavy_deletes_merge_chunks_and_keep_ranks() {
        let mut map: CountedMap<u64> = (0..10_000u64).map(|key| (key, 1)).collect();
        for key in (0..10_000u64).filter(|key| key % 50 != 0) {
            map.remove(&key);
        }
        assert_eq!(map.len(), 200);
        assert!(
            map.chunks.len() <= 200 / MIN_CHUNK + 1,
            "{}",
            map.chunks.len()
        );
        assert!(
            map.chunks
                .iter()
                .all(|chunk| chunk.entries.len() <= MAX_CHUNK)
        );
        for probe in [0, 1, 50, 51, 4_999, 5_000, 9_950, 10_000] {
            assert_eq!(map.positive_before(&probe), probe.div_ceil(50) as usize);
        }
        for key in (0..10_000u64).step_by(50) {
            map.remove(&key);
        }
        assert_eq!(map.len(), 0);
        assert_eq!(map.positive_before(&7), 0);
        map.insert(7, 1);
        assert_eq!(map.positive_before(&8), 1);
    }
}
