//! Ordered weighted map with order-statistic ranks (#3505).
//!
//! A sorted sequence of bounded chunks. Each chunk records how many of its
//! entries carry a positive weight, so the number of positive entries before
//! a key costs one chunk-prefix sum plus a search inside one chunk, instead of
//! a walk over every preceding entry. Iteration stays a contiguous scan.
//! Purely in-memory: this has no durable encoding.

use std::borrow::Borrow;

/// Upper bound on a chunk's entries; a full chunk splits in half.
const MAX_CHUNK: usize = 256;

#[derive(Clone, Debug)]
pub(super) struct CountedMap<K> {
    chunks: Vec<Chunk<K>>,
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
            len: 0,
        }
    }
}

impl<K: Ord> CountedMap<K> {
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.len
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
        }
        Some(weight)
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
        let before: usize = self.chunks[..chunk_index]
            .iter()
            .map(|chunk| chunk.positive)
            .sum();
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
}
