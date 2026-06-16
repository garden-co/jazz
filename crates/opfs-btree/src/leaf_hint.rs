//! Small most-recently-used cache of recently visited leaf pages.
//!
//! Point lookups, the put fast path, and range starts consult these hints to
//! skip a root-to-leaf descent. The cache is a pure data structure: it stores
//! each leaf's cached key span and answers "is this slot worth checking?", but
//! it never touches the page cache. Callers re-validate a candidate against the
//! leaf's live bytes before trusting it, so a stale span can only cost a wasted
//! check, never a wrong answer.

use crate::page::{PageId, RawLeafSpan};

// Recently used leaf pages, most recent first; page id 0 marks an empty slot
// (ids below 2 are superblocks and never tree pages). Multiple slots let
// workloads that interleave several key regions (separate tables in one tree)
// keep a hint per region; the could_cover prefilter keeps non-front slots to
// a couple of byte comparisons on the miss path.
pub(crate) const LEAF_HINT_SLOTS: usize = 4;

#[derive(Clone, Debug, Default)]
struct LeafHint {
    page_id: PageId,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    is_tail: bool,
}

impl LeafHint {
    /// Overwrites this slot in place, reusing its key buffers so descents
    /// don't allocate two fresh Vecs each time a hint is remembered.
    fn set(&mut self, page_id: PageId, span: &RawLeafSpan<'_>) {
        self.page_id = page_id;
        self.first_key.clear();
        self.first_key.extend_from_slice(span.first_key);
        self.last_key.clear();
        self.last_key.extend_from_slice(span.last_key);
        self.is_tail = span.next_page_id.is_none();
    }

    fn is_empty(&self) -> bool {
        self.page_id == 0
    }

    /// Prefilter only: false positives are re-checked by
    /// `raw_leaf_covers_key` on the live page bytes; `is_tail` can only go
    /// stale in the safe direction (a tail becomes non-tail on split, never
    /// the reverse, because leaves are never merged).
    fn could_cover(&self, key: &[u8]) -> bool {
        !self.is_empty()
            && self.first_key.as_slice() <= key
            && (key <= self.last_key.as_slice() || self.is_tail)
    }

    fn clear(&mut self) {
        self.page_id = 0;
        self.first_key.clear();
        self.last_key.clear();
        self.is_tail = false;
    }
}

/// MRU array of leaf hints. Slot 0 is the most recently used.
#[derive(Debug, Default)]
pub(crate) struct LeafHintCache {
    slots: [LeafHint; LEAF_HINT_SLOTS],
}

impl LeafHintCache {
    /// True when `page_id` is already the most-recently-used hint, so callers
    /// can skip re-remembering it.
    pub(crate) fn is_mru(&self, page_id: PageId) -> bool {
        self.slots[0].page_id == page_id
    }

    /// Records `page_id`'s span as the most-recently-used hint. If the page is
    /// already cached its slot is refreshed and promoted; otherwise it is
    /// rotated into slot 0, evicting the least-recently-used slot.
    pub(crate) fn remember(&mut self, page_id: PageId, span: &RawLeafSpan<'_>) {
        if let Some(pos) = self.slots.iter().position(|slot| slot.page_id == page_id) {
            self.slots[pos].set(page_id, span);
            self.slots[..=pos].rotate_right(1);
        } else {
            self.slots.rotate_right(1);
            self.slots[0].set(page_id, span);
        }
    }

    /// Like [`remember`](Self::remember), but only for the tail (rightmost)
    /// leaf. The append path uses this so a hint survives only while the leaf
    /// it points at keeps owning `[first_key, +inf)`.
    pub(crate) fn remember_if_tail(&mut self, page_id: PageId, span: &RawLeafSpan<'_>) {
        if span.next_page_id.is_some() {
            return;
        }
        self.remember(page_id, span);
    }

    /// Drops any slot pointing at `page_id` (e.g. when the page is freed).
    pub(crate) fn forget(&mut self, page_id: PageId) {
        for slot in &mut self.slots {
            if slot.page_id == page_id {
                slot.clear();
            }
        }
    }

    /// Empties every slot. Used after WAL replay, when all cached spans are
    /// stale.
    pub(crate) fn clear(&mut self) {
        for slot in &mut self.slots {
            slot.clear();
        }
    }

    /// Prefilter for point lookups and range starts: returns the page id in
    /// slot `idx` when it might cover `key` and is therefore worth validating
    /// against the leaf's live bytes, or `None` to skip the slot.
    ///
    /// `prefilter_mru` extends the cached-span prefilter to slot 0:
    /// latency-sensitive callers (the put fast path, range starts) skip the
    /// page lookup when even the most recent hint cannot cover `key`, while
    /// plain point lookups let slot 0 through to the authoritative check
    /// because it usually hits.
    pub(crate) fn covering_candidate(
        &self,
        idx: usize,
        key: &[u8],
        prefilter_mru: bool,
    ) -> Option<PageId> {
        let slot = &self.slots[idx];
        if slot.is_empty() || ((prefilter_mru || idx > 0) && !slot.could_cover(key)) {
            return None;
        }
        Some(slot.page_id)
    }

    /// Returns the page id in slot `idx` when its span starts at or below
    /// `start` — a "floor" candidate whose successor may be where a range scan
    /// for `start` begins — or `None` to skip the slot.
    pub(crate) fn floor_candidate(&self, idx: usize, start: &[u8]) -> Option<PageId> {
        let slot = &self.slots[idx];
        if slot.is_empty() || slot.first_key.as_slice() > start {
            return None;
        }
        Some(slot.page_id)
    }

    /// Promotes slot `idx` to most-recently-used, after a candidate there was
    /// confirmed to cover the key.
    pub(crate) fn promote(&mut self, idx: usize) {
        self.slots[..=idx].rotate_right(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a span over borrowed key bytes. `next` controls tail-ness.
    fn span<'a>(first: &'a [u8], last: &'a [u8], next: Option<PageId>) -> RawLeafSpan<'a> {
        RawLeafSpan {
            first_key: first,
            last_key: last,
            next_page_id: next,
        }
    }

    /// The page ids currently held, slot 0 (MRU) first.
    fn order(cache: &LeafHintCache) -> Vec<PageId> {
        cache.slots.iter().map(|s| s.page_id).collect()
    }

    #[test]
    fn remember_rotates_new_pages_to_the_front() {
        let mut cache = LeafHintCache::default();
        cache.remember(10, &span(b"a", b"b", Some(99)));
        cache.remember(20, &span(b"c", b"d", Some(99)));
        cache.remember(30, &span(b"e", b"f", Some(99)));
        // Newest first, oldest pushed toward the back; unused slots stay empty.
        assert_eq!(order(&cache), vec![30, 20, 10, 0]);
    }

    #[test]
    fn remember_existing_page_promotes_without_duplicating() {
        let mut cache = LeafHintCache::default();
        cache.remember(10, &span(b"a", b"b", Some(99)));
        cache.remember(20, &span(b"c", b"d", Some(99)));
        cache.remember(30, &span(b"e", b"f", Some(99)));
        // Touch the oldest live page again: it moves to the front, no dup.
        cache.remember(10, &span(b"a", b"b", Some(99)));
        assert_eq!(order(&cache), vec![10, 30, 20, 0]);
    }

    #[test]
    fn remember_evicts_least_recently_used_when_full() {
        let mut cache = LeafHintCache::default();
        for (id, (lo, hi)) in [(10, (b"a", b"b")), (20, (b"c", b"d")), (30, (b"e", b"f"))]
            .into_iter()
            .chain([(40, (b"g", b"h")), (50, (b"i", b"j"))])
        {
            cache.remember(id, &span(lo, hi, Some(99)));
        }
        // Four slots: 10 (the oldest) is gone, 20 is now the tail.
        assert_eq!(order(&cache), vec![50, 40, 30, 20]);
    }

    #[test]
    fn forget_clears_only_the_matching_slot() {
        let mut cache = LeafHintCache::default();
        cache.remember(10, &span(b"a", b"b", Some(99)));
        cache.remember(20, &span(b"c", b"d", Some(99)));
        cache.forget(10);
        assert_eq!(order(&cache), vec![20, 0, 0, 0]);
        assert!(!cache.is_mru(10));
        assert!(cache.is_mru(20));
    }

    #[test]
    fn clear_empties_all_slots() {
        let mut cache = LeafHintCache::default();
        cache.remember(10, &span(b"a", b"b", Some(99)));
        cache.remember(20, &span(b"c", b"d", Some(99)));
        cache.clear();
        assert_eq!(order(&cache), vec![0, 0, 0, 0]);
    }

    #[test]
    fn remember_if_tail_skips_non_tail_leaves() {
        let mut cache = LeafHintCache::default();
        cache.remember_if_tail(10, &span(b"a", b"b", Some(42))); // has a successor
        assert_eq!(order(&cache), vec![0, 0, 0, 0]);
        cache.remember_if_tail(20, &span(b"c", b"d", None)); // rightmost
        assert_eq!(order(&cache), vec![20, 0, 0, 0]);
    }

    #[test]
    fn covering_candidate_prefilters_non_front_slots_but_trusts_slot_zero() {
        let mut cache = LeafHintCache::default();
        cache.remember(10, &span(b"d", b"f", Some(99))); // becomes slot 1 below
        cache.remember(20, &span(b"m", b"p", Some(99))); // slot 0 (MRU)

        // A key in no slot's span: slot 0 is still offered when not prefiltering
        // (the authoritative check usually hits), but the prefilter rejects it.
        assert_eq!(cache.covering_candidate(0, b"a", false), Some(20));
        assert_eq!(cache.covering_candidate(0, b"a", true), None);

        // Non-front slots are always prefiltered, regardless of `prefilter_mru`.
        assert_eq!(cache.covering_candidate(1, b"a", false), None);
        assert_eq!(cache.covering_candidate(1, b"e", false), Some(10)); // in span

        // Empty slots never produce a candidate.
        assert_eq!(cache.covering_candidate(3, b"e", false), None);
    }

    #[test]
    fn covering_candidate_treats_tail_leaf_as_unbounded_above() {
        let mut cache = LeafHintCache::default();
        cache.remember(10, &span(b"m", b"p", None)); // tail: owns [m, +inf)
        // A key past last_key still passes the prefilter on a tail leaf.
        assert_eq!(cache.covering_candidate(0, b"z", true), Some(10));
        // ...but not on a non-tail leaf.
        cache.remember(10, &span(b"m", b"p", Some(99)));
        assert_eq!(cache.covering_candidate(0, b"z", true), None);
    }

    #[test]
    fn floor_candidate_matches_spans_starting_at_or_below_start() {
        let mut cache = LeafHintCache::default();
        cache.remember(10, &span(b"c", b"f", Some(99)));
        // start strictly above first_key: a floor candidate.
        assert_eq!(cache.floor_candidate(0, b"g"), Some(10));
        assert_eq!(cache.floor_candidate(0, b"c"), Some(10)); // equal is fine
        // start below first_key: not a floor.
        assert_eq!(cache.floor_candidate(0, b"a"), None);
        // empty slot.
        assert_eq!(cache.floor_candidate(1, b"g"), None);
    }

    #[test]
    fn promote_moves_a_slot_to_the_front() {
        let mut cache = LeafHintCache::default();
        cache.remember(10, &span(b"a", b"b", Some(99)));
        cache.remember(20, &span(b"c", b"d", Some(99)));
        cache.remember(30, &span(b"e", b"f", Some(99)));
        cache.promote(2); // confirm slot 2 (page 10) covered the key
        assert_eq!(order(&cache), vec![10, 30, 20, 0]);
    }
}
