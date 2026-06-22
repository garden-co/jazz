//! In-memory free-page tracker backed by a bitmap.
//!
//! Replaces the previous sorted-`Vec` + `HashSet` pair. A set bit at index
//! `page_id` means that page is free and available for reuse. Pages 0 and 1 are
//! reserved for the superblock slots and are never tracked here.
//!
//! The on-disk freelist format (a linked list of id chunks; see
//! `Page::Freelist`) is unchanged. This structure is the in-memory working set
//! only: it is populated on load/WAL replay and serialized back on flush.
//!
//! Allocation policy is preserved from the previous implementation:
//! single-page allocation reuses the highest free id, while extent allocation
//! reuses the lowest contiguous run that is large enough.

use crate::page::PageId;

const BITS_PER_WORD: u64 = 64;

#[derive(Debug, Default)]
pub(crate) struct FreeBitmap {
    /// Bit `i` of `words[i / 64]` is set when page `i` is free.
    words: Vec<u64>,
    /// Number of set bits, kept in sync with `words` so `len` stays O(1).
    free_count: usize,
}

impl FreeBitmap {
    #[inline]
    fn locate(page_id: PageId) -> (usize, u64) {
        let word = (page_id / BITS_PER_WORD) as usize;
        let bit = 1u64 << (page_id % BITS_PER_WORD);
        (word, bit)
    }

    /// Marks `page_id` free. Returns `true` if it was not already free.
    pub(crate) fn insert(&mut self, page_id: PageId) -> bool {
        let (word, bit) = Self::locate(page_id);
        if word >= self.words.len() {
            self.words.resize(word + 1, 0);
        }
        if self.words[word] & bit != 0 {
            return false;
        }
        self.words[word] |= bit;
        self.free_count += 1;
        true
    }

    /// Marks `page_id` allocated. Returns `true` if it had been free.
    pub(crate) fn remove(&mut self, page_id: PageId) -> bool {
        let (word, bit) = Self::locate(page_id);
        if word >= self.words.len() || self.words[word] & bit == 0 {
            return false;
        }
        self.words[word] &= !bit;
        self.free_count -= 1;
        true
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, page_id: PageId) -> bool {
        let (word, bit) = Self::locate(page_id);
        word < self.words.len() && self.words[word] & bit != 0
    }

    pub(crate) fn len(&self) -> usize {
        self.free_count
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.free_count == 0
    }

    pub(crate) fn clear(&mut self) {
        self.words.clear();
        self.free_count = 0;
    }

    /// Highest free page id, or `None` when empty.
    pub(crate) fn highest(&self) -> Option<PageId> {
        for (idx, word) in self.words.iter().enumerate().rev() {
            if *word != 0 {
                let bit = BITS_PER_WORD - 1 - word.leading_zeros() as u64;
                return Some(idx as u64 * BITS_PER_WORD + bit);
            }
        }
        None
    }

    /// Removes and returns the highest free page id, or `None` when empty.
    pub(crate) fn take_highest(&mut self) -> Option<PageId> {
        let page_id = self.highest()?;
        self.remove(page_id);
        self.trim_trailing_empty_words();
        Some(page_id)
    }

    /// Iterates free page ids in ascending order.
    pub(crate) fn iter(&self) -> impl Iterator<Item = PageId> + '_ {
        self.words
            .iter()
            .enumerate()
            .flat_map(|(idx, &word)| WordBits {
                word,
                base: idx as u64 * BITS_PER_WORD,
            })
    }

    /// Lowest start page of `count` consecutive free pages, or `None` if no
    /// such run exists. `count` must be >= 1.
    pub(crate) fn find_run(&self, count: usize) -> Option<PageId> {
        debug_assert!(count >= 1);
        let mut run_start = 0u64;
        let mut run_len = 0usize;
        let mut prev = 0u64;
        for page_id in self.iter() {
            if run_len != 0 && page_id == prev + 1 {
                run_len += 1;
            } else {
                run_start = page_id;
                run_len = 1;
            }
            prev = page_id;
            if run_len >= count {
                return Some(run_start);
            }
        }
        None
    }

    /// Finds the lowest run of `count` consecutive free pages, marks them
    /// allocated, and returns the start page. Returns `None` if no run that
    /// long is free. `count` must be >= 1.
    pub(crate) fn take_run(&mut self, count: usize) -> Option<PageId> {
        let run_start = self.find_run(count)?;
        // `find_run` just confirmed every bit in the run is set, so each
        // `remove` succeeds and the count stays accurate.
        for offset in 0..count as u64 {
            self.remove(run_start + offset);
        }
        self.trim_trailing_empty_words();
        Some(run_start)
    }

    /// Clears every free bit for a page id `>= limit`.
    pub(crate) fn retain_below(&mut self, limit: PageId) {
        let (limit_word, limit_bit) = Self::locate(limit);
        // Mask off the high bits of the boundary word, then drop the words that
        // sit entirely above `limit`.
        if limit_word < self.words.len() {
            let keep_mask = limit_bit - 1;
            let word = &mut self.words[limit_word];
            let dropped = (*word & !keep_mask).count_ones() as usize;
            *word &= keep_mask;
            self.free_count -= dropped;

            for word in &mut self.words[limit_word + 1..] {
                self.free_count -= word.count_ones() as usize;
                *word = 0;
            }
            self.words.truncate(limit_word + 1);
        }
        self.trim_trailing_empty_words();
    }

    /// Drops trailing all-zero words so `highest`/`take_highest` never scan
    /// dead space left behind after high pages are cleared.
    pub(crate) fn trim_trailing_empty_words(&mut self) {
        while matches!(self.words.last(), Some(0)) {
            self.words.pop();
        }
    }
}

/// Iterator over the set bits of a single word, yielding absolute page ids.
struct WordBits {
    word: u64,
    base: u64,
}

impl Iterator for WordBits {
    type Item = PageId;

    fn next(&mut self) -> Option<PageId> {
        if self.word == 0 {
            return None;
        }
        let bit = self.word.trailing_zeros() as u64;
        self.word &= self.word - 1;
        Some(self.base + bit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_remove_and_membership() {
        let mut bm = FreeBitmap::default();
        assert!(bm.insert(5));
        assert!(!bm.insert(5));
        assert!(bm.contains(5));
        assert_eq!(bm.len(), 1);
        assert!(bm.remove(5));
        assert!(!bm.remove(5));
        assert!(!bm.contains(5));
        assert!(bm.is_empty());
    }

    #[test]
    fn highest_picks_largest_id() {
        let mut bm = FreeBitmap::default();
        for id in [3u64, 70, 64, 130] {
            bm.insert(id);
        }
        assert_eq!(bm.highest(), Some(130));
        assert_eq!(bm.take_highest(), Some(130));
        assert_eq!(bm.highest(), Some(70));
    }

    #[test]
    fn iter_is_ascending() {
        let mut bm = FreeBitmap::default();
        for id in [130u64, 3, 64, 70, 2] {
            bm.insert(id);
        }
        let ids: Vec<PageId> = bm.iter().collect();
        assert_eq!(ids, vec![2, 3, 64, 70, 130]);
    }

    #[test]
    fn find_run_returns_lowest_contiguous_start() {
        let mut bm = FreeBitmap::default();
        // Runs: [2,3], [10,11,12], [20]
        for id in [2u64, 3, 10, 11, 12, 20] {
            bm.insert(id);
        }
        assert_eq!(bm.find_run(1), Some(2));
        assert_eq!(bm.find_run(2), Some(2));
        assert_eq!(bm.find_run(3), Some(10));
        assert_eq!(bm.find_run(4), None);
    }

    #[test]
    fn find_run_spans_word_boundary() {
        let mut bm = FreeBitmap::default();
        for id in [62u64, 63, 64, 65] {
            bm.insert(id);
        }
        assert_eq!(bm.find_run(4), Some(62));
        assert_eq!(bm.find_run(5), None);
    }

    #[test]
    fn retain_below_clears_high_ids_and_recounts() {
        let mut bm = FreeBitmap::default();
        for id in [2u64, 63, 64, 65, 200] {
            bm.insert(id);
        }
        bm.retain_below(64);
        assert_eq!(bm.iter().collect::<Vec<_>>(), vec![2, 63]);
        assert_eq!(bm.len(), 2);
    }

    #[test]
    fn take_run_clears_lowest_run_and_updates_count() {
        let mut bm = FreeBitmap::default();
        for id in [2u64, 3, 10, 11, 12, 20] {
            bm.insert(id);
        }
        assert_eq!(bm.take_run(3), Some(10));
        assert_eq!(bm.len(), 3);
        assert!(!bm.contains(10) && !bm.contains(11) && !bm.contains(12));
        assert_eq!(bm.iter().collect::<Vec<_>>(), vec![2, 3, 20]);
        assert_eq!(bm.take_run(3), None);
    }

    #[test]
    fn trim_keeps_highest_scan_tight_after_high_pages_clear() {
        let mut bm = FreeBitmap::default();
        // 200 sits in a high word; 2 sits in word 0.
        bm.insert(2);
        bm.insert(200);
        assert_eq!(bm.take_highest(), Some(200));
        // After trimming, only word 0 remains backing the bitmap.
        assert_eq!(bm.words.len(), 1);
        assert_eq!(bm.highest(), Some(2));

        // retain_below also trims the empty tail it leaves behind.
        bm.insert(300);
        bm.retain_below(64);
        assert_eq!(bm.words.len(), 1);
        assert_eq!(bm.iter().collect::<Vec<_>>(), vec![2]);
    }
}
