//! Stateless ranges in text values: an anchor pair that also remembers the
//! text it covered.
//!
//! A [`Range`] keeps the text before it, the text it covers and the text
//! after it (the W3C text quote selector's `prefix`, `exact` and `suffix`).
//! It survives heavy edits to its own ends: when the covered text changed a
//! lot, the text around it still roots it in roughly the same place.

use crate::anchor::{
    CONTEXT_UNITS, context_end, context_start, edit_distance, fuzzy_ends_with_distance,
    moved_penalty, occurrences, pick, prefix_distance, shortlist, side_similarity,
    within_then_whole,
};

/// Past this many UTF-16 code units, a range's covered text is compared by
/// its first and last [`EDGE_UNITS`] and its length only.
const WHOLE_MATCH_UNITS: usize = 128;
const EDGE_UNITS: usize = 64;
/// How many starts and ends found by the text around a range are paired
/// with each other regardless of length.
const HELD_PAIRS: usize = 16;

/// A span of a text value, found again in later versions by its text and
/// the text around it. Like an [`crate::Anchor`], it can be captured from a
/// value a client holds or authored from the current value alone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Range {
    /// The `$version` of the value `start` and `end` count in, if known.
    pub version: Option<u64>,
    /// UTF-16 code units from the value's start, if known.
    pub start: Option<usize>,
    pub end: Option<usize>,
    /// Text right before the range (W3C `prefix`).
    pub before: String,
    /// The text the range covered (W3C `exact`).
    pub matched: String,
    /// Text right after the range (W3C `suffix`).
    pub after: String,
}

/// Where a range lands in a value, how sure the match is, and whether
/// another place matched about as well.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ResolvedRange {
    pub start: usize,
    pub end: usize,
    pub confidence: f64,
    pub ambiguous: bool,
}

impl Range {
    /// The range `start..end` (UTF-16 code units, clamped and ordered) of
    /// `text`, the value at `version`.
    pub fn capture(text: &str, version: u64, start: usize, end: usize) -> Self {
        let units: Vec<u16> = text.encode_utf16().collect();
        let (start, end) = (
            start.min(end).min(units.len()),
            end.max(start).min(units.len()),
        );
        Self {
            version: Some(version),
            start: Some(start),
            end: Some(end),
            before: String::from_utf16_lossy(&units[context_start(&units, start)..start]),
            matched: String::from_utf16_lossy(&units[start..end]),
            after: String::from_utf16_lossy(&units[end..context_end(&units, end)]),
        }
    }

    /// Where this range lands in `text`, the value at `version`.
    pub fn resolve(&self, text: &str, version: u64) -> ResolvedRange {
        let text: Vec<u16> = text.encode_utf16().collect();
        let hint = self.start.map(|start| {
            let start = start.min(text.len());
            (start, self.end.unwrap_or(start).clamp(start, text.len()))
        });
        if let (Some((start, end)), Some(at)) = (hint, self.version)
            && at == version
        {
            return ResolvedRange {
                start,
                end,
                confidence: 1.0,
                ambiguous: false,
            };
        }
        let search = RangeSearch {
            text: &text,
            before: self.before.encode_utf16().collect(),
            matched: self.matched.encode_utf16().collect(),
            after: self.after.encode_utf16().collect(),
            hint,
        };
        within_then_whole(
            text.len(),
            hint.map(|(start, _)| start),
            |span| search.best(span),
            |found| found.confidence,
        )
    }
}

struct RangeSearch<'a> {
    text: &'a [u16],
    before: Vec<u16>,
    matched: Vec<u16>,
    after: Vec<u16>,
    hint: Option<(usize, usize)>,
}

impl RangeSearch<'_> {
    fn captured(&self) -> bool {
        self.hint.is_some()
    }

    /// The covered text's first and last units, to find its ends by.
    fn head(&self) -> &[u16] {
        &self.matched[..self.matched.len().min(EDGE_UNITS)]
    }

    fn tail(&self) -> &[u16] {
        &self.matched[self.matched.len().saturating_sub(EDGE_UNITS)..]
    }

    fn best(&self, (lo, hi): (usize, usize)) -> ResolvedRange {
        let text = self.text;
        let len = self.matched.len();
        let mut starts: Vec<usize> = Vec::new();
        let mut ends: Vec<usize> = Vec::new();
        let mut spans: Vec<(usize, usize)> = self.hint.into_iter().collect();
        // The whole quote, or the covered text, unchanged.
        let joined: Vec<u16> = [&self.before[..], &self.matched, &self.after].concat();
        if !self.matched.is_empty() {
            for at in occurrences(text, lo, hi, &joined) {
                spans.push((at + self.before.len(), at + self.before.len() + len));
            }
            for at in occurrences(text, lo, hi, &self.matched) {
                spans.push((at, at + len));
            }
        }
        // The covered text's ends, approximately.
        for (end, distance) in fuzzy_ends_with_distance(text, lo, hi, self.tail()) {
            ends.push(end);
            let reversed_head: Vec<u16> = self.head().iter().rev().copied().collect();
            let from = end.saturating_sub(len + distance + EDGE_UNITS);
            let near: Vec<u16> = text[from..end].iter().rev().copied().collect();
            for (back, _) in fuzzy_ends_with_distance(&near, 0, near.len(), &reversed_head) {
                starts.push(end - back);
            }
        }
        // The text around it, for covered text rewritten too much to find.
        let mut held_starts: Vec<usize> = fuzzy_ends_with_distance(text, lo, hi, &self.before)
            .into_iter()
            .map(|(end, _)| end)
            .collect();
        let mut held_ends: Vec<usize> = Vec::new();
        for (end, distance) in fuzzy_ends_with_distance(text, lo, hi, &self.after) {
            held_ends.extend(
                end.saturating_sub(self.after.len() + distance)
                    ..=end.saturating_sub(self.after.len().saturating_sub(distance)),
            );
        }
        let near = |list: Vec<usize>, hint: Option<usize>| shortlist(list, text.len(), hint);
        let (start_hint, end_hint) = (
            self.hint.map(|(start, _)| start),
            self.hint.map(|(_, end)| end),
        );
        starts.extend_from_slice(&held_starts);
        ends.extend_from_slice(&held_ends);
        let starts = near(starts, start_hint);
        let ends = near(ends, end_hint);
        held_starts = near(held_starts, start_hint);
        held_starts.truncate(HELD_PAIRS);
        held_ends = near(held_ends, end_hint);
        held_ends.truncate(HELD_PAIRS);
        // Each end with the start nearest the covered text's length before
        // it, and the other way around.
        let nearest = |from: &[usize], to: usize, forwards: bool| {
            from.iter()
                .copied()
                .filter(|&at| if forwards { at >= to } else { at <= to })
                .min_by_key(|&at| at.abs_diff(to).abs_diff(len))
        };
        for &start in &starts {
            spans.extend(nearest(&ends, start, true).map(|end| (start, end)));
        }
        for &end in &ends {
            spans.extend(nearest(&starts, end, false).map(|start| (start, end)));
        }
        // Spans the text around them holds, whatever their length: covered
        // text that grew or shrank a lot.
        for &start in &held_starts {
            for &end in held_ends.iter().filter(|&&end| end >= start) {
                spans.push((start, end));
            }
        }
        if spans.is_empty() {
            spans.push((lo, lo));
        }
        let mut seen = std::collections::HashSet::new();
        spans
            .retain(|&(start, end)| start <= end && end <= text.len() && seen.insert((start, end)));
        let (span, confidence, ambiguous) = pick(
            spans.into_iter().map(|span| (span, self.score(span))),
            |a, b| a.0.abs_diff(b.0).max(a.1.abs_diff(b.1)),
        )
        .expect("there is always a span");
        ResolvedRange {
            start: span.0,
            end: span.1,
            confidence,
            ambiguous,
        }
    }

    /// How well `start..end` and the text around it match the range's:
    /// mostly its covered text, or mostly the text around it when the
    /// covered text was heavily rewritten.
    fn score(&self, (start, end): (usize, usize)) -> f64 {
        let edges = self.captured();
        let covered = self.covered_similarity(&self.text[start..end]);
        let before = side_similarity(&self.before, &self.text[..start], true, edges);
        let after = side_similarity(&self.after, &self.text[end..], false, edges);
        let held = (0.6 * covered + 0.2 * before + 0.2 * after)
            .max(0.25 * covered + 0.3 * before + 0.3 * after);
        (held - moved_penalty(start, self.hint.map(|(start, _)| start), self.text.len()))
            .clamp(0.0, 1.0)
    }

    fn covered_similarity(&self, found: &[u16]) -> f64 {
        let len = self.matched.len();
        if len == 0 {
            return if found.is_empty() { 1.0 } else { 0.0 };
        }
        if len <= WHOLE_MATCH_UNITS {
            if found.len() > len * 2 + CONTEXT_UNITS {
                return 0.0;
            }
            let distance = edit_distance(&self.matched, found);
            return 1.0 - (distance as f64 / len.max(found.len()) as f64).min(1.0);
        }
        let similarity = |pattern: &[u16], text: Vec<u16>| {
            1.0 - (prefix_distance(pattern, &text) as f64 / pattern.len() as f64).min(1.0)
        };
        let reach = |n: usize| n.min(found.len());
        let head = similarity(self.head(), found[..reach(EDGE_UNITS * 2)].to_vec());
        let tail_pattern: Vec<u16> = self.tail().iter().rev().copied().collect();
        let tail = similarity(
            &tail_pattern,
            found[found.len() - reach(EDGE_UNITS * 2)..]
                .iter()
                .rev()
                .copied()
                .collect(),
        );
        let lengths = len.min(found.len()) as f64 / len.max(found.len()) as f64;
        (head + tail) / 2.0 * (0.5 + 0.5 * lengths)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_range_follows_its_text() {
        let text = "Intro. Bring the red van keys. Outro.";
        let start = text.find("the red").unwrap();
        let range = Range::capture(text, 1, start, start + "the red van keys".len());
        let new = "Intro, rewritten. Bring the red van keys. Outro.";
        let found = range.resolve(new, 2);
        let at = new.find("the red").unwrap();
        assert_eq!(
            (found.start, found.end),
            (at, at + "the red van keys".len())
        );
        assert!(found.confidence > 0.9, "{found:?}");
    }

    #[test]
    fn an_authored_range_needs_no_position() {
        let range = Range {
            version: None,
            start: None,
            end: None,
            before: "Rehearsal at ".into(),
            matched: "the studio".into(),
            after: String::new(),
        };
        let text = "Monday: Rehearsal at the studio. Tuesday: the studio is closed.";
        let found = range.resolve(text, 7);
        let at = text.find("the studio").unwrap();
        assert_eq!((found.start, found.end), (at, at + 10));
        assert!(!found.ambiguous);
    }

    #[test]
    fn an_authored_range_without_context_can_be_ambiguous() {
        let range = Range {
            version: None,
            start: None,
            end: None,
            before: String::new(),
            matched: "the studio".into(),
            after: String::new(),
        };
        let found = range.resolve("Rehearsal at the studio. Tuesday: the studio is closed.", 7);
        assert!(found.ambiguous, "{found:?}");
    }
}
