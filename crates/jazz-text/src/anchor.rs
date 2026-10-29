//! Stateless anchors into text values.
//!
//! An [`Anchor`] is a position in one version of a text value, plus the text
//! around it, like the W3C Web Annotation text quote selector. Any holder of
//! a later version finds the position again from that text alone: no
//! history, no edit maps, no server round trip. [`Anchor::resolve`] says how
//! sure it is, and each use decides what is good enough: a cursor shows at
//! almost any confidence, a comment highlight wants a high one or shows as
//! orphaned. The client that owns an anchor rewrites it with fresh context
//! ([`Anchor::capture`]) when it drifts.
//!
//! Positions count UTF-16 code units, as JavaScript strings and ProseMirror
//! do.

/// The most text an anchor keeps on each side, in UTF-16 code units.
pub const CONTEXT_UNITS: usize = 32;

/// How far from the anchor's old position a resolve first looks, in UTF-16
/// code units, before searching the whole value.
const WINDOW_UNITS: usize = 8192;

/// Below this confidence in the window, a resolve also searches the whole
/// value.
const SEARCH_WHOLE_BELOW: f64 = 0.75;

/// The most candidate positions a resolve scores, nearest the old position
/// first.
const MAX_CANDIDATES: usize = 64;

/// Which text an anchor sticks to when someone types exactly at it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    /// The text before it: typing at the anchor goes after it (a range's
    /// end).
    Before,
    /// The text after it: typing at the anchor goes before it (a cursor, a
    /// range's start).
    After,
}

/// A position in one version of a text value, found again in later versions
/// by the text around it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Anchor {
    /// The `$version` of the value `position` counts in.
    pub version: u64,
    /// UTF-16 code units from the start of that value.
    pub position: usize,
    pub side: Side,
    /// Up to [`CONTEXT_UNITS`] of text right before `position`; shorter only
    /// at the value's start.
    pub before: String,
    /// Up to [`CONTEXT_UNITS`] of text right after `position`; shorter only
    /// at the value's end.
    pub after: String,
}

/// Where an anchor lands in a value, and how sure the match is: 1 when its
/// text around it is found unchanged, falling towards 0 as less of it
/// matches.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Resolved {
    pub position: usize,
    pub confidence: f64,
}

impl Anchor {
    /// The anchor at `position` (UTF-16 code units, clamped to the value) in
    /// `text`, the value at `version`.
    pub fn capture(text: &str, version: u64, position: usize, side: Side) -> Self {
        let units: Vec<u16> = text.encode_utf16().collect();
        let position = position.min(units.len());
        let mut start = position.saturating_sub(CONTEXT_UNITS);
        if start > 0 && is_low_surrogate(units[start]) {
            start += 1;
        }
        let mut end = (position + CONTEXT_UNITS).min(units.len());
        if end < units.len() && is_low_surrogate(units[end]) {
            end -= 1;
        }
        Self {
            version,
            position,
            side,
            before: String::from_utf16_lossy(&units[start..position]),
            after: String::from_utf16_lossy(&units[position..end]),
        }
    }

    /// Where this anchor lands in `text`, the value at `version`.
    pub fn resolve(&self, text: &str, version: u64) -> Resolved {
        let text: Vec<u16> = text.encode_utf16().collect();
        let hint = self.position.min(text.len());
        if version == self.version {
            return Resolved {
                position: hint,
                confidence: 1.0,
            };
        }
        let before: Vec<u16> = self.before.encode_utf16().collect();
        let after: Vec<u16> = self.after.encode_utf16().collect();
        let search = Search {
            text: &text,
            before: &before,
            after: &after,
            side: self.side,
            hint,
        };
        let window = (
            hint.saturating_sub(WINDOW_UNITS),
            (hint + WINDOW_UNITS).min(text.len()),
        );
        let found = search.best(window);
        if found.confidence >= SEARCH_WHOLE_BELOW || window == (0, text.len()) {
            return found;
        }
        let whole = search.best((0, text.len()));
        if whole.confidence > found.confidence {
            whole
        } else {
            found
        }
    }
}

struct Search<'a> {
    text: &'a [u16],
    before: &'a [u16],
    after: &'a [u16],
    side: Side,
    hint: usize,
}

impl Search<'_> {
    /// The best-scoring position among candidates in `start..end`.
    fn best(&self, (start, end): (usize, usize)) -> Resolved {
        let mut candidates = vec![self.hint];
        let joined: Vec<u16> = [self.before, self.after].concat();
        if !self.before.is_empty() && !self.after.is_empty() {
            candidates.extend(
                occurrences(self.text, start, end, &joined).map(|at| at + self.before.len()),
            );
        }
        candidates.extend(occurrences(self.text, start, end, self.after));
        candidates.extend(
            occurrences(self.text, start, end, self.before).map(|at| at + self.before.len()),
        );
        for at in fuzzy_ends(self.text, start, end, self.before) {
            candidates.push(at);
        }
        for (at, distance) in fuzzy_ends_with_distance(self.text, start, end, self.after) {
            // The match ends at `at`; its start is within `distance` of a
            // pattern's length before.
            let from = at.saturating_sub(self.after.len() + distance);
            let to = at.saturating_sub(self.after.len().saturating_sub(distance));
            candidates.extend(from..=to);
        }
        if self.before.len() < CONTEXT_UNITS {
            candidates.push(0);
        }
        if self.after.len() < CONTEXT_UNITS {
            candidates.push(self.text.len());
        }
        candidates.retain(|&at| at <= self.text.len());
        candidates.sort_unstable_by_key(|&at| (at.abs_diff(self.hint), at));
        candidates.dedup();
        candidates.truncate(MAX_CANDIDATES);
        candidates
            .into_iter()
            .map(|at| Resolved {
                position: at,
                confidence: self.score(at),
            })
            .fold(None::<Resolved>, |best, next| match best {
                Some(best) if best.confidence >= next.confidence => Some(best),
                _ => Some(next),
            })
            .expect("the hint is always a candidate")
    }

    /// How well the text around `at` matches the anchor's, weighted towards
    /// the side it sticks to, less a small penalty for moving far.
    fn score(&self, at: usize) -> f64 {
        // The side it sticks to must touch the position; text typed at the
        // anchor may sit between the position and the other side.
        let (before, after) = match self.side {
            Side::Before => (
                side_similarity(self.before, &self.text[..at], true),
                gap_similarity(self.after, &self.text[at..], false),
            ),
            Side::After => (
                gap_similarity(self.before, &self.text[..at], true),
                side_similarity(self.after, &self.text[at..], false),
            ),
        };
        let (stuck, other) = match self.side {
            Side::Before => (before, after),
            Side::After => (after, before),
        };
        // Held in place by the side it sticks to; or, when that side was
        // rewritten, by the other side touching the position.
        let touching_other = match self.side {
            Side::Before => side_similarity(self.after, &self.text[at..], false),
            Side::After => side_similarity(self.before, &self.text[..at], true),
        };
        let held = (0.65 * stuck + 0.35 * other).max(0.6 * touching_other + 0.3 * stuck);
        let moved = at.abs_diff(self.hint) as f64 / self.text.len().max(1) as f64;
        (held - 0.02 * moved.min(1.0)).clamp(0.0, 1.0)
    }
}

/// How much of `context` the text on one side of a position still shows: 1
/// for an exact match, falling with the edit distance. `towards_start`
/// compares with the end of `text` (text before the position), otherwise
/// with its start. A context shorter than [`CONTEXT_UNITS`] was cut by the
/// value's edge, so it must still reach that edge.
fn side_similarity(context: &[u16], text: &[u16], towards_start: bool) -> f64 {
    if context.is_empty() {
        return if text.is_empty() { 1.0 } else { 0.0 };
    }
    let reach = (context.len() * 2).min(text.len());
    let near: Vec<u16> = if towards_start {
        text[text.len() - reach..].iter().rev().copied().collect()
    } else {
        text[..reach].to_vec()
    };
    let pattern: Vec<u16> = if towards_start {
        context.iter().rev().copied().collect()
    } else {
        context.to_vec()
    };
    let distance = prefix_distance(&pattern, &near);
    1.0 - (distance as f64 / context.len() as f64).min(1.0) - edge_penalty(context, text.len())
}

/// A context shorter than [`CONTEXT_UNITS`] was cut by the value's edge; a
/// little less sure when that side now runs well past it.
fn edge_penalty(context: &[u16], side_len: usize) -> f64 {
    if context.len() < CONTEXT_UNITS && side_len > context.len() + CONTEXT_UNITS / 4 {
        1.0 / context.len().max(CONTEXT_UNITS / 2) as f64
    } else {
        0.0
    }
}

/// Up to how much text typed at an anchor still leaves its other side
/// recognisable, in UTF-16 code units.
const GAP_UNITS: usize = 256;

/// Like [`side_similarity`], but `context` may sit up to [`GAP_UNITS`] away
/// from the position, for a small penalty: text typed at the anchor lands
/// between them.
fn gap_similarity(context: &[u16], text: &[u16], towards_start: bool) -> f64 {
    if context.is_empty() {
        return if text.len() <= GAP_UNITS { 1.0 } else { 0.5 };
    }
    let reach = (GAP_UNITS + context.len() * 2).min(text.len());
    let near: Vec<u16> = if towards_start {
        text[text.len() - reach..].iter().rev().copied().collect()
    } else {
        text[..reach].to_vec()
    };
    let pattern: Vec<u16> = if towards_start {
        context.iter().rev().copied().collect()
    } else {
        context.to_vec()
    };
    let similarity = |distance: usize| 1.0 - (distance as f64 / context.len() as f64).min(1.0);
    // The whole side, when it is about as short as the context.
    let whole = if text.len() <= context.len() + CONTEXT_UNITS / 4 {
        similarity(edit_distance(&pattern, &near))
    } else {
        0.0
    };
    let Some((end, distance)) = fuzzy_ends_with_distance(&near, 0, near.len(), &pattern)
        .into_iter()
        .next()
    else {
        return whole.max(0.0);
    };
    let gap = end.saturating_sub(context.len()) as f64 / GAP_UNITS as f64;
    let found = similarity(distance) - 0.15 * gap.min(1.0) - edge_penalty(context, text.len());
    found.max(whole).max(0.0)
}

/// The edit distance from `pattern` to the closest prefix of `text`.
fn prefix_distance(pattern: &[u16], text: &[u16]) -> usize {
    let mut row: Vec<usize> = (0..=text.len()).collect();
    for (i, &unit) in pattern.iter().enumerate() {
        let mut diagonal = row[0];
        row[0] = i + 1;
        for j in 1..=text.len() {
            let above = row[j];
            row[j] = (diagonal + usize::from(text[j - 1] != unit))
                .min(above + 1)
                .min(row[j - 1] + 1);
            diagonal = above;
        }
    }
    row.into_iter().min().unwrap_or(pattern.len())
}

/// The edit distance between `a` and `b`.
fn edit_distance(a: &[u16], b: &[u16]) -> usize {
    let mut row: Vec<usize> = (0..=b.len()).collect();
    for (i, &unit) in a.iter().enumerate() {
        let mut diagonal = row[0];
        row[0] = i + 1;
        for j in 1..=b.len() {
            let above = row[j];
            row[j] = (diagonal + usize::from(b[j - 1] != unit))
                .min(above + 1)
                .min(row[j - 1] + 1);
            diagonal = above;
        }
    }
    row[b.len()]
}

/// Where `pattern` starts in `text[start..end]`, exactly.
fn occurrences<'a>(
    text: &'a [u16],
    start: usize,
    end: usize,
    pattern: &'a [u16],
) -> impl Iterator<Item = usize> + 'a {
    let span = &text[start..end];
    let windows = if pattern.is_empty() || pattern.len() > span.len() {
        None
    } else {
        Some(span.windows(pattern.len()))
    };
    windows
        .into_iter()
        .flatten()
        .enumerate()
        .filter(move |(_, window)| *window == pattern)
        .map(move |(at, _)| start + at)
}

/// Where the closest approximate matches of `pattern` end in
/// `text[start..end]`, if they keep at least half of it.
fn fuzzy_ends(text: &[u16], start: usize, end: usize, pattern: &[u16]) -> Vec<usize> {
    fuzzy_ends_with_distance(text, start, end, pattern)
        .into_iter()
        .map(|(at, _)| at)
        .collect()
}

/// [`fuzzy_ends`] with the edit distance of each match (Sellers' algorithm).
fn fuzzy_ends_with_distance(
    text: &[u16],
    start: usize,
    end: usize,
    pattern: &[u16],
) -> Vec<(usize, usize)> {
    if pattern.is_empty() {
        return Vec::new();
    }
    let mut column: Vec<usize> = (0..=pattern.len()).collect();
    let mut best = usize::MAX;
    let mut ends = Vec::new();
    for (j, &unit) in text[start..end].iter().enumerate() {
        let mut diagonal = column[0];
        for i in 1..=pattern.len() {
            let left = column[i];
            column[i] = (diagonal + usize::from(pattern[i - 1] != unit))
                .min(left + 1)
                .min(column[i - 1] + 1);
            diagonal = left;
        }
        let distance = column[pattern.len()];
        if distance < best {
            best = distance;
            ends.clear();
        }
        if distance == best {
            ends.push((start + j + 1, distance));
        }
    }
    if best.saturating_mul(2) > pattern.len() {
        return Vec::new();
    }
    ends
}

fn is_low_surrogate(unit: u16) -> bool {
    (0xDC00..0xE000).contains(&unit)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(text: &str, marker: &str) -> usize {
        text[..text.find(marker).unwrap()].encode_utf16().count()
    }

    #[test]
    fn the_same_version_resolves_to_its_position() {
        let anchor = Anchor::capture("Hello world", 3, 6, Side::After);
        assert_eq!(
            anchor.resolve("Hello world", 3),
            Resolved {
                position: 6,
                confidence: 1.0
            }
        );
    }

    #[test]
    fn an_insert_before_shifts_the_anchor() {
        let anchor = Anchor::capture("Load-in 2pm. Doors 8pm.", 1, 13, Side::After);
        let text = "Load-in 2pm. Soundcheck 6pm. Doors 8pm.";
        let found = anchor.resolve(text, 2);
        assert_eq!(found.position, at(text, "Doors"));
        assert!(found.confidence > 0.95, "{found:?}");
    }

    #[test]
    fn typing_at_the_anchor_follows_its_side() {
        let text = "one two";
        let after = Anchor::capture(text, 1, 4, Side::After);
        let before = Anchor::capture(text, 1, 4, Side::Before);
        let typed = "one XYZtwo";
        assert_eq!(after.resolve(typed, 2).position, at(typed, "two"));
        assert_eq!(before.resolve(typed, 2).position, at(typed, "XYZ"));
    }

    #[test]
    fn a_deleted_context_orphans_the_anchor() {
        let anchor = Anchor::capture(
            "Intro. The paragraph somebody will delete entirely soon. Outro.",
            1,
            30,
            Side::After,
        );
        let found = anchor.resolve("Intro. Outro.", 2);
        assert!(found.confidence < 0.6, "{found:?}");
    }
}
