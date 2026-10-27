//! Three-way merge of a text-merging string column (SPEC 4 §4.3).
//!
//! A column declared `text`, `markdown` or `html` stores its value as a
//! plain string. A write to it carries, beside the value it set, the edits
//! that turn that value back into the base its writer saw (`$base.<column>`,
//! see [`base_cell`]). Every node applying the write (the authority, a relay,
//! a client replaying its pending writes) merges the base, the value it holds
//! and the write's value: each side's changes are diffed against the base and
//! both applied. Nothing but the merged string is stored.
//!
//! Values are cut into tokens first (words, single whitespace and
//! other characters; for markdown also delimiter runs and link destinations, for
//! HTML also tags and entities), so an edit never splits one. Two sides'
//! changes then combine by these rules, in base order:
//!
//! - changes to disjoint parts of the base all apply;
//! - an identical change made by both applies once;
//! - inserts at one point apply in seq order, the held value's first;
//! - where the sides overlap and every change in the overlap only inserts
//!   or only deletes, the deletes are unioned and the inserts kept;
//! - otherwise (two replacements of overlapping spans) the incoming write,
//!   which the authority sequences later, wins that span.

use crate::options::Error;
use crate::records::Value;
use crate::schema::TextFormat;
use std::collections::HashMap;

/// The cell name a write's base edits for text column `column` travel under.
pub(crate) fn base_cell(column: &str) -> String {
    format!("{BASE_PREFIX}{column}")
}

/// The text column a `$base.` cell belongs to.
pub(crate) fn based_column(name: &str) -> Option<&str> {
    name.strip_prefix(BASE_PREFIX)
}

const BASE_PREFIX: &str = "$base.";

/// Past this many edit steps in one diff the changed middle is treated as
/// one replacement, so a rewrite costs linear time.
const MAX_EDIT_DISTANCE: usize = 4096;

fn malformed(message: impl std::fmt::Display) -> Error {
    crate::db::rejected(format!("malformed text base: {message}"))
}

/// One replacement of `value[start..end]` (byte offsets) by `insert`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Edit {
    pub start: usize,
    pub end: usize,
    pub insert: String,
}

/// The edit that turns `from` into `to`: their differing middle, past the
/// common prefix and suffix (none when they are equal). A base travels as
/// this one edit; where it lands is what the merge's diff decides.
pub(crate) fn edits(from: &str, to: &str) -> Vec<Edit> {
    if from == to {
        return Vec::new();
    }
    let mut prefix = from
        .bytes()
        .zip(to.bytes())
        .take_while(|(a, b)| a == b)
        .count();
    while !from.is_char_boundary(prefix) || !to.is_char_boundary(prefix) {
        prefix -= 1;
    }
    let room = from.len().min(to.len()) - prefix;
    let mut suffix = from
        .bytes()
        .rev()
        .zip(to.bytes().rev())
        .take(room)
        .take_while(|(a, b)| a == b)
        .count();
    while !from.is_char_boundary(from.len() - suffix) || !to.is_char_boundary(to.len() - suffix) {
        suffix -= 1;
    }
    vec![Edit {
        start: prefix,
        end: from.len() - suffix,
        insert: to[prefix..to.len() - suffix].to_owned(),
    }]
}

/// `value` with `edits` (ordered, disjoint, at char boundaries) applied.
pub(crate) fn apply(value: &str, edits: &[Edit]) -> Result<String, Error> {
    let mut out = String::with_capacity(value.len());
    let mut at = 0;
    for edit in edits {
        if edit.start < at || edit.end < edit.start || edit.end > value.len() {
            return Err(malformed("edits out of order or out of range"));
        }
        if !value.is_char_boundary(edit.start) || !value.is_char_boundary(edit.end) {
            return Err(malformed("an edit splits a character"));
        }
        out.push_str(&value[at..edit.start]);
        out.push_str(&edit.insert);
        at = edit.end;
    }
    out.push_str(&value[at..]);
    Ok(out)
}

/// `edits` as the `$base.` cell value: an array of (start, end, insert).
pub(crate) fn encode(edits: &[Edit]) -> Value {
    Value::Array(
        edits
            .iter()
            .map(|edit| {
                Value::Tuple(vec![
                    Value::U64(edit.start as u64),
                    Value::U64(edit.end as u64),
                    Value::String(edit.insert.clone()),
                ])
            })
            .collect(),
    )
}

/// The edits a `$base.` cell value holds.
pub(crate) fn decode(value: &Value) -> Result<Vec<Edit>, Error> {
    let Value::Array(items) = value else {
        return Err(malformed("not an array of edits"));
    };
    items
        .iter()
        .map(|item| match item {
            Value::Tuple(fields) => match fields.as_slice() {
                [Value::U64(start), Value::U64(end), Value::String(insert)] => Ok(Edit {
                    start: usize::try_from(*start).map_err(|_| malformed("offset too large"))?,
                    end: usize::try_from(*end).map_err(|_| malformed("offset too large"))?,
                    insert: insert.clone(),
                }),
                _ => Err(malformed("an edit is not (start, end, insert)")),
            },
            _ => Err(malformed("an edit is not (start, end, insert)")),
        })
        .collect()
}

/// The merge of `held` and `incoming`, both made from `base`.
pub(crate) fn merge3(format: TextFormat, base: &str, held: &str, incoming: &str) -> String {
    if held == base {
        return incoming.to_owned();
    }
    if incoming == base || held == incoming {
        return held.to_owned();
    }
    let base_tokens = tokenize(format, base);
    let held_tokens = tokenize(format, held);
    let incoming_tokens = tokenize(format, incoming);
    let ours = diff(&base_tokens, &held_tokens);
    let mut theirs = diff(&base_tokens, &incoming_tokens);
    // A change both made applies once.
    theirs.retain(|hunk| {
        !ours.iter().any(|mine| {
            mine.old == hunk.old
                && held_tokens[mine.new.0..mine.new.1] == incoming_tokens[hunk.new.0..hunk.new.1]
        })
    });
    let side = |hunk: &Hunk, held: bool| Change {
        old: hunk.old,
        insert: if held {
            held_tokens[hunk.new.0..hunk.new.1].concat()
        } else {
            incoming_tokens[hunk.new.0..hunk.new.1].concat()
        },
        held,
    };
    let mut changes: Vec<Change> = ours
        .iter()
        .map(|hunk| side(hunk, true))
        .chain(theirs.iter().map(|hunk| side(hunk, false)))
        .collect();
    // Base order; at one point the held value's change first.
    changes.sort_by_key(|change| (change.old.0, change.old.1, !change.held));
    let mut out = String::with_capacity(held.len().max(incoming.len()));
    let mut at = 0; // base tokens consumed
    let mut index = 0;
    while index < changes.len() {
        // The cluster of changes that overlap one another.
        let mut end = index + 1;
        let mut reach = changes[index].old.1;
        while end < changes.len() && changes[end].overlaps(changes[index].old.0, reach) {
            reach = reach.max(changes[end].old.1);
            end += 1;
        }
        let cluster = &changes[index..end];
        let start = cluster[0].old.0;
        out.extend(base_tokens[at..start].iter().copied());
        resolve(&base_tokens, start, reach, cluster, &mut out);
        at = reach;
        index = end;
    }
    out.extend(base_tokens[at..].iter().copied());
    out
}

/// One side's replacement of base tokens `old` by `insert`.
struct Change {
    old: (usize, usize),
    insert: String,
    held: bool,
}

impl Change {
    /// Whether this change, which starts no earlier, joins a cluster over
    /// base span `start..end`: it starts inside the span, or is an insert
    /// strictly inside it or at the same point as a cluster of inserts. A
    /// change that only touches the span's end does not, nor does a span
    /// starting where a cluster of inserts sits.
    fn overlaps(&self, start: usize, end: usize) -> bool {
        if self.inserts_only() {
            (start < self.old.0 && self.old.0 < end) || (start == end && self.old.0 == start)
        } else {
            self.old.0 < end
        }
    }

    fn deletes_only(&self) -> bool {
        self.insert.is_empty()
    }

    fn inserts_only(&self) -> bool {
        self.old.0 == self.old.1
    }
}

/// Write the merge of base tokens `start..end` under `cluster`'s changes.
fn resolve(base: &[&str], start: usize, end: usize, cluster: &[Change], out: &mut String) {
    let one_side = cluster.iter().all(|c| c.held) || cluster.iter().all(|c| !c.held);
    let unions = cluster
        .iter()
        .all(|change| change.deletes_only() || change.inserts_only());
    if one_side || unions {
        // Deletes unioned, inserts kept in base order (held first).
        let mut deleted = vec![false; end - start];
        for change in cluster {
            deleted[change.old.0 - start..change.old.1 - start].fill(true);
        }
        let mut inserts = cluster.iter().peekable();
        for (offset, token) in base[start..end].iter().enumerate() {
            while let Some(change) = inserts.next_if(|c| c.old.0 <= start + offset) {
                out.push_str(&change.insert);
            }
            if !deleted[offset] {
                out.push_str(token);
            }
        }
        for change in inserts {
            out.push_str(&change.insert);
        }
        return;
    }
    // Overlapping replacements: the incoming write's version of the span.
    let mut at = start;
    for change in cluster.iter().filter(|c| !c.held) {
        out.extend(base[at..change.old.0].iter().copied());
        out.push_str(&change.insert);
        at = change.old.1;
    }
    out.extend(base[at..end].iter().copied());
}

/// A diff hunk: base tokens `old` replaced by new tokens `new`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Hunk {
    old: (usize, usize),
    new: (usize, usize),
}

/// The hunks that turn `old` into `new` (Myers, linear space), coalesced.
fn diff(old: &[&str], new: &[&str]) -> Vec<Hunk> {
    // Tokens as small ids, so comparisons are integer compares.
    let mut ids: HashMap<&str, u32> = HashMap::new();
    let mut a = Vec::with_capacity(old.len());
    let mut b = Vec::with_capacity(new.len());
    for (tokens, out) in [(old, &mut a), (new, &mut b)] {
        for token in tokens {
            let next = ids.len() as u32;
            out.push(*ids.entry(token).or_insert(next));
        }
    }
    let mut hunks = Vec::new();
    diff_range(&a, &b, 0, a.len(), 0, b.len(), &mut hunks);
    // Coalesce touching hunks.
    let mut out: Vec<Hunk> = Vec::with_capacity(hunks.len());
    for hunk in hunks {
        match out.last_mut() {
            Some(last) if last.old.1 == hunk.old.0 && last.new.1 == hunk.new.0 => {
                last.old.1 = hunk.old.1;
                last.new.1 = hunk.new.1;
            }
            _ => out.push(hunk),
        }
    }
    out
}

fn diff_range(
    a: &[u32],
    b: &[u32],
    mut a0: usize,
    mut a1: usize,
    mut b0: usize,
    mut b1: usize,
    out: &mut Vec<Hunk>,
) {
    while a0 < a1 && b0 < b1 && a[a0] == b[b0] {
        a0 += 1;
        b0 += 1;
    }
    while a0 < a1 && b0 < b1 && a[a1 - 1] == b[b1 - 1] {
        a1 -= 1;
        b1 -= 1;
    }
    if a0 == a1 || b0 == b1 {
        if a0 != a1 || b0 != b1 {
            out.push(Hunk {
                old: (a0, a1),
                new: (b0, b1),
            });
        }
        return;
    }
    match middle_snake(&a[a0..a1], &b[b0..b1]) {
        Some((x, y)) => {
            diff_range(a, b, a0, a0 + x, b0, b0 + y, out);
            diff_range(a, b, a0 + x, a1, b0 + y, b1, out);
        }
        None => out.push(Hunk {
            old: (a0, a1),
            new: (b0, b1),
        }),
    }
}

/// A split point (x, y) on an optimal edit path of `a` into `b` that is
/// neither (0, 0) nor the end, found by the forward and reverse Myers
/// searches meeting; `None` past [`MAX_EDIT_DISTANCE`].
fn middle_snake(a: &[u32], b: &[u32]) -> Option<(usize, usize)> {
    let (n, m) = (a.len() as isize, b.len() as isize);
    let max = ((n + m + 1) / 2).min(MAX_EDIT_DISTANCE as isize);
    let delta = n - m;
    let odd = delta % 2 != 0;
    let offset = max + 1;
    let size = (2 * max + 3) as usize;
    let mut forward = vec![0isize; size];
    let mut reverse = vec![0isize; size];
    for d in 0..=max {
        let mut k = -d;
        while k <= d {
            let i = (k + offset) as usize;
            let mut x = if k == -d || (k != d && forward[i - 1] < forward[i + 1]) {
                forward[i + 1]
            } else {
                forward[i - 1] + 1
            };
            let mut y = x - k;
            let (x0, y0) = (x, y);
            while x < n && y < m && a[x as usize] == b[y as usize] {
                x += 1;
                y += 1;
            }
            forward[i] = x;
            let rk = delta - k;
            if odd && rk > -d && rk < d {
                let ri = (rk + offset) as usize;
                if forward[i] + reverse[ri] >= n {
                    return split(x0, y0, x, y, n, m);
                }
            }
            k += 2;
        }
        let mut k = -d;
        while k <= d {
            let i = (k + offset) as usize;
            let mut x = if k == -d || (k != d && reverse[i - 1] < reverse[i + 1]) {
                reverse[i + 1]
            } else {
                reverse[i - 1] + 1
            };
            let mut y = x - k;
            while x < n && y < m && a[(n - x - 1) as usize] == b[(m - y - 1) as usize] {
                x += 1;
                y += 1;
            }
            reverse[i] = x;
            let fk = delta - k;
            if !odd && fk >= -d && fk <= d {
                let fi = (fk + offset) as usize;
                if forward[fi] + reverse[i] >= n {
                    let (fx, fy) = (n - x, m - y);
                    return split(fx, fy, fx, fy, n, m);
                }
            }
            k += 2;
        }
    }
    None
}

/// A split strictly inside the problem: the snake's start, or its end when
/// the start is a corner. The recursion then always shrinks.
fn split(x0: isize, y0: isize, x1: isize, y1: isize, n: isize, m: isize) -> Option<(usize, usize)> {
    let inside = |x: isize, y: isize| (x, y) != (0, 0) && (x, y) != (n, m);
    if inside(x0, y0) {
        Some((x0 as usize, y0 as usize))
    } else if inside(x1, y1) {
        Some((x1 as usize, y1 as usize))
    } else {
        None
    }
}

/// `text` cut into the tokens a merge keeps whole.
fn tokenize(format: TextFormat, text: &str) -> Vec<&str> {
    let mut tokens = Vec::new();
    let mut rest = text;
    let mut line_start = true;
    while !rest.is_empty() {
        let len = token_len(format, rest, line_start);
        let (token, tail) = rest.split_at(len);
        line_start = token.ends_with('\n');
        tokens.push(token);
        rest = tail;
    }
    tokens
}

/// The byte length of the token `rest` starts with (at least one char).
fn token_len(format: TextFormat, rest: &str, line_start: bool) -> usize {
    let first = rest.chars().next().expect("non-empty");
    let run = |pred: &dyn Fn(char) -> bool| -> usize {
        rest.char_indices()
            .find(|(_, c)| !pred(*c))
            .map_or(rest.len(), |(at, _)| at)
    };
    match format {
        TextFormat::Html => {
            if first == '<'
                && let Some(end) = rest.find('>')
            {
                return end + 1;
            }
            if first == '&'
                && let Some(end) = rest.find(';')
                && end <= 32
                && rest[1..end]
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '#')
            {
                return end + 1;
            }
        }
        TextFormat::Markdown => {
            if matches!(
                first,
                '*' | '_' | '~' | '`' | '#' | '=' | '-' | '>' | '+' | '|'
            ) {
                return run(&|c| c == first);
            }
            if line_start && first.is_ascii_digit() {
                let digits = run(&|c| c.is_ascii_digit());
                if rest[digits..].starts_with(". ") || rest[digits..].starts_with(") ") {
                    return digits + 1;
                }
            }
            if rest.starts_with("](")
                && let Some(end) = rest.find(')')
                && !rest[..end].contains('\n')
            {
                return end + 1;
            }
            if rest.starts_with("![") {
                return 2;
            }
        }
        TextFormat::Plain => {}
    }
    if first.is_alphanumeric() {
        run(&|c| c.is_alphanumeric())
    } else {
        first.len_utf8()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn merge(base: &str, held: &str, incoming: &str) -> String {
        merge3(TextFormat::Plain, base, held, incoming)
    }

    #[test]
    fn disjoint_edits_both_apply() {
        assert_eq!(
            merge("The cat sat.", "The black cat sat.", "The cat slept."),
            "The black cat slept."
        );
    }

    #[test]
    fn inserts_at_one_point_keep_seq_order() {
        assert_eq!(merge("Hi", "Hi!", "Hi?"), "Hi!?");
    }

    #[test]
    fn an_identical_change_applies_once() {
        assert_eq!(merge("a b c", "a x b c d", "a x b c"), "a x b c d");
    }

    #[test]
    fn an_insert_inside_a_deleted_span_survives() {
        assert_eq!(
            merge("one two three four", "one four", "one two 2.5 three four"),
            "one 2.5 four"
        );
    }

    #[test]
    fn overlapping_replacements_go_to_the_incoming_write() {
        assert_eq!(
            merge("the quick fox", "the slow fox", "the fast fox"),
            "the fast fox"
        );
    }

    #[test]
    fn unchanged_sides_short_circuit() {
        assert_eq!(merge("a", "a", "b"), "b");
        assert_eq!(merge("a", "b", "a"), "b");
        assert_eq!(merge("a", "b", "b"), "b");
    }

    #[test]
    fn edits_round_trip() {
        let cases = [
            ("", "hello"),
            ("hello", ""),
            ("hello world", "hello brave new world"),
            ("ünïcødé text", "ünïcødé tëxt!"),
            ("a\nb\nc", "a\nB\nc\nd"),
        ];
        for (from, to) in cases {
            let edits = edits(from, to);
            assert_eq!(apply(from, &edits).unwrap(), to, "{from:?} -> {to:?}");
            assert_eq!(decode(&encode(&edits)).unwrap(), edits);
        }
    }

    #[test]
    fn markdown_delimiters_stay_whole() {
        let tokens = tokenize(TextFormat::Markdown, "**bold** and [a link](http://x.y/z)");
        assert!(tokens.contains(&"**"));
        assert!(tokens.contains(&"](http://x.y/z)"));
        // Bolding a word while another edit lands next to it keeps both
        // delimiter runs intact.
        assert_eq!(
            merge3(
                TextFormat::Markdown,
                "make this bold",
                "make **this** bold",
                "make this bold!"
            ),
            "make **this** bold!"
        );
    }

    #[test]
    fn html_tags_stay_whole() {
        let tokens = tokenize(TextFormat::Html, "<p class=\"a\">x &amp; y</p>");
        assert_eq!(tokens[0], "<p class=\"a\">");
        assert!(tokens.contains(&"&amp;"));
        assert!(tokens.contains(&"</p>"));
        assert_eq!(
            merge3(
                TextFormat::Html,
                "<p>one two</p>",
                "<p>one <b>two</b></p>",
                "<p>zero one two</p>"
            ),
            "<p>zero one <b>two</b></p>"
        );
    }

    /// A small deterministic generator, so the property test needs no
    /// dependency.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
    }

    const WORDS: [&str; 8] = ["alpha", "beta", "gamma", " ", "\n", ".", "ü", "delta"];

    fn random_text(rng: &mut Rng, len: usize) -> String {
        (0..len).map(|_| WORDS[rng.below(WORDS.len())]).collect()
    }

    /// Up to three random inserts or (with `delete`) deletes at token
    /// boundaries of `text`, with the inserted markers (unique to `side`).
    fn random_edit(
        rng: &mut Rng,
        text: &str,
        side: char,
        delete: bool,
    ) -> (String, Option<Vec<String>>) {
        let tokens = tokenize(TextFormat::Plain, text);
        let mut out: Vec<String> = tokens.iter().map(|t| (*t).to_owned()).collect();
        let mut inserted = Vec::new();
        for n in 0..1 + rng.below(3) {
            if delete {
                if !out.is_empty() {
                    let at = rng.below(out.len());
                    out.remove(at);
                }
            } else {
                let at = rng.below(out.len() + 1);
                let marker = format!("[{side}{n}]");
                inserted.push(marker.clone());
                out.insert(at, marker);
            }
        }
        // Only edits that keep the other tokens whole are pure inserts or
        // deletes once retokenized (deleting a separator joins two words).
        let text = out.concat();
        let markers: Vec<String> = out
            .iter()
            .flat_map(|t| tokenize(TextFormat::Plain, t))
            .map(str::to_owned)
            .collect();
        let whole = tokenize(TextFormat::Plain, &text) == markers;
        (text, whole.then_some(inserted))
    }

    #[test]
    fn random_concurrent_inserts_all_survive_and_diffs_round_trip() {
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        let mut checked = 0;
        for _ in 0..2000 {
            let len = rng.below(30);
            let base = random_text(&mut rng, len);
            let (held_deletes, incoming_deletes) = (rng.below(2) == 0, rng.below(2) == 0);
            let (held, held_inserts) = random_edit(&mut rng, &base, 'H', held_deletes);
            let (incoming, incoming_inserts) = random_edit(&mut rng, &base, 'I', incoming_deletes);
            for (from, to) in [(&base, &held), (&base, &incoming), (&held, &incoming)] {
                let edits = edits(from, to);
                assert_eq!(&apply(from, &edits).unwrap(), to);
            }
            let merged = merge(&base, &held, &incoming);
            // Deterministic, and merging a side with itself changes nothing.
            assert_eq!(merged, merge(&base, &held, &incoming));
            assert_eq!(merge(&base, &held, &held), held);
            assert_eq!(merge(&base, &held, &base), held);
            // Pure inserts and deletes never conflict: every insert survives.
            let (Some(held_inserts), Some(incoming_inserts)) = (held_inserts, incoming_inserts)
            else {
                continue;
            };
            checked += 1;
            for marker in held_inserts.iter().chain(&incoming_inserts) {
                assert!(
                    merged.contains(marker.as_str()),
                    "{marker} lost: base {base:?} held {held:?} incoming {incoming:?} -> {merged:?}"
                );
            }
        }
        assert!(
            checked > 1000,
            "only {checked} cases kept their tokens whole"
        );
    }
}
