//! Three-way merge of a text-merging string column (SPEC 4 §4.3).
//!
//! A column declared `text`, `markdown` or `html` stores its value as a
//! plain string; writes travel as plain values. Only the authority merges:
//! when the version a write was built on (its base, SPEC 4 §4.6) is not the
//! last to author the column, it merges the base's value, the value it
//! holds and the write's value: each side's changes are diffed against the
//! base and both applied. For a write its merge changed, the authority
//! keeps the edits back to its value as sent (`_jazz_text_in`, see
//! [`encode_in`]), so a later write built on it merges over what its writer
//! saw.
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

use crate::options::{Error, ErrorCode};
use crate::schema::TextFormat;
use std::collections::HashMap;

/// Past this many edit steps, one search of a diff stops looking for the
/// shortest edit and splits where it got furthest instead: the diff stays
/// a fine-grained (if not minimal) one, and a heavily edited value costs
/// time in proportion to its size times this bound, not its size squared.
const MAX_EDIT_DISTANCE: usize = 256;

fn malformed(message: impl std::fmt::Display) -> Error {
    Error::new(
        ErrorCode::Storage,
        format!("malformed text edit: {message}"),
    )
}

/// One replacement of `value[start..end]` (byte offsets) by `insert`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Edit {
    pub start: usize,
    pub end: usize,
    pub insert: String,
}

/// The edits that turn `from` into `to`, token by token past their common
/// prefix and suffix (none when they are equal), so a value changed in a few
/// places keeps only those places.
pub(crate) fn edits(format: TextFormat, from: &str, to: &str) -> Vec<Edit> {
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
    let (old, new) = (
        &from[prefix..from.len() - suffix],
        &to[prefix..to.len() - suffix],
    );
    let (old_tokens, new_tokens) = (tokenize(format, old), tokenize(format, new));
    let offsets = |tokens: &[&str]| {
        let mut at = 0;
        let mut offsets = Vec::with_capacity(tokens.len() + 1);
        offsets.push(0);
        for token in tokens {
            at += token.len();
            offsets.push(at);
        }
        offsets
    };
    let (old_at, new_at) = (offsets(&old_tokens), offsets(&new_tokens));
    diff(&old_tokens, &new_tokens)
        .into_iter()
        .map(|hunk| Edit {
            start: prefix + old_at[hunk.old.0],
            end: prefix + old_at[hunk.old.1],
            insert: new[new_at[hunk.new.0]..new_at[hunk.new.1]].to_owned(),
        })
        .collect()
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

/// A version's `_jazz_text_in` entry: per stored column its merge changed,
/// the edits from the merged value back to the write's value as sent.
pub(crate) fn encode_in(entries: &[(String, Vec<Edit>)]) -> Vec<u8> {
    fn put(out: &mut Vec<u8>, n: usize) {
        out.extend_from_slice(&(n as u64).to_le_bytes());
    }
    let mut out = Vec::new();
    put(&mut out, entries.len());
    for (column, edits) in entries {
        put(&mut out, column.len());
        out.extend_from_slice(column.as_bytes());
        put(&mut out, edits.len());
        for edit in edits {
            put(&mut out, edit.start);
            put(&mut out, edit.end);
            put(&mut out, edit.insert.len());
            out.extend_from_slice(edit.insert.as_bytes());
        }
    }
    out
}

/// The entries [`encode_in`] wrote.
pub(crate) fn decode_in(mut bytes: &[u8]) -> Result<Vec<(String, Vec<Edit>)>, Error> {
    fn take<'a>(bytes: &mut &'a [u8], n: usize) -> Result<&'a [u8], Error> {
        if bytes.len() < n {
            return Err(malformed("truncated"));
        }
        let (head, rest) = bytes.split_at(n);
        *bytes = rest;
        Ok(head)
    }
    fn number(bytes: &mut &[u8]) -> Result<usize, Error> {
        let raw: [u8; 8] = take(bytes, 8)?.try_into().expect("eight bytes");
        usize::try_from(u64::from_le_bytes(raw)).map_err(|_| malformed("offset too large"))
    }
    fn text(bytes: &mut &[u8]) -> Result<String, Error> {
        let n = number(bytes)?;
        String::from_utf8(take(bytes, n)?.to_vec()).map_err(|_| malformed("not UTF-8"))
    }
    let mut entries = Vec::new();
    for _ in 0..number(&mut bytes)? {
        let column = text(&mut bytes)?;
        let mut edits = Vec::new();
        for _ in 0..number(&mut bytes)? {
            let start = number(&mut bytes)?;
            let end = number(&mut bytes)?;
            edits.push(Edit {
                start,
                end,
                insert: text(&mut bytes)?,
            });
        }
        entries.push((column, edits));
    }
    if !bytes.is_empty() {
        return Err(malformed("trailing bytes"));
    }
    Ok(entries)
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
    // A change both made applies once. Hunks of one diff never share a base
    // span (touching ones are coalesced), so each has at most one twin.
    let mine: HashMap<(usize, usize), (usize, usize)> =
        ours.iter().map(|hunk| (hunk.old, hunk.new)).collect();
    theirs.retain(|hunk| {
        mine.get(&hunk.old).is_none_or(|&(start, end)| {
            held_tokens[start..end] != incoming_tokens[hunk.new.0..hunk.new.1]
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
    // A side that only inserts replaces nothing: the other side's
    // replacements and its inserts all apply.
    let inserts_beside = cluster.iter().filter(|c| c.held).all(Change::inserts_only)
        || cluster.iter().filter(|c| !c.held).all(Change::inserts_only);
    if one_side || unions || inserts_beside {
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
/// searches meeting, or past [`MAX_EDIT_DISTANCE`] the furthest point the
/// forward search reached.
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
    // Too costly to find the shortest edit: split where the forward search
    // got furthest. The part before it costs at most `max` edits; the rest
    // is diffed on its own.
    let mut best: Option<(isize, isize)> = None;
    let mut k = -max;
    while k <= max {
        let x = forward[(k + offset) as usize];
        let y = x - k;
        let on_grid = (0..=n).contains(&x) && (0..=m).contains(&y);
        if on_grid && best.is_none_or(|(bx, by)| x + y > bx + by) {
            best = Some((x, y));
        }
        k += 2;
    }
    best.and_then(|(x, y)| split(x, y, x, y, n, m))
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

/// How far a tag, link destination or entity is looked for (each search
/// also stops at what cannot be inside one): an unclosed `<`, `](` or `&`
/// is then one char, and cutting a value stays linear.
const TAG_MAX_BYTES: usize = 512;
const ENTITY_MAX_BYTES: usize = 32;

/// The byte length of the token `rest` starts with (at least one char),
/// with the marks, joiners and modifiers that complete its last character.
fn token_len(format: TextFormat, rest: &str, line_start: bool) -> usize {
    let mut len = base_token_len(format, rest, line_start);
    // Two regional indicators make one flag.
    let regional = |c: Option<char>| c.is_some_and(|c| ('\u{1F1E6}'..='\u{1F1FF}').contains(&c));
    if regional(rest.chars().next()) && regional(rest[len..].chars().next()) {
        len += 4;
    }
    len + grapheme_tail(&rest[len..])
}

/// How much of `rest` continues the character before it: combining marks,
/// variation selectors and emoji modifiers, and after a zero-width joiner
/// the character it joins (with its own tail).
fn grapheme_tail(rest: &str) -> usize {
    let mut at = 0;
    let mut chars = rest.chars();
    while let Some(c) = chars.next() {
        if c == '\u{200d}' {
            at += c.len_utf8();
            match chars.next() {
                Some(joined) => at += joined.len_utf8(),
                None => break,
            }
        } else if extends(c) {
            at += c.len_utf8();
        } else {
            break;
        }
    }
    at
}

fn base_token_len(format: TextFormat, rest: &str, line_start: bool) -> usize {
    let first = rest.chars().next().expect("non-empty");
    let run = |pred: &dyn Fn(char) -> bool| -> usize {
        rest.char_indices()
            .find(|(_, c)| !pred(*c))
            .map_or(rest.len(), |(at, _)| at)
    };
    match format {
        TextFormat::Html => {
            if first == '<'
                && let Some(end) = rest
                    .bytes()
                    .take(TAG_MAX_BYTES)
                    .skip(1)
                    .position(|b| b == b'>' || b == b'<')
                    .map(|at| at + 1)
                && rest.as_bytes()[end] == b'>'
            {
                return end + 1;
            }
            if first == '&'
                && let Some(end) = rest
                    .bytes()
                    .take(ENTITY_MAX_BYTES)
                    .skip(1)
                    .position(|b| !(b.is_ascii_alphanumeric() || b == b'#'))
                    .map(|at| at + 1)
                && rest.as_bytes()[end] == b';'
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
                && let Some(end) = rest
                    .bytes()
                    .take(TAG_MAX_BYTES)
                    .skip(2)
                    .position(|b| matches!(b, b')' | b'\n' | b'[' | b']'))
                    .map(|at| at + 2)
                && rest.as_bytes()[end] == b')'
            {
                return end + 1;
            }
            if rest.starts_with("![") {
                return 2;
            }
        }
        TextFormat::Plain => {}
    }
    if word(first) {
        run(&|c| word(c) || extends(c))
    } else {
        first.len_utf8()
    }
}

/// Whether `c` belongs to a word: an ASCII letter or digit, or any other
/// character outside the punctuation, symbol and space blocks and the
/// scripts written without spaces between words (whose characters are
/// tokens of their own, as are emoji). The tables are fixed here, not taken
/// from the toolchain's Unicode version, so every authority cuts a value
/// the same way.
fn word(c: char) -> bool {
    if c.is_ascii() {
        return c.is_ascii_alphanumeric();
    }
    let c = c as u32;
    let not_word: &[(u32, u32)] = &[
        (0x0080, 0x00BF), // Latin-1 controls, punctuation and symbols
        (0x00D7, 0x00D7), // ×
        (0x00F7, 0x00F7), // ÷
        (0x0E00, 0x0FFF), // Thai, Lao, Tibetan
        (0x1000, 0x109F), // Myanmar
        (0x1680, 0x1680), // Ogham space
        (0x1780, 0x17FF), // Khmer
        (0x2000, 0x2BFF), // spaces, punctuation, symbols, arrows, math, shapes
        (0x2E00, 0x2E7F), // supplemental punctuation
        (0x2E80, 0x303F), // CJK radicals, ideographic description, CJK punctuation
        (0x3040, 0x31FF), // kana, bopomofo, CJK strokes
        (0x3400, 0x4DBF), // CJK extension A
        (0x4E00, 0x9FFF), // CJK unified ideographs
        (0xA000, 0xA4CF), // Yi
        (0xF900, 0xFAFF), // CJK compatibility ideographs
        (0xFE30, 0xFE4F), // CJK compatibility forms
        (0xFE50, 0xFE6F), // small form variants
        (0xFF00, 0xFF0F), // fullwidth punctuation
        (0xFF1A, 0xFF20),
        (0xFF3B, 0xFF40),
        (0xFF5B, 0xFF65),
        (0xFF66, 0xFF9F),   // halfwidth kana
        (0xFFF0, 0xFFFF),   // specials
        (0x1F000, 0x1FAFF), // emoji and pictographs
        (0x20000, 0x3FFFF), // CJK extensions B on
    ];
    !not_word.iter().any(|&(lo, hi)| (lo..=hi).contains(&c))
}

/// Whether `c` completes the character before it rather than starting one:
/// a combining mark, variation selector, emoji skin-tone modifier or tag.
fn extends(c: char) -> bool {
    let c = c as u32;
    let marks: &[(u32, u32)] = &[
        (0x0300, 0x036F),
        // Marks of the scripts whose letters are tokens of their own.
        (0x0E31, 0x0E31),
        (0x0E34, 0x0E3A),
        (0x0E47, 0x0E4E),
        (0x0EB1, 0x0EB1),
        (0x0EB4, 0x0EBC),
        (0x0EC8, 0x0ECE),
        (0x0F18, 0x0F19),
        (0x0F35, 0x0F35),
        (0x0F37, 0x0F37),
        (0x0F39, 0x0F39),
        (0x0F3E, 0x0F3F),
        (0x0F71, 0x0F84),
        (0x0F86, 0x0F87),
        (0x0F8D, 0x0FBC),
        (0x0FC6, 0x0FC6),
        (0x102B, 0x103E),
        (0x1056, 0x1059),
        (0x105E, 0x1060),
        (0x1062, 0x1064),
        (0x1067, 0x106D),
        (0x1071, 0x1074),
        (0x1082, 0x108D),
        (0x108F, 0x108F),
        (0x109A, 0x109D),
        (0x17B4, 0x17D3),
        (0x17DD, 0x17DD),
        (0x3099, 0x309A),
        (0x1AB0, 0x1AFF),
        (0x1DC0, 0x1DFF),
        (0x20D0, 0x20FF),
        (0xFE00, 0xFE0F),
        (0xFE20, 0xFE2F),
        (0x1F3FB, 0x1F3FF),
        (0xE0020, 0xE007F),
        (0xE0100, 0xE01EF),
    ];
    marks.iter().any(|&(lo, hi)| (lo..=hi).contains(&c))
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
            let edits = edits(TextFormat::Plain, from, to);
            assert_eq!(apply(from, &edits).unwrap(), to, "{from:?} -> {to:?}");
            let entries = vec![("body".to_owned(), edits.clone())];
            assert_eq!(decode_in(&encode_in(&entries)).unwrap(), entries);
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

    #[test]
    fn an_insert_beside_a_replacement_keeps_both() {
        // The held write replaces "two three"; the incoming one inserts
        // inside it. Neither replaces what the other did.
        assert_eq!(
            merge(
                "one two three four",
                "one 2-3 four",
                "one two and three four"
            ),
            "one 2-3and  four"
        );
    }

    #[test]
    fn unspaced_scripts_and_graphemes_tokenize_by_character() {
        let tokens = tokenize(TextFormat::Plain, "日本語の文章です");
        assert_eq!(tokens.len(), 8);
        // Two inserts into one sentence both land.
        assert_eq!(
            merge(
                "日本語の文章です",
                "日本語の長い文章です",
                "日本語の文章でした"
            ),
            "日本語の長い文章でした"
        );
        assert_eq!(
            tokenize(TextFormat::Plain, "cafe\u{301} au lait")[0],
            "cafe\u{301}"
        );
        assert_eq!(tokenize(TextFormat::Plain, "👍🏽!"), vec!["👍🏽", "!"]);
        assert_eq!(tokenize(TextFormat::Plain, "🇩🇪🇫🇷"), vec!["🇩🇪", "🇫🇷"]);
        assert_eq!(
            tokenize(TextFormat::Plain, "กินข้าว"),
            vec!["กิ", "น", "ข้", "า", "ว"]
        );
        assert_eq!(
            tokenize(TextFormat::Plain, "か\u{3099}"),
            vec!["か\u{3099}"]
        );
        assert_eq!(
            tokenize(TextFormat::Plain, "👩\u{200d}💻 x")[0],
            "👩\u{200d}💻"
        );
    }

    #[test]
    fn unclosed_delimiters_are_one_char_and_cutting_stays_linear() {
        assert_eq!(
            tokenize(TextFormat::Html, "< b then\n<p>")[..3],
            ["<", " ", "b"]
        );
        assert!(tokenize(TextFormat::Html, "a<p>b").contains(&"<p>"));
        assert_eq!(tokenize(TextFormat::Html, "&amp x;")[0], "&");
        assert_eq!(tokenize(TextFormat::Markdown, "](x\ny)")[0], "]");
        for (format, unit) in [
            (TextFormat::Html, "1 < 2 "),
            (TextFormat::Html, "a & b "),
            (TextFormat::Markdown, "]( x "),
        ] {
            let text = unit.repeat(400_000 / unit.len());
            let started = std::time::Instant::now();
            let tokens = tokenize(format, &text);
            assert_eq!(tokens.concat(), text);
            assert!(
                started.elapsed() < std::time::Duration::from_secs(2),
                "{unit:?} took {:?}",
                started.elapsed()
            );
        }
    }

    #[test]
    fn dissimilar_large_values_merge_in_linear_time() {
        // Every token differs: one hunk per token on each side.
        let base: String = (0..120_000).map(|i| format!("b{i:06} ")).collect();
        let held: String = (0..120_000).map(|i| format!("h{i:06} ")).collect();
        let incoming: String = (0..120_000).map(|i| format!("i{i:06} ")).collect();
        assert!(base.len() > 900_000, "{}", base.len());
        let started = std::time::Instant::now();
        let merged = merge(&base, &held, &incoming);
        assert!(
            merged.contains("i119999 "),
            "{:?}",
            &merged[merged.len() - 80..]
        );
        let limit = if cfg!(debug_assertions) { 60 } else { 5 };
        assert!(
            started.elapsed() < std::time::Duration::from_secs(limit),
            "took {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn stored_edits_keep_only_the_changed_places() {
        let from = format!("start {} end", "middle ".repeat(10_000));
        let to = format!("begin {} finish", "middle ".repeat(10_000));
        let edits = edits(TextFormat::Plain, &from, &to);
        assert_eq!(apply(&from, &edits).unwrap(), to);
        assert!(edits.iter().map(|e| e.insert.len()).sum::<usize>() < 20);
    }

    #[test]
    fn heavily_edited_values_lose_no_edits() {
        // Distinct words, so every edit lands in a token of its own.
        let words: Vec<String> = (0..60_000).map(|i| format!("w{i}")).collect();
        let base = words.join(" ");
        let edited = |side: char, every: usize, from: usize| {
            let mut out = words.clone();
            let mut markers = Vec::new();
            for i in (from..out.len()).step_by(every) {
                let marker = format!("{side}{i}");
                out[i] = format!("{} {marker}", out[i]);
                markers.push(marker);
            }
            (out.join(" "), markers)
        };
        // Thousands of edits a side: far past one search's bound.
        let (held, held_markers) = edited('H', 13, 0);
        let (incoming, incoming_markers) = edited('I', 17, 5);
        let merged = merge(&base, &held, &incoming);
        let words: std::collections::HashSet<&str> = merged.split(' ').collect();
        let lost = held_markers
            .iter()
            .chain(&incoming_markers)
            .filter(|marker| !words.contains(marker.as_str()))
            .count();
        assert_eq!(lost, 0, "{lost} edits lost");
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
                let edits = edits(TextFormat::Plain, from, to);
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
