//! The `text` index kind (SPEC 20.10.1): analyzers, posting impacts and
//! query terms.
//!
//! An [`Analyzer`] turns a string into terms; [`row_impacts`] gives a row's
//! postings (term → impact) for its `__post` rows; [`Analyzer::query`] gives
//! the terms a search binds. All three are pure functions of their inputs
//! with tables fixed in this crate (Unicode 16.0.0), never the toolchain's,
//! so every node and every build produces the same bytes (`INV-IX-1`).

mod porter2;
mod tables;

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// The text kind's shadow-row encoding (SPEC 20.2): `__terms` holds UTF-8
/// terms, `__post` one integer impact 0–255 per (term, row).
pub const ENCODING: &str = "jazz-ix-text/1";

/// The analyzer family's version. A change to segmentation, folding,
/// stemming or the impact formula is a new version (`INV-IX-5`).
pub const ANALYZER_VERSION: &str = "jazz-text/1";

/// Every analyzer [`Analyzer::parse`] accepts. Crates that cannot depend on
/// this one validate declarations against a copy of this list.
pub const ANALYZERS: &[&str] = &["jazz-text/1:en", "jazz-text/1:simple"];

/// Terms longer than this many bytes are dropped (SPEC 20.10.1, step 5).
pub const MAX_TERM_BYTES: usize = 64;

/// A field is indexed by its first this many terms; the rest of it is not
/// analyzed (SPEC 20.10.1). This bounds one write's postings; a value is
/// never refused for its length (`INV-IX-7`).
pub const MAX_FIELD_TERMS: usize = 65_536;

/// The longest search text, in UTF-8 bytes (SPEC 20.4); longer is
/// [`QueryError::TooLong`], before any analysis.
pub const MAX_QUERY_BYTES: usize = 1024;

/// The most slots a search may bind (distinct terms, plus the prefix);
/// more is [`QueryError::TooManyTerms`] (SPEC 20.4).
pub const MAX_QUERY_TERMS: usize = 32;

/// The fewest characters a prefix slot has; a shorter last word is an
/// exact term instead (SPEC 20.10.1).
pub const MIN_PREFIX_CHARS: usize = 2;

/// A text analyzer, `jazz-text/1:<lang>`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Analyzer {
    lang: Lang,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Lang {
    En,
    Simple,
}

/// An analyzer name this version does not know.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AnalyzerError {
    /// `analyzer` is not one of [`ANALYZERS`].
    Unsupported { analyzer: String },
}

impl fmt::Display for AnalyzerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AnalyzerError::Unsupported { analyzer } => write!(
                f,
                "text analyzer `{analyzer}` is not supported yet (supported: {})",
                ANALYZERS.join(", ")
            ),
        }
    }
}

impl std::error::Error for AnalyzerError {}

/// A search text that cannot be searched for.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueryError {
    /// No terms are left after analysis (only punctuation, emoji or
    /// whitespace, or only words over [`MAX_TERM_BYTES`]).
    Empty,
    /// The text is longer than [`MAX_QUERY_BYTES`].
    TooLong { bytes: usize },
    /// The text analyzes to more than [`MAX_QUERY_TERMS`] distinct slots.
    TooManyTerms,
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            QueryError::Empty => f.write_str("the search text has no words to search for"),
            QueryError::TooLong { bytes } => write!(
                f,
                "the search text is {bytes} bytes; at most {MAX_QUERY_BYTES} are supported"
            ),
            QueryError::TooManyTerms => write!(
                f,
                "the search text has more than {MAX_QUERY_TERMS} distinct words"
            ),
        }
    }
}

impl std::error::Error for QueryError {}

/// The terms a text search binds (SPEC 20.5). A row matches when every
/// slot matches one of its postings: each of `terms` exactly, and `prefix`
/// as described there. Slots number `terms.len() + prefix.is_some()`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueryTerms {
    /// Analyzed terms, deduplicated, in query order.
    pub terms: Vec<String>,
    /// The last word, when the index has `prefix` and the search asked for it.
    pub prefix: Option<PrefixTerm>,
}

/// The last query word as a prefix. It matches an indexed term `t` when
/// `t == exact`, or `lo <= t` and (`hi` is `None` or `t < hi`), comparing
/// UTF-8 bytes (SQLite's `BINARY` collation). The range is every term that
/// starts with `lo`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrefixTerm {
    /// The word analyzed as a whole word (stemmed for `en`), so a fully
    /// typed word matches its stemmed postings.
    pub exact: String,
    /// The word folded but not stemmed: the prefix itself.
    pub lo: String,
    /// The first string after every string starting with `lo`: `lo` with
    /// its last char incremented. `None` only if `lo` is all U+10FFFF.
    pub hi: Option<String>,
}

impl Analyzer {
    /// Parses an analyzer name (SPEC 20.1), e.g. `"jazz-text/1:en"`.
    pub fn parse(name: &str) -> Result<Analyzer, AnalyzerError> {
        let lang = match name {
            "jazz-text/1:en" => Lang::En,
            "jazz-text/1:simple" => Lang::Simple,
            _ => {
                return Err(AnalyzerError::Unsupported {
                    analyzer: name.to_owned(),
                });
            }
        };
        Ok(Analyzer { lang })
    }

    /// The analyzer's name, as declared.
    pub fn name(&self) -> &'static str {
        match self.lang {
            Lang::En => "jazz-text/1:en",
            Lang::Simple => "jazz-text/1:simple",
        }
    }

    /// The `_jazz_ix.encoding` of an index using this analyzer (SPEC 20.2),
    /// e.g. `"jazz-ix-text/1 jazz-text/1:en"`.
    pub fn encoding(&self) -> String {
        format!("{ENCODING} {}", self.name())
    }

    /// The terms of `text`, in order, with repeats (SPEC 20.10.1). Unlike
    /// [`row_impacts`], this does not stop at [`MAX_FIELD_TERMS`].
    pub fn terms(&self, text: &str) -> Vec<String> {
        self.term_iter(text).collect()
    }

    fn term_iter<'a>(&'a self, text: &'a str) -> impl Iterator<Item = String> + 'a {
        words(text).filter_map(|word| self.term(&fold(word)))
    }

    /// The terms a search for `text` binds. With `prefix`, the last word
    /// becomes [`QueryTerms::prefix`] instead of a term; pass `prefix` only
    /// for an index declared with `"prefix": true`.
    pub fn query(&self, text: &str, prefix: bool) -> Result<QueryTerms, QueryError> {
        if text.len() > MAX_QUERY_BYTES {
            return Err(QueryError::TooLong { bytes: text.len() });
        }
        let mut folded: Vec<String> = words(text).map(fold).filter(|w| !w.is_empty()).collect();
        let mut last = if prefix { folded.pop() } else { None };
        // A one-character last word would match too many terms as a prefix:
        // it is an exact term.
        if let Some(word) = last.take_if(|w| w.chars().count() < MIN_PREFIX_CHARS) {
            folded.push(word);
        }
        let mut terms: Vec<String> = Vec::new();
        let mut seen = BTreeSet::new();
        for word in &folded {
            if let Some(term) = self.term(word)
                && seen.insert(term.clone())
            {
                terms.push(term);
                if terms.len() > MAX_QUERY_TERMS {
                    return Err(QueryError::TooManyTerms);
                }
            }
        }
        let prefix = last.and_then(|lo| {
            if lo.len() > MAX_TERM_BYTES {
                return None;
            }
            let exact = self.term(&lo).unwrap_or_else(|| lo.clone());
            let hi = successor(&lo);
            Some(PrefixTerm { exact, lo, hi })
        });
        // The prefix slot already matches its whole-word term exactly.
        if let Some(p) = &prefix {
            terms.retain(|t| *t != p.exact);
        }
        if terms.len() + usize::from(prefix.is_some()) > MAX_QUERY_TERMS {
            return Err(QueryError::TooManyTerms);
        }
        if terms.is_empty() && prefix.is_none() {
            return Err(QueryError::Empty);
        }
        Ok(QueryTerms { terms, prefix })
    }

    /// Steps 4 and 5 on a folded word.
    fn term(&self, folded: &str) -> Option<String> {
        let term = match self.lang {
            Lang::En => porter2::stem(folded),
            Lang::Simple => folded.to_owned(),
        };
        (!term.is_empty() && term.len() <= MAX_TERM_BYTES).then_some(term)
    }
}

/// The reference length of a text field that declares none (SPEC 20.1).
pub const DEFAULT_FIELD_LENGTH: u16 = 32;

/// The largest reference length a text field may declare (SPEC 20.1).
pub const MAX_FIELD_LENGTH: u16 = 4096;

/// A row's postings: for each term of its fields, the sum over the fields
/// of the term's impact there, capped at 255 (SPEC 20.10.1). `fields` are
/// `(weight, length, value)` as declared: weight 1–15, reference length
/// 1–[`MAX_FIELD_LENGTH`] ([`DEFAULT_FIELD_LENGTH`] when the declaration
/// gives none). Null fields are left out. Each field contributes its first
/// [`MAX_FIELD_TERMS`] terms, and its length counts only those.
pub fn row_impacts(analyzer: &Analyzer, fields: &[(u8, u16, &str)]) -> BTreeMap<String, u8> {
    // Impacts are non-negative, so saturating at 255 as they add up is the
    // capped sum.
    let mut row: BTreeMap<String, u8> = BTreeMap::new();
    for &(weight, reference, value) in fields {
        let mut length = 0;
        let mut freq: BTreeMap<String, usize> = BTreeMap::new();
        for term in analyzer.term_iter(value).take(MAX_FIELD_TERMS) {
            *freq.entry(term).or_default() += 1;
            length += 1;
        }
        for (term, f) in freq {
            let sum = row.entry(term).or_default();
            *sum = sum.saturating_add(impact(weight, reference, f, length));
        }
    }
    row
}

/// The impact of a term with frequency `f` in a field of `length` terms,
/// declared with `weight` and reference length `reference`, in binary64 in
/// exactly this order (SPEC 20.10.1). Rust never fuses these operations,
/// and each is correctly rounded on every target.
pub fn impact(weight: u8, reference: u16, f: usize, length: usize) -> u8 {
    let w = f64::from(weight);
    let r = f64::from(reference);
    let f = f as f64;
    let l = length as f64;
    let x = w * 16.0 * (f * 2.2) / (f + 1.2 * (0.25 + 0.75 * l / r));
    x.floor().min(255.0) as u8
}

/// Step 1: the words of `text`, by the frozen classes of
/// [`tables::CLASSES`]: runs of word characters (letters, digits, marks and
/// ignored characters), and each standalone letter (an ideograph, a kana or
/// Thai letter, ...) with the marks after it. Separators (punctuation,
/// spaces, symbols, controls) end words and are dropped. Two runs with a
/// single `'` or `’` between them are one word, so `don't` and `Anselm's`
/// stay whole. Lazy, so a capped field analyzes only what it keeps.
fn words(text: &str) -> impl Iterator<Item = &str> {
    let mut at = 0;
    std::iter::from_fn(move || {
        let rest = &text[at..];
        let mut chars = rest.char_indices();
        // Skip to the first character that starts a word.
        let (start, first) = loop {
            let (i, c) = chars.next()?;
            match class(c) {
                Class::Word | Class::Mark | Class::Alone => break (i, c),
                Class::Separator | Class::Ignored => {}
            }
        };
        let alone = class(first) == Class::Alone;
        let mut end = start + first.len_utf8();
        while let Some(c) = rest[end..].chars().next() {
            let continues = match class(c) {
                Class::Mark | Class::Ignored => true,
                Class::Word => !alone,
                Class::Alone | Class::Separator => {
                    let before = rest[start..end].chars().next_back();
                    let after = rest[end + c.len_utf8()..].chars().next();
                    !alone && joins(c, before, after)
                }
            };
            if !continues {
                break;
            }
            end += c.len_utf8();
        }
        at += end;
        Some(&rest[start..end])
    })
}

/// Whether the separator `c` between `before` and `after` stays inside a
/// word (UAX #29's MidLetter rules): an apostrophe (`'`, `’`) or a middle
/// dot (`·`, Catalan `col·lecció`) between two word characters, and a
/// geresh or gershayim (`׳`, `״`, or ASCII `'`, `"`) between two Hebrew
/// letters (`צה״ל`, as WB7b and WB7c).
fn joins(c: char, before: Option<char>, after: Option<char>) -> bool {
    let word = |x: Option<char>| x.is_some_and(|x| class(x) == Class::Word);
    let hebrew = |x: Option<char>| x.is_some_and(hebrew_letter);
    match c {
        '\'' | '\u{2019}' | '\u{00B7}' => word(before) && word(after),
        '"' | '\u{05F3}' | '\u{05F4}' => hebrew(before) && hebrew(after),
        _ => false,
    }
}

/// UAX #29's Hebrew_Letter: the Hebrew letters and their presentation forms.
fn hebrew_letter(c: char) -> bool {
    matches!(c as u32,
        0x05D0..=0x05EA | 0x05EF..=0x05F2 | 0xFB1D | 0xFB1F..=0xFB28 | 0xFB2A..=0xFB36
        | 0xFB38..=0xFB3C | 0xFB3E | 0xFB40..=0xFB41 | 0xFB43..=0xFB44 | 0xFB46..=0xFB4F)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Class {
    Separator,
    Word,
    Mark,
    Alone,
    Ignored,
}

fn class(c: char) -> Class {
    let n = c as u32;
    let runs = tables::CLASSES;
    // The table starts at U+0000, so `i >= 1`.
    let i = runs.partition_point(|&run| run >> 3 <= n);
    match runs[i - 1] & 7 {
        1 => Class::Word,
        2 => Class::Mark,
        3 => Class::Alone,
        4 => Class::Ignored,
        _ => Class::Separator,
    }
}

/// Steps 2 and 3: simple case folding, then `jazz-text/fold/1`, dropping
/// combining diacritical marks, variation selectors and ignored characters,
/// `’` and the Hebrew geresh `׳` as `'`, and dropping the word-internal
/// middle dot and gershayim (`·`, `״`, `"`), so `col·lecció` meets
/// `collecció` and `צה״ל` meets `צה"ל` and `צהל`.
fn fold(word: &str) -> String {
    let mut out = String::with_capacity(word.len());
    for c in word.chars() {
        let c = case_fold(c);
        let n = c as u32;
        if dropped_mark(n)
            || class(c) == Class::Ignored
            || matches!(c, '\u{00B7}' | '\u{05F4}' | '"')
        {
            continue;
        }
        if matches!(c, '\u{2019}' | '\u{05F3}') {
            out.push('\'');
        } else if let Ok(n) = u16::try_from(n)
            && let Some(base) = fold_letter(n)
        {
            out.push_str(base);
        } else if let Ok(n) = u16::try_from(n)
            && let Ok(i) = tables::FOLD_ONE.binary_search_by_key(&n, |&(c, _)| c)
        {
            out.push(char::from_u32(u32::from(tables::FOLD_ONE[i].1)).expect("BMP letter"));
        } else {
            out.push(c);
        }
    }
    out
}

fn fold_letter(n: u16) -> Option<&'static str> {
    tables::FOLD_MANY
        .iter()
        .find(|&&(c, _)| c == n)
        .map(|&(_, s)| s)
}

/// Marks that fold away: the combining diacritical mark blocks (so
/// decomposed and precomposed spellings meet) and variation selectors.
fn dropped_mark(n: u32) -> bool {
    matches!(n,
        0x0300..=0x036F | 0x1AB0..=0x1AFF | 0x1DC0..=0x1DFF | 0x20D0..=0x20FF
        | 0xFE20..=0xFE2F | 0xFE00..=0xFE0F | 0xE0100..=0xE01EF)
}

/// Unicode 16.0.0 simple case folding.
fn case_fold(c: char) -> char {
    let n = c as u32;
    let runs = tables::CASE_FOLD;
    let i = runs.partition_point(|&(first, _)| first & 0x1F_FFFF <= n);
    if i == 0 {
        return c;
    }
    let (first, run) = runs[i - 1];
    let lo = first & 0x1F_FFFF;
    let stride = if first >> 31 == 1 { 2 } else { 1 };
    let last = lo + (run >> 20);
    // The low 20 bits are the delta, two's complement.
    let delta = ((run << 12) as i32) >> 12;
    if n > last || !(n - lo).is_multiple_of(stride) {
        return c;
    }
    char::from_u32(n.wrapping_add_signed(delta)).unwrap_or(c)
}

/// The least string greater than every string starting with `s`.
fn successor(s: &str) -> Option<String> {
    let mut chars: Vec<char> = s.chars().collect();
    while let Some(last) = chars.pop() {
        let next = match last {
            '\u{D7FF}' => Some('\u{E000}'),
            c => char::from_u32(c as u32 + 1),
        };
        if let Some(next) = next {
            chars.push(next);
            return Some(chars.into_iter().collect());
        }
    }
    None
}

/// White-box checks of the generated tables: they cannot be observed
/// precisely enough through the public API.
#[cfg(test)]
mod tests {
    use super::*;

    /// The segmentation classes are frozen with `jazz-text/1`: the class of
    /// every code point, hashed (FNV-1a over the class numbers in code point
    /// order, surrogates skipped), equals the digest `gen_tables.py` prints
    /// from its own classification, and the counts per class are pinned. Any change to the table is a new analyzer version.
    #[test]
    fn segmentation_classes_are_pinned() {
        let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
        let mut counts = [0usize; 5];
        for c in (0..=0x10_FFFF).filter_map(char::from_u32) {
            let k = class(c) as u64;
            counts[k as usize] += 1;
            hash ^= k;
            hash = hash.wrapping_mul(0x0100_0000_01b3);
        }
        assert_eq!(counts, [148_001, 863_203, 209, 100_646, 5]);
        assert_eq!(hash, 0x9a95_fb3a_ee2f_648a);
        // Spot checks, one per class and the separators the review found.
        assert_eq!(class('a'), Class::Word);
        assert_eq!(class('\u{0E31}'), Class::Mark);
        assert_eq!(class('東'), Class::Alone);
        assert_eq!(class('\u{00AD}'), Class::Ignored);
        assert_eq!(class('\u{FEFF}'), Class::Ignored);
        // The mid-letter marks are separators by class; `joins` keeps them
        // inside a word only between letters.
        for mid in ['\'', '"', '\u{00B7}', '\u{05F3}', '\u{05F4}', '\u{2019}'] {
            assert_eq!(class(mid), Class::Separator, "U+{:04X}", mid as u32);
        }
        assert!(joins('\u{00B7}', Some('l'), Some('l')));
        assert!(joins('"', Some('ה'), Some('ל')) && !joins('"', Some('a'), Some('b')));
        assert!(!joins('\u{05F4}', Some('ל'), None));
        for sep in [
            '\u{0964}', '\u{060C}', '\u{05C3}', '\u{0589}', '\u{3000}', '_', '\u{2028}',
        ] {
            assert_eq!(class(sep), Class::Separator, "U+{:04X}", sep as u32);
        }
    }

    /// The runs are sorted and disjoint, so the binary searches are exact.
    #[test]
    fn tables_are_sorted() {
        let first = |r: &(u32, u32)| r.0 & 0x1F_FFFF;
        let last = |r: &(u32, u32)| first(r) + (r.1 >> 20);
        let runs = tables::CASE_FOLD;
        assert!(runs.windows(2).all(|w| last(&w[0]) < first(&w[1])));
        assert!(tables::FOLD_ONE.windows(2).all(|w| w[0].0 < w[1].0));
        assert_eq!(tables::CLASSES[0] >> 3, 0);
        assert!(tables::CLASSES.windows(2).all(|w| w[0] >> 3 < w[1] >> 3));
    }

    /// Folding is idempotent: a folded word folds to itself, so query text
    /// and indexed text meet whichever is folded twice.
    #[test]
    fn fold_is_idempotent_over_its_tables() {
        let mut all = String::new();
        for &(first, run) in tables::CASE_FOLD {
            let lo = first & 0x1F_FFFF;
            all.extend((lo..=lo + (run >> 20)).filter_map(char::from_u32));
        }
        for &(c, _) in tables::FOLD_ONE {
            all.push(char::from_u32(c.into()).unwrap());
        }
        let once = fold(&all);
        assert_eq!(fold(&once), once);
    }

    #[test]
    fn successor_skips_surrogates_and_carries() {
        assert_eq!(successor("ab").as_deref(), Some("ac"));
        assert_eq!(successor("a\u{D7FF}").as_deref(), Some("a\u{E000}"));
        assert_eq!(successor("a\u{10FFFF}").as_deref(), Some("b"));
        assert_eq!(successor("\u{10FFFF}"), None);
    }
}
