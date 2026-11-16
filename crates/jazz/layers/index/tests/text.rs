//! The text index kind's analysis, impacts and query terms (SPEC 20.10.1),
//! through `jazz_index::text`'s public API.
//!
//! Byte fixtures in `crates/jazz/SPEC/fixtures/ix/text/` pin the output for
//! every case below; `JAZZ_UPDATE_FIXTURES=1` rewrites them after an
//! intended change (which is a new analyzer version, `INV-IX-5`).

#![cfg(feature = "text")]

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::PathBuf;

use jazz_index::text::{
    ANALYZER_VERSION, ANALYZERS, Analyzer, AnalyzerError, DEFAULT_FIELD_LENGTH, ENCODING,
    MAX_FIELD_LENGTH, MAX_TERM_BYTES, PrefixTerm, QueryError, QueryTerms, impact, row_impacts,
};

fn en() -> Analyzer {
    Analyzer::parse("jazz-text/1:en").unwrap()
}

fn simple() -> Analyzer {
    Analyzer::parse("jazz-text/1:simple").unwrap()
}

fn terms(a: Analyzer, text: &str) -> Vec<String> {
    a.terms(text)
}

/// Both declared analyzers parse, name themselves and give the
/// `_jazz_ix.encoding` the store records; any other name is refused as not
/// supported yet, naming it.
#[test]
fn analyzers_parse_and_unknown_ones_are_not_supported_yet() {
    for name in ANALYZERS {
        let a = Analyzer::parse(name).unwrap();
        assert_eq!(a.name(), *name);
        assert_eq!(a.encoding(), format!("{ENCODING} {name}"));
        assert!(name.starts_with(ANALYZER_VERSION));
    }
    assert_eq!(en().encoding(), "jazz-ix-text/1 jazz-text/1:en");
    for name in ["jazz-text/1:de", "jazz-text/2:en", "en", ""] {
        let err = Analyzer::parse(name).unwrap_err();
        assert_eq!(
            err,
            AnalyzerError::Unsupported {
                analyzer: name.into()
            }
        );
        assert!(err.to_string().contains("not supported yet"));
    }
}

/// Words are cut by `jazz-text`'s plain units: punctuation, symbols, emoji
/// and spaces separate words and are dropped; apostrophes between letters
/// stay inside a word, and `en` strips possessives.
#[test]
fn words_split_on_punctuation_and_keep_inner_apostrophes() {
    assert_eq!(
        terms(simple(), "Hello, world! (it's) e-mail snake_case a/b"),
        [
            "hello", "world", "it's", "e", "mail", "snake", "case", "a", "b"
        ]
    );
    // A curly apostrophe is the same word as a straight one.
    assert_eq!(terms(simple(), "it’s"), ["it's"]);
    // Possessives and plural possessives meet the bare noun in `en`.
    assert_eq!(terms(en(), "Anselm's"), terms(en(), "Anselm"));
    assert_eq!(terms(en(), "the dogs' bowls"), ["the", "dog", "bowl"]);
    assert_eq!(terms(en(), "don't won't"), ["don't", "won't"]);
    // Leading and trailing apostrophes are quotes, not part of a word.
    assert_eq!(terms(simple(), "'quoted'"), ["quoted"]);
}

/// Digits are words: numbers split at their separators, and letters and
/// digits run together.
#[test]
fn numbers_split_at_separators() {
    assert_eq!(
        terms(simple(), "3.14 1,000 v1.2 2026-11-15 4x4 ٣٤"),
        [
            "3", "14", "1", "000", "v1", "2", "2026", "11", "15", "4x4", "٣٤"
        ]
    );
}

/// Ideographs, kana and Thai letters are one term each (their scripts have
/// no spaces between words, so a search matches their characters); Hangul,
/// Arabic, Devanagari and Cyrillic words stay whole.
#[test]
fn scripts_without_spaces_index_each_character() {
    assert_eq!(
        terms(simple(), "東京タワー"),
        ["東", "京", "タ", "ワ", "ー"]
    );
    assert_eq!(terms(simple(), "ภาษาไทย").len(), 7);
    assert_eq!(
        terms(simple(), "한국어 مرحبا नमस्ते Привет"),
        ["한국어", "مرحبا", "नमस्ते", "привет"]
    );
    // Mixed scripts in one string split where the script changes to an
    // unspaced one, and stay together otherwise.
    assert_eq!(
        terms(simple(), "Jazz東京abcΩμέγα"),
        ["jazz", "東", "京", "abcωμεγα"]
    );
}

/// Emoji, flags, ZWJ sequences, skin tones and symbols are no terms: a text of
/// only those has nothing to search for.
#[test]
fn emoji_and_symbols_are_dropped() {
    assert_eq!(terms(simple(), "👍🏽 🇩🇪 👩‍👩‍👧 ★ € © ½ jazz🎷"), ["jazz"]);
    assert_eq!(simple().query("🎷 — !!", false), Err(QueryError::Empty));
    assert_eq!(simple().query("   ", true), Err(QueryError::Empty));
}

/// Case folds by Unicode simple case folding, so `ß` stays a letter of its
/// own there; `jazz-text/fold/1` then folds it to `ss`, with the other
/// ligatures, so `Straße` and `STRASSE` meet. `İ` folds to `i`, and every
/// sigma to `σ`.
#[test]
fn case_folding_meets_the_usual_spellings() {
    assert_eq!(
        terms(simple(), "Straße STRASSE strasse ẞ"),
        ["strasse", "strasse", "strasse", "ss"]
    );
    assert_eq!(
        terms(simple(), "İstanbul ISTANBUL"),
        ["istanbul", "istanbul"]
    );
    assert_eq!(terms(simple(), "ΣΊΣΥΦΟΣ σίσυφος"), ["σισυφοσ", "σισυφοσ"]);
    assert_eq!(
        terms(simple(), "Æsir ŒUVRE ﬁne Łódź Ørsted"),
        ["aesir", "oeuvre", "fine", "lodz", "orsted"]
    );
}

/// Diacritics fold away in Latin, Greek and Cyrillic, written precomposed
/// or with combining marks, and fullwidth letters and digits fold to ASCII.
#[test]
fn diacritics_fold_whether_precomposed_or_combining() {
    let precomposed = "café naïve Ångström ἀρχή Ёлка";
    let combining =
        "cafe\u{301} nai\u{308}ve A\u{30a}ngstro\u{308}m α\u{313}ρχη\u{301} Е\u{308}лка";
    assert_eq!(terms(simple(), precomposed), terms(simple(), combining));
    assert_eq!(
        terms(simple(), precomposed),
        ["cafe", "naive", "angstrom", "αρχη", "елка"]
    );
    assert_eq!(terms(simple(), "ＪＡＺＺ１２"), ["jazz12"]);
    // Other scripts keep their marks.
    assert_eq!(terms(simple(), "नमस्ते"), ["नमस्ते"]);
}

/// `en` stems with Snowball English; `simple` does not.
#[test]
fn en_stems_and_simple_does_not() {
    assert_eq!(
        terms(
            en(),
            "Running runs ran connection connected generously skies"
        ),
        ["run", "run", "ran", "connect", "connect", "generous", "sky"]
    );
    assert_eq!(
        terms(simple(), "Running connections"),
        ["running", "connections"]
    );
    // Stop words are kept: AND semantics make them harmless.
    assert_eq!(
        terms(en(), "To be or not to be"),
        ["to", "be", "or", "not", "to", "be"]
    );
}

/// Terms over 64 bytes are dropped, counted in UTF-8 bytes after folding
/// and stemming.
#[test]
fn terms_over_64_bytes_are_dropped() {
    let at = "a".repeat(MAX_TERM_BYTES);
    let over = "a".repeat(MAX_TERM_BYTES + 1);
    assert_eq!(
        terms(simple(), &format!("{at} {over} ok")),
        [at.clone(), "ok".into()]
    );
    // 32 two-byte letters fit, 33 do not.
    let greek = "α".repeat(32);
    assert_eq!(terms(simple(), &format!("{greek} {greek}α")), [greek]);
    assert_eq!(simple().query(&over, false), Err(QueryError::Empty));
}

/// The impact formula, evaluated in binary64 in SPEC order, at the default
/// reference length (32 terms) and around it.
#[test]
fn impacts_follow_the_formula() {
    // f = 1 at the reference length: 16 * 2.2 / 2.2 = 16 per weight.
    assert_eq!(impact(1, 32, 1, 32), 16);
    assert_eq!(impact(4, 32, 1, 32), 64);
    // Short fields score higher, long ones lower, never below zero.
    assert_eq!(impact(1, 32, 1, 1), 26);
    assert_eq!(impact(1, 32, 1, 1000), 1);
    assert_eq!(impact(1, 32, 1, 5000), 0);
    // Repeats saturate; heavy weights cap at 255.
    assert_eq!(impact(1, 32, 10, 32), 31);
    assert_eq!(impact(15, 32, 10, 32), 255);
}

/// A field's declared reference length replaces 32: a field as long as its
/// reference scores 16 per weight for one occurrence whatever the length, so
/// a long body declared `length: 256` keeps scoring where the default would
/// flatten it to 0 or 1.
#[test]
fn reference_length_is_the_fields_declared_length() {
    assert_eq!(DEFAULT_FIELD_LENGTH, 32);
    for reference in [1u16, 16, 256, MAX_FIELD_LENGTH] {
        assert_eq!(impact(1, reference, 1, usize::from(reference)), 16);
    }
    assert_eq!(impact(1, 256, 1, 1000), 7);
    assert_eq!(impact(1, 32, 1, 1000), 1);
    assert_eq!(impact(1, 4096, 1, 5000), 14);
    // Against a shorter reference the same title counts as longer, so it
    // scores lower (80 against 92).
    assert_eq!((impact(4, 16, 1, 8), impact(4, 32, 1, 8)), (80, 92));
    assert_eq!(
        row_impacts(&en(), &[(1, 256, "jazz")]),
        BTreeMap::from([("jazz".to_owned(), 27)])
    );
}

/// A row's postings sum each term's impact over its fields, capped at 255,
/// and each field's length is its own.
///
/// ```text
/// title (w=4): "Jazz"            jazz: 4*16*2.2/(1+1.2*(0.25+0.75/32)) = 106
/// body  (w=1): "jazz and blues"  jazz: 16*2.2/(1+1.2*(0.25+2.25/32))   = 25
///                                 => jazz 131, and 25, blue 25
/// ```
#[test]
fn row_impacts_sum_over_fields() {
    let row = row_impacts(&en(), &[(4, 32, "Jazz"), (1, 32, "jazz and blues")]);
    let expected: BTreeMap<String, u8> = [("jazz", 131), ("and", 25), ("blue", 25)]
        .into_iter()
        .map(|(t, i)| (t.to_owned(), i))
        .collect();
    assert_eq!(row, expected);
    // Capped at 255 per row.
    let capped = row_impacts(&en(), &[(15, 32, "jazz"), (15, 32, "jazz")]);
    assert_eq!(capped["jazz"], 255);
    // A field with no words adds nothing.
    assert!(row_impacts(&en(), &[(1, 32, ""), (2, 32, "🎷")]).is_empty());
}

/// A query's terms are the analyzed words, deduplicated; with `prefix`, the
/// last word is a prefix slot instead: folded but unstemmed for the range,
/// and stemmed for an exact match of a fully typed word.
#[test]
fn queries_dedupe_terms_and_split_off_the_prefix() {
    assert_eq!(
        en().query("Jazz jazz running", false),
        Ok(QueryTerms {
            terms: vec!["jazz".into(), "run".into()],
            prefix: None
        })
    );
    assert_eq!(
        en().query("jazz Runni", true),
        Ok(QueryTerms {
            terms: vec!["jazz".into()],
            prefix: Some(PrefixTerm {
                exact: "runni".into(),
                lo: "runni".into(),
                hi: Some("runnj".into())
            })
        })
    );
    let full = en().query("Running", true).unwrap();
    assert!(full.terms.is_empty());
    let p = full.prefix.unwrap();
    assert_eq!((p.exact.as_str(), p.lo.as_str()), ("run", "running"));
    // A prefix that folds: the range holds folded terms.
    let p = simple().query("Cafe\u{301}", true).unwrap().prefix.unwrap();
    assert_eq!((p.lo.as_str(), p.hi.as_deref()), ("cafe", Some("caff")));
}

/// A prefix matches exactly the indexed terms starting with it under
/// SQLite's BINARY collation (UTF-8 byte order), as `term >= lo AND term < hi`.
#[test]
fn prefix_range_holds_exactly_the_terms_starting_with_it() {
    let indexed = [
        "caf",
        "cafe",
        "cafes",
        "cafeteria",
        "caff",
        "cafz",
        "café",
        "cafe\u{10ffff}",
    ];
    let p = simple().query("cafe", true).unwrap().prefix.unwrap();
    let hi = p.hi.unwrap();
    for t in indexed {
        let in_range = t >= p.lo.as_str() && t < hi.as_str();
        assert_eq!(in_range, t.starts_with("cafe"), "{t:?}");
    }
}

// ---------------------------------------------------------------------------
// Byte fixtures
// ---------------------------------------------------------------------------

const TERM_CASES: &[&str] = &[
    "",
    "Hello, World!",
    "Don't stop the Running dogs’ owners — Anselm's café",
    "'quoted' rock'n'roll O'Neill",
    "3.14 1,000 v1.2 2026-11-15 4x4 ٣٤ ½",
    "東京タワー 한국어 ภาษาไทย",
    "Jazz東京abcΩμέγα मराठी مرحبا",
    "👍🏽 🇩🇪 👩‍👩‍👧 ★ € © jazz🎷",
    "Straße STRASSE ẞ İstanbul ΣΊΣΥΦΟΣ",
    "cafe\u{301} nai\u{308}ve A\u{30a}ngstro\u{308}m Ёлка й ǅemal",
    "Æsir ŒUVRE ﬁne Łódź Ørsted ＪＡＺＺ１２",
    "generously connections communication universities interval skies dying",
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "\u{301}x \u{200d} tab\tnew\nline\u{a0}nbsp",
];

const IMPACT_CASES: &[&[(u8, u16, &str)]] = &[
    &[(1, 32, "jazz")],
    &[(4, 32, "Jazz"), (1, 32, "jazz and blues")],
    &[(15, 32, "jazz jazz jazz"), (15, 32, "jazz")],
    &[(2, 32, "the quick brown fox jumps over the lazy dog the end")],
    &[(1, 32, ""), (3, 32, "🎷")],
    &[(1, 32, "Running runner runs ran")],
    &[
        (4, 16, "Jazz in the park"),
        (
            1,
            256,
            "jazz jazz and the blues, all summer long in the park",
        ),
    ],
    &[(1, 1, "jazz blues"), (1, 4096, "jazz")],
];

const QUERY_CASES: &[(&str, bool)] = &[
    ("Jazz jazz running", false),
    ("jazz Runni", true),
    ("Running", true),
    ("Café\u{301}", true),
    ("東京", true),
    ("🎷 — !!", false),
    ("   ", true),
];

fn hex(s: &str) -> String {
    s.bytes().map(|b| format!("{b:02x}")).collect()
}

fn render_terms() -> String {
    let mut out = String::from(
        "# jazz-ix-text/1 terms (SPEC 20.10.1). Per case: `analyzer`, `text <utf8 hex>`,\n\
         # then one `term <utf8 hex>` line per term in order. `#` starts a readable comment.\n",
    );
    for name in ANALYZERS {
        let a = Analyzer::parse(name).unwrap();
        for text in TERM_CASES {
            writeln!(out, "\nanalyzer {name}\ntext {}  # {text:?}", hex(text)).unwrap();
            for t in a.terms(text) {
                writeln!(out, "term {}  # {t:?}", hex(&t)).unwrap();
            }
        }
    }
    out
}

fn render_impacts() -> String {
    let mut out = String::from(
        "# jazz-ix-text/1 postings (SPEC 20.10.1). Per case: `analyzer`, one\n\
         # `field <weight> <length> <utf8 hex>` per field, then `posting <term utf8 hex> <impact>`\n\
         # in term byte order.\n",
    );
    for name in ANALYZERS {
        let a = Analyzer::parse(name).unwrap();
        for fields in IMPACT_CASES {
            writeln!(out, "\nanalyzer {name}").unwrap();
            for (w, length, text) in *fields {
                writeln!(out, "field {w} {length} {}  # {text:?}", hex(text)).unwrap();
            }
            for (t, i) in row_impacts(&a, fields) {
                writeln!(out, "posting {} {i}  # {t:?}", hex(&t)).unwrap();
            }
        }
    }
    out.push_str(
        "\n# impact(w, R, f, L) over a grid, R the field's reference length:\n\
         # `impact <w> <R> <f> <L> <impact>`.\n",
    );
    for w in [1u8, 2, 4, 8, 15] {
        for r in [1u16, 16, 32, 256, 4096] {
            for f in [1usize, 2, 3, 5, 10, 100] {
                for l in [1usize, 2, 8, 31, 32, 33, 100, 256, 1000, 10_000] {
                    if f <= l {
                        writeln!(out, "impact {w} {r} {f} {l} {}", impact(w, r, f, l)).unwrap();
                    }
                }
            }
        }
    }
    out
}

fn render_queries() -> String {
    let mut out = String::from(
        "# jazz-ix-text/1 query terms (SPEC 20.5, 20.10.1). Per case: `analyzer`,\n\
         # `query <prefix 0|1> <utf8 hex>`, then `term <utf8 hex>` per exact slot and\n\
         # `prefix <exact hex> <lo hex> <hi hex or ->`, or `error empty`.\n",
    );
    for name in ANALYZERS {
        let a = Analyzer::parse(name).unwrap();
        for (text, prefix) in QUERY_CASES {
            writeln!(
                out,
                "\nanalyzer {name}\nquery {} {}  # {text:?}",
                u8::from(*prefix),
                hex(text)
            )
            .unwrap();
            match a.query(text, *prefix) {
                Err(QueryError::Empty) => out.push_str("error empty\n"),
                Ok(q) => {
                    for t in &q.terms {
                        writeln!(out, "term {}  # {t:?}", hex(t)).unwrap();
                    }
                    if let Some(p) = q.prefix {
                        let hi = p.hi.as_deref().map_or("-".into(), hex);
                        writeln!(
                            out,
                            "prefix {} {} {hi}  # {:?} {:?}",
                            hex(&p.exact),
                            hex(&p.lo),
                            p.exact,
                            p.lo
                        )
                        .unwrap();
                    }
                }
            }
        }
    }
    out
}

fn check_fixture(name: &str, rendered: String) {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../SPEC/fixtures/ix/text")
        .join(name);
    if std::env::var_os("JAZZ_UPDATE_FIXTURES").is_some() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, &rendered).unwrap();
        return;
    }
    let stored = std::fs::read_to_string(&path).unwrap_or_else(|_| {
        panic!(
            "{} is missing; run with JAZZ_UPDATE_FIXTURES=1",
            path.display()
        )
    });
    assert!(
        stored == rendered,
        "{} differs from the analyzer's output: a change to it is a new analyzer version",
        path.display()
    );
}

/// The terms of every case match the pinned bytes.
#[test]
fn terms_match_fixture() {
    check_fixture("terms.txt", render_terms());
}

/// The postings of every case, and the impact grid, match the pinned bytes.
#[test]
fn impacts_match_fixture() {
    check_fixture("impacts.txt", render_impacts());
}

/// The query terms of every case match the pinned bytes.
#[test]
fn queries_match_fixture() {
    check_fixture("queries.txt", render_queries());
}
