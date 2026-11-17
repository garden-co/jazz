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
    MAX_FIELD_LENGTH, MAX_FIELD_TERMS, MAX_QUERY_BYTES, MAX_QUERY_TERMS, MAX_TERM_BYTES,
    MIN_PREFIX_CHARS, PrefixTerm, QueryError, QueryTerms, impact, row_impacts,
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

/// Words are cut at separators: punctuation, symbols, emoji
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

/// Punctuation and spaces of every script separate words, under `simple`
/// as under `en`: the danda, the Arabic comma, the Hebrew sof pasuq, the
/// Armenian full stop. A byte order mark is not a letter.
#[test]
fn punctuation_of_every_script_separates_words() {
    assert_eq!(
        terms(simple(), "नमस्ते।दुनिया مرحبا،عالم שלום׃עולם x։y"),
        ["नमस्ते", "दुनिया", "مرحبا", "عالم", "שלום", "עולם", "x", "y"]
    );
    assert_eq!(terms(simple(), "\u{feff}jazz"), ["jazz"]);
    assert_eq!(
        terms(simple(), "a\u{3000}b\u{2028}c\u{85}d"),
        ["a", "b", "c", "d"]
    );
}

/// Soft hyphens, byte order marks and zero-width joiners inside a word are
/// ignored: the word stays whole and its term has none of them.
#[test]
fn invisible_characters_inside_words_are_ignored() {
    assert_eq!(terms(simple(), "co\u{ad}operate"), ["cooperate"]);
    assert_eq!(terms(simple(), "fi\u{feff}ne"), ["fine"]);
    assert_eq!(terms(simple(), "می\u{200c}خواهم"), ["میخواهم"]);
    assert_eq!(
        terms(en(), "hy\u{ad}phen\u{ad}ation"),
        terms(en(), "hyphenation")
    );
}

/// Combining marks of every combining-diacritics block fold away, so the
/// marked and unmarked spellings meet.
#[test]
fn every_combining_diacritics_block_folds_away() {
    for mark in [
        '\u{0301}', '\u{1AB0}', '\u{1DC0}', '\u{20D0}', '\u{FE20}', '\u{FE0F}',
    ] {
        assert_eq!(
            terms(simple(), &format!("e{mark}x")),
            ["ex"],
            "U+{:04X}",
            mark as u32
        );
    }
}

/// Search text over 1 KiB is refused before it is analyzed, and so is text
/// with more than 32 distinct terms (repeats count once); the dedupe is not
/// quadratic.
#[test]
fn queries_are_capped_at_1_kib_and_32_terms() {
    let long = "jazz ".repeat(205);
    assert_eq!(long.len(), 1025);
    assert_eq!(
        en().query(&long, false),
        Err(QueryError::TooLong { bytes: 1025 })
    );
    assert_eq!(
        en().query(&long[..MAX_QUERY_BYTES], false).unwrap().terms,
        ["jazz"]
    );
    let words: Vec<String> = (0..MAX_QUERY_TERMS + 1).map(|i| format!("w{i}")).collect();
    assert_eq!(
        simple()
            .query(&words[..MAX_QUERY_TERMS].join(" "), false)
            .unwrap()
            .terms
            .len(),
        32
    );
    assert_eq!(
        simple().query(&words.join(" "), false),
        Err(QueryError::TooManyTerms)
    );
    // 32 terms and a prefix are 33 slots.
    assert_eq!(
        simple().query(&words.join(" "), true),
        Err(QueryError::TooManyTerms)
    );
    assert_eq!(
        simple()
            .query(&words[..32].join(" "), true)
            .unwrap()
            .terms
            .len(),
        31
    );
    assert_eq!((MAX_QUERY_BYTES, MAX_QUERY_TERMS), (1024, 32));
}

/// A prefix needs two characters: a one-character last word is an exact
/// term, so typing `jazz b` does not match every term starting with `b`.
#[test]
fn a_one_character_prefix_is_an_exact_term() {
    assert_eq!(MIN_PREFIX_CHARS, 2);
    let q = simple().query("jazz b", true).unwrap();
    assert_eq!(
        (q.terms, q.prefix),
        (vec!["jazz".to_owned(), "b".to_owned()], None)
    );
    let q = simple().query("東京", true).unwrap();
    assert_eq!(
        (q.terms, q.prefix),
        (vec!["東".to_owned(), "京".to_owned()], None)
    );
    let p = simple().query("ab", true).unwrap().prefix.unwrap();
    assert_eq!((p.lo.as_str(), p.hi.as_deref()), ("ab", Some("ac")));
}

/// A field is indexed by its first 65 536 terms: later words get no
/// postings and do not count toward its length, and the value is still
/// indexed, never refused.
#[test]
fn fields_are_indexed_by_their_first_65536_terms() {
    assert_eq!(MAX_FIELD_TERMS, 65_536);
    let field = format!("{}tail", "jazz ".repeat(MAX_FIELD_TERMS));
    let row = row_impacts(&en(), &[(1, 32, &field)]);
    assert_eq!(row.len(), 1);
    assert_eq!(row["jazz"], impact(1, 32, MAX_FIELD_TERMS, MAX_FIELD_TERMS));
    // `terms` itself is not capped.
    assert_eq!(en().terms(&field).len(), MAX_FIELD_TERMS + 1);
}

/// The formula's operations happen in SPEC order: `0.75 * L / R` is
/// `(0.75 * L) / R`, which differs in the last bit from `0.75 * (L / R)` at
/// these points, enough to move the floor.
#[test]
fn impact_division_order_is_the_specs() {
    assert_eq!(impact(1, 7, 1, 29), 7);
    assert_eq!(impact(8, 7, 1, 29), 56);
}

/// Word-internal punctuation follows UAX #29's MidLetter rules: a middle
/// dot between letters (Catalan) and a geresh or gershayim between Hebrew
/// letters, in their Hebrew or ASCII forms, stay inside the word and fold
/// away, so the spellings with and without them meet.
#[test]
fn mid_letter_punctuation_stays_inside_words() {
    assert_eq!(terms(simple(), "col·lecció"), ["colleccio"]);
    assert_eq!(terms(simple(), "col·lecció"), terms(simple(), "colleccio"));
    assert_eq!(terms(simple(), "צה״ל צה\"ל צהל"), ["צהל", "צהל", "צהל"]);
    assert_eq!(terms(simple(), "ג׳ירפה ג'ירפה"), ["ג'ירפה", "ג'ירפה"]);
    // Only between letters: elsewhere they still separate.
    assert_eq!(terms(simple(), "a·"), ["a"]);
    // The middle dot joins only the Catalan `l·l`.
    assert_eq!(terms(simple(), "COL·LECCIÓ"), ["colleccio"]);
    assert_eq!(
        terms(simple(), "3·4 Home·About a·b"),
        ["3", "4", "home", "about", "a", "b"]
    );
    // WB4: niqqud before a gershayim belongs to the letter before it.
    assert_eq!(terms(simple(), "צהָ״ל"), ["צהל"]);
    assert_eq!(terms(simple(), "say \"hi\""), ["say", "hi"]);
    assert_eq!(terms(simple(), "a\"b"), ["a", "b"]);
    assert_eq!(terms(simple(), "שלום״"), ["שלום"]);
}

/// A word repeated as the prefix keeps its exact slot: the prefix slot
/// also matches longer terms, so dropping the exact slot would let
/// `running run` under `All` match a row holding only `runway`. SQL sums
/// each posting once, so the row's score does not double.
#[test]
fn the_prefix_word_keeps_its_exact_term_slot() {
    let q = en().query("run run", true).unwrap();
    assert_eq!(q.terms, ["run"]);
    assert_eq!(q.prefix.unwrap().exact, "run");
    let q = en().query("jazz running run", true).unwrap();
    assert_eq!(q.terms, ["jazz", "run"]);
    assert_eq!(q.prefix.unwrap().lo, "run");
}

/// Hebrew points and cantillation and Arabic harakat fold away, so pointed
/// and unpointed spellings meet; the letters stay.
#[test]
fn hebrew_points_and_arabic_harakat_fold_away() {
    assert_eq!(terms(simple(), "שָׁלוֹם"), terms(simple(), "שלום"));
    assert_eq!(terms(simple(), "בְּרֵאשִׁ֖ית"), ["בראשית"]);
    assert_eq!(terms(simple(), "مَرْحَبًا"), terms(simple(), "مرحبا"));
    assert_eq!(terms(simple(), "رَحْمٰن"), ["رحمن"]);
    // Hebrew punctuation is not a point: maqaf (U+05BE) still separates.
    assert_eq!(terms(simple(), "בית־ספר"), ["בית", "ספר"]);
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
    "नमस्ते।दुनिया مرحبا،عالم שלום׃עולם x։y \u{feff}bom",
    "co\u{ad}operate fi\u{feff}ne می\u{200c}خواهم",
    "e\u{1ab0}x e\u{1dc0}x e\u{20d0}x e\u{fe20}x 葛\u{e0100}",
    "col·lecció צה״ל צה\"ל ג׳ירפה a· say \"hi\"",
    "3·4 Home·About COL·LECCIÓ צהָ״ל",
    "שָׁלוֹם בְּרֵאשִׁ֖ית مَرْحَبًا رَحْمٰن בית־ספר",
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
    ("jazz b", true),
    ("ab", true),
    ("run run", true),
    ("jazz running run", true),
    (
        "a b c d e f g h i j k l m n o p q r s t u v w x y z A B C 1 2 3 4 5",
        false,
    ),
    (
        "a b c d e f g h i j k l m n o p q r s t u v w x y z 1 2 3 4 5 6 7",
        false,
    ),
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
         # `prefix <exact hex> <lo hex> <hi hex or ->`, or `error <empty|too-long|too-many-terms>`.\n",
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
                Err(QueryError::TooLong { .. }) => out.push_str("error too-long\n"),
                Err(QueryError::TooManyTerms) => out.push_str("error too-many-terms\n"),
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
