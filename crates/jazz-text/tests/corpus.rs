//! The text corpus: realistic collaboration scenarios with expected
//! outcomes, run against the authority's merge and against anchors.
//!
//! Cases live in `tests/corpus/*.json` so they can be written and reviewed
//! as data. A case that today's code gets wrong carries
//! `"baseline": "fails"`; the run checks that each case passes or fails as
//! its flag says, so an improvement shows up as a flag to flip.
//!
//! Run with `cargo test -p jazz-text --test corpus -- --nocapture` for the
//! per-case report.

use jazz_text::{Anchor, Side, TextFormat, merge3};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

/// Marks a position in a case's text.
const MARKER: char = '‸';
/// An anchor found where expected must be at least this sure, unless the
/// case says otherwise.
const FOUND_AT_LEAST: f64 = 0.7;
/// An anchor whose text is gone must be less sure than this.
const ORPHANED_BELOW: f64 = 0.6;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AnchorCase {
    name: String,
    side: CaseSide,
    old: String,
    /// The later value, with the marker where the anchor belongs; without
    /// one, the anchor's text is gone and it must come back orphaned.
    new: String,
    #[serde(default)]
    min_confidence: Option<f64>,
    #[serde(default)]
    baseline: Option<Baseline>,
}

#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum CaseSide {
    Before,
    After,
}

#[derive(Deserialize, Clone, Copy, PartialEq, Eq, Debug)]
#[serde(rename_all = "lowercase")]
enum Baseline {
    Fails,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MergeCase {
    name: String,
    format: CaseFormat,
    /// What the case checks, in a few words.
    intent: String,
    base: String,
    steps: Vec<Step>,
    /// The exact final value, when only one is right.
    #[serde(default)]
    expect: Option<String>,
    /// Text the final value must contain (no lost work).
    #[serde(default)]
    keeps: Vec<String>,
    /// Text the final value must not contain (deletes stay deleted).
    #[serde(default)]
    drops: Vec<String>,
    /// Text that must appear in this order.
    #[serde(default)]
    order: Vec<String>,
    #[serde(default)]
    baseline: Option<Baseline>,
}

#[derive(Deserialize, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum CaseFormat {
    Plain,
    Markdown,
    Html,
}

impl From<CaseFormat> for TextFormat {
    fn from(format: CaseFormat) -> Self {
        match format {
            CaseFormat::Plain => TextFormat::Plain,
            CaseFormat::Markdown => TextFormat::Markdown,
            CaseFormat::Html => TextFormat::Html,
        }
    }
}

/// One step of a scenario. Online users send each edit at once and see
/// others' edits only when they sync; offline users queue their edits.
#[derive(Deserialize)]
#[serde(untagged)]
enum Step {
    /// Replace the `nth` (default first) occurrence of `replace` in the
    /// user's text.
    Replace {
        user: String,
        replace: String,
        with: String,
        #[serde(default)]
        nth: usize,
    },
    /// Overwrite the user's whole text, as a client without an editor does.
    Set {
        user: String,
        set: String,
    },
    /// The user receives the authority's current value.
    Sync {
        sync: String,
    },
    Offline {
        offline: String,
    },
    /// The user reconnects and sends its queued edits in order.
    Online {
        online: String,
    },
}

#[derive(Default)]
struct User {
    /// What the user's editor shows.
    local: String,
    /// The value the user's next write is built on: the last value it
    /// received, or its own last write as sent.
    base: String,
    offline: bool,
    /// Writes made offline: (base, value).
    queued: Vec<(String, String)>,
}

struct Authority {
    format: TextFormat,
    current: String,
    spent: Duration,
}

impl Authority {
    fn accept(&mut self, base: &str, value: &str) {
        let started = Instant::now();
        self.current = merge3(self.format, base, &self.current, value);
        self.spent += started.elapsed();
    }
}

fn run_merge(case: &MergeCase) -> Result<(String, Duration), String> {
    let mut authority = Authority {
        format: case.format.into(),
        current: case.base.clone(),
        spent: Duration::ZERO,
    };
    let mut users: BTreeMap<String, User> = BTreeMap::new();
    let user = |name: &str, users: &mut BTreeMap<String, User>| {
        users.entry(name.to_owned()).or_insert_with(|| User {
            local: case.base.clone(),
            base: case.base.clone(),
            ..User::default()
        });
    };
    for step in &case.steps {
        match step {
            Step::Replace {
                user: name,
                replace,
                with,
                nth,
            } => {
                user(name, &mut users);
                let state = users.get_mut(name).unwrap();
                let Some((at, _)) = state.local.match_indices(replace.as_str()).nth(*nth) else {
                    return Err(format!(
                        "{name} has no {replace:?} (#{nth}) in {:?}",
                        state.local
                    ));
                };
                state.local.replace_range(at..at + replace.len(), with);
                send(state, &mut authority);
            }
            Step::Set { user: name, set } => {
                user(name, &mut users);
                let state = users.get_mut(name).unwrap();
                state.local = set.clone();
                send(state, &mut authority);
            }
            Step::Sync { sync } => {
                user(sync, &mut users);
                let state = users.get_mut(sync).unwrap();
                if !state.offline && state.queued.is_empty() {
                    state.local = authority.current.clone();
                    state.base = authority.current.clone();
                }
            }
            Step::Offline { offline } => {
                user(offline, &mut users);
                users.get_mut(offline).unwrap().offline = true;
            }
            Step::Online { online } => {
                user(online, &mut users);
                let state = users.get_mut(online).unwrap();
                state.offline = false;
                for (base, value) in state.queued.drain(..) {
                    authority.accept(&base, &value);
                }
            }
        }
    }
    Ok((authority.current, authority.spent))
}

fn send(user: &mut User, authority: &mut Authority) {
    let value = user.local.clone();
    let base = std::mem::replace(&mut user.base, value.clone());
    if user.offline {
        user.queued.push((base, value));
    } else {
        authority.accept(&base, &value);
    }
}

fn check_merge(case: &MergeCase, result: &str) -> Vec<String> {
    let mut problems = Vec::new();
    if let Some(expect) = &case.expect
        && result != expect
    {
        problems.push(format!("expected {expect:?}"));
    }
    for keep in &case.keeps {
        if !result.contains(keep.as_str()) {
            problems.push(format!("lost {keep:?}"));
        }
    }
    for drop in &case.drops {
        if result.contains(drop.as_str()) {
            problems.push(format!("kept deleted {drop:?}"));
        }
    }
    let mut from = 0;
    for piece in &case.order {
        match result[from..].find(piece.as_str()) {
            Some(at) => from += at + piece.len(),
            None => {
                problems.push(format!("order broken at {piece:?}"));
                break;
            }
        }
    }
    problems
}

fn marker_position(text: &str) -> Option<(String, usize)> {
    let at = text.find(MARKER)?;
    let clean = text.replacen(MARKER, "", 1);
    Some((clean, text[..at].encode_utf16().count()))
}

fn check_anchor(case: &AnchorCase) -> Result<Vec<String>, String> {
    let (old, position) =
        marker_position(&case.old).ok_or_else(|| format!("{}: no marker in old", case.name))?;
    let side = match case.side {
        CaseSide::Before => Side::Before,
        CaseSide::After => Side::After,
    };
    let anchor = Anchor::capture(&old, 1, position, side);
    let mut problems = Vec::new();
    match marker_position(&case.new) {
        Some((new, expected)) => {
            let found = anchor.resolve(&new, 2);
            if found.position != expected {
                problems.push(format!(
                    "landed at {} (confidence {:.2}), expected {expected}",
                    found.position, found.confidence
                ));
            }
            let least = case.min_confidence.unwrap_or(FOUND_AT_LEAST);
            if found.confidence < least {
                problems.push(format!("confidence {:.2} < {least}", found.confidence));
            }
        }
        None => {
            let found = anchor.resolve(&case.new, 2);
            if found.confidence >= ORPHANED_BELOW {
                problems.push(format!(
                    "not orphaned: confidence {:.2} at {}",
                    found.confidence, found.position
                ));
            }
        }
    }
    Ok(problems)
}

fn load<T: for<'de> Deserialize<'de>>(file: &str) -> Vec<T> {
    let path = format!("{}/tests/corpus/{file}", env!("CARGO_MANIFEST_DIR"));
    let json = std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("{path}: {error}"));
    serde_json::from_str(&json).unwrap_or_else(|error| panic!("{path}: {error}"))
}

/// Checks each case against its baseline flag and returns the mismatches.
fn verdict(name: &str, baseline: Option<Baseline>, problems: &[String]) -> Option<String> {
    let fails = !problems.is_empty();
    let flagged = baseline == Some(Baseline::Fails);
    let status = match (fails, flagged) {
        (false, false) => "ok",
        (true, true) => "known failure",
        (true, false) => "FAILS",
        (false, true) => "PASSES (flip its baseline flag)",
    };
    println!("  {status:<34} {name}");
    for problem in problems {
        println!("      {problem}");
    }
    (fails != flagged).then(|| format!("{name}: {status} {problems:?}"))
}

#[test]
fn merge_corpus() {
    let cases: Vec<MergeCase> = load("merges.json");
    let mut mismatches = Vec::new();
    let mut passed = 0;
    let mut spent = Duration::ZERO;
    println!("merge corpus ({} cases):", cases.len());
    for case in &cases {
        let problems = match run_merge(case) {
            Ok((result, time)) => {
                spent += time;
                let problems = check_merge(case, &result);
                if !problems.is_empty() {
                    let mut problems = problems;
                    problems.push(format!("got {result:?}"));
                    problems.push(format!("intent: {}", case.intent));
                    problems
                } else {
                    problems
                }
            }
            Err(error) => vec![format!("scenario error: {error}")],
        };
        passed += usize::from(problems.is_empty());
        mismatches.extend(verdict(&case.name, case.baseline, &problems));
    }
    println!(
        "merge corpus: {passed}/{} pass, merge time {spent:?}",
        cases.len()
    );
    assert!(mismatches.is_empty(), "{mismatches:#?}");
}

#[test]
fn anchor_corpus() {
    let cases: Vec<AnchorCase> = load("anchors.json");
    let mut mismatches = Vec::new();
    let mut passed = 0;
    println!("anchor corpus ({} cases):", cases.len());
    for case in &cases {
        let problems = check_anchor(case).unwrap_or_else(|error| vec![error]);
        passed += usize::from(problems.is_empty());
        mismatches.extend(verdict(&case.name, case.baseline, &problems));
    }
    println!("anchor corpus: {passed}/{} pass", cases.len());
    assert!(mismatches.is_empty(), "{mismatches:#?}");
}
