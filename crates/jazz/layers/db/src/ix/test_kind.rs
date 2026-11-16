//! Tiny kinds that exercise the framework (SPEC 20) without the real
//! kinds' analysis: `words` stands in for text on the SQL path and
//! `near` for vector on the Rust-scored path. Enabled per thread with
//! [`use_test_kinds`]; gated `cfg(any(test, feature = "testing"))` so the
//! hook also fires when a crate above runs its tests.

use std::cell::Cell as StdCell;
use std::collections::BTreeMap;

use rusqlite::Connection;
use rusqlite::types::ValueRef;

use super::{Candidates, Cell, Kind, Scorer};
use crate::options::{Error, ErrorCode};
use crate::query::IndexOp;
use crate::values::{Column, Sql, quote, storage};
use jazz_model::model::public_schema::{SpecialisedIndex, SpecialisedIndexKind};

thread_local! {
    static ENABLED: StdCell<bool> = const { StdCell::new(false) };
}

/// Make this thread's stores implement text indexes with `words` and
/// vector indexes with `near` (geo keeps its real kind).
pub fn use_test_kinds(enabled: bool) {
    ENABLED.with(|e| e.set(enabled));
}

pub(super) fn override_for(
    index: &SpecialisedIndex,
    _columns: &[&Column],
) -> Option<Box<dyn Kind>> {
    if !ENABLED.with(StdCell::get) {
        return None;
    }
    match &index.kind {
        SpecialisedIndexKind::Text { fields, .. } => Some(Box::new(Words {
            weights: fields.values().copied().collect(),
        })),
        SpecialisedIndexKind::Vector { .. } => Some(Box::new(Near)),
        SpecialisedIndexKind::Geo { .. } => None,
    }
}

/// Whitespace-separated, lowercased words; a row's score is minus the
/// weighted count of its matched words' occurrences.
struct Words {
    weights: Vec<u8>,
}

fn words(text: &str) -> impl Iterator<Item = String> + '_ {
    text.split_whitespace().map(str::to_lowercase)
}

impl Kind for Words {
    fn encoding(&self) -> String {
        "jazz-ix-test-words/1".into()
    }

    fn ddl(&self, prefix: &str) -> String {
        let terms = quote(&super::shadow(prefix, "terms"));
        let post = quote(&super::shadow(prefix, "post"));
        let by_term = quote(&format!("{prefix}_post_by_term"));
        let by_rid = quote(&format!("{prefix}_post_by_rid"));
        format!(
            "CREATE TABLE IF NOT EXISTS {terms} (tid INTEGER PRIMARY KEY, term TEXT UNIQUE, n INTEGER);\n\
             CREATE TABLE IF NOT EXISTS {post} (rid INTEGER NOT NULL, term TEXT NOT NULL, impact INTEGER NOT NULL);\n\
             CREATE INDEX IF NOT EXISTS {by_term} ON {post} (term, rid);\n\
             CREATE INDEX IF NOT EXISTS {by_rid} ON {post} (rid);\n"
        )
    }

    fn write(
        &self,
        conn: &Connection,
        prefix: &str,
        rid: i64,
        cells: Option<&[Cell]>,
    ) -> Result<(), Error> {
        let post = quote(&super::shadow(prefix, "post"));
        conn.prepare_cached(&format!("DELETE FROM {post} WHERE rid = ?1"))
            .and_then(|mut s| s.execute([rid]))
            .map_err(storage)?;
        let Some(cells) = cells else {
            return Ok(());
        };
        let mut impacts: BTreeMap<String, i64> = BTreeMap::new();
        for (cell, weight) in cells.iter().zip(&self.weights) {
            if let Cell::Text(text) = cell {
                for word in words(text) {
                    *impacts.entry(word).or_default() += i64::from(*weight);
                }
            }
        }
        let mut insert = conn
            .prepare_cached(&format!(
                "INSERT INTO {post} (rid, term, impact) VALUES (?1, ?2, ?3)"
            ))
            .map_err(storage)?;
        for (word, impact) in impacts {
            insert
                .execute(rusqlite::params![rid, word, impact])
                .map_err(storage)?;
        }
        Ok(())
    }

    fn plan(
        &self,
        prefix: &str,
        op: &IndexOp,
        bind: &mut dyn FnMut(Sql) -> String,
    ) -> Result<Candidates, Error> {
        let IndexOp::Match { text, mode } = op else {
            return Err(Error::new(ErrorCode::Query, "words: not a match"));
        };
        let mut terms: Vec<String> = words(text).collect();
        terms.sort();
        terms.dedup();
        if terms.is_empty() {
            return Err(Error::new(
                ErrorCode::Query,
                "the search text has no words after analysis",
            ));
        }
        let post = quote(&super::shadow(prefix, "post"));
        let json = bind(Sql::Text(
            serde_json::to_string(&terms).unwrap().into_bytes(),
        ));
        let n = bind(Sql::Integer(match mode {
            crate::query::MatchMode::Any => 1,
            crate::query::MatchMode::All => terms.len() as i64,
        }));
        Ok(Candidates::Sql(format!(
            "SELECT p.rid AS rid, -sum(p.impact) AS score FROM json_each({json}) AS q \
             JOIN {post} AS p ON p.term = q.value GROUP BY p.rid HAVING count(DISTINCT p.term) >= {n}"
        )))
    }
}

/// The vector's `f32le` bytes as stored; a row's score is its squared L2
/// distance to the query, computed in Rust.
struct Near;

impl Kind for Near {
    fn encoding(&self) -> String {
        "jazz-ix-test-near/1".into()
    }

    fn ddl(&self, prefix: &str) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {} (rid INTEGER PRIMARY KEY, v BLOB NOT NULL);\n",
            quote(&super::shadow(prefix, "vec"))
        )
    }

    fn write(
        &self,
        conn: &Connection,
        prefix: &str,
        rid: i64,
        cells: Option<&[Cell]>,
    ) -> Result<(), Error> {
        let vec = quote(&super::shadow(prefix, "vec"));
        conn.prepare_cached(&format!("DELETE FROM {vec} WHERE rid = ?1"))
            .and_then(|mut s| s.execute([rid]))
            .map_err(storage)?;
        if let Some([Cell::Bytes(bytes)]) = cells {
            conn.prepare_cached(&format!("INSERT INTO {vec} (rid, v) VALUES (?1, ?2)"))
                .and_then(|mut s| s.execute(rusqlite::params![rid, bytes]))
                .map_err(storage)?;
        }
        Ok(())
    }

    fn plan(
        &self,
        _prefix: &str,
        op: &IndexOp,
        _bind: &mut dyn FnMut(Sql) -> String,
    ) -> Result<Candidates, Error> {
        let IndexOp::Nearest { vector, k } = op else {
            return Err(Error::new(ErrorCode::Query, "near: not a nearest"));
        };
        Ok(Candidates::Scored(Box::new(NearScorer {
            query: vector.clone(),
            k: *k as usize,
        })))
    }
}

struct NearScorer {
    query: Vec<f32>,
    k: usize,
}

impl Scorer for NearScorer {
    fn payload(&self) -> (&'static str, &'static str) {
        ("vec", "v")
    }

    fn k(&self) -> usize {
        self.k
    }

    fn score(&self, payload: ValueRef<'_>) -> Result<f64, Error> {
        let ValueRef::Blob(bytes) = payload else {
            return Err(Error::new(
                ErrorCode::Storage,
                "near: payload is not a blob",
            ));
        };
        let mut sum = 0f64;
        for (chunk, q) in bytes.chunks_exact(4).zip(&self.query) {
            let x = f32::from_le_bytes(chunk.try_into().unwrap());
            let d = f64::from(x) - f64::from(*q);
            sum += d * d;
        }
        Ok(sum)
    }
}
