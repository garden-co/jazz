//! The text kind's shadow tables and SQL (SPEC 20.2, 20.5, 20.10.1); the
//! analysis and impacts are `jazz_index::text`.
//!
//! `_terms(tid, term, n)` interns each indexed term (`n` counts its
//! postings, to drop dead terms; no query reads it). `_post(tid, rid,
//! impact)` holds one posting per (term, row). A write diffs the row's old
//! postings against its new ones, so an edit touching one word writes one
//! posting. A search sums the impacts of the rows holding every query term
//! (one slot per term, the last one a prefix range with `prefix`): an
//! integer sum, exact in any order.

use std::collections::BTreeMap;

use jazz_index::text::{Analyzer, QueryError};
use rusqlite::Connection;

use super::{Candidates, Cell, Kind, shadow};
use crate::options::{Error, ErrorCode};
use crate::query::IndexOp;
use crate::values::{Sql, quote, storage};

pub(super) struct Text {
    pub analyzer: Analyzer,
    /// Each field's weight, in column order.
    pub weights: Vec<u8>,
    pub prefix: bool,
}

impl Text {
    pub(super) fn new(analyzer: &str, weights: Vec<u8>, prefix: bool) -> Result<Self, Error> {
        let analyzer = Analyzer::parse(analyzer)
            .map_err(|error| Error::new(ErrorCode::Schema, error.to_string()))?;
        Ok(Self {
            analyzer,
            weights,
            prefix,
        })
    }
}

impl Kind for Text {
    fn encoding(&self) -> String {
        self.analyzer.encoding()
    }

    fn ddl(&self, prefix: &str) -> String {
        let terms = quote(&shadow(prefix, "terms"));
        let post = quote(&shadow(prefix, "post"));
        let by_term = quote(&format!("{prefix}_post_tid"));
        let by_rid = quote(&format!("{prefix}_post_rid"));
        format!(
            "CREATE TABLE IF NOT EXISTS {terms} (tid INTEGER PRIMARY KEY, term TEXT NOT NULL UNIQUE, n INTEGER NOT NULL);\n\
             CREATE TABLE IF NOT EXISTS {post} (tid INTEGER NOT NULL, rid INTEGER NOT NULL, impact INTEGER NOT NULL);\n\
             CREATE UNIQUE INDEX IF NOT EXISTS {by_term} ON {post} (tid, rid);\n\
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
        let terms = quote(&shadow(prefix, "terms"));
        let post = quote(&shadow(prefix, "post"));
        let new: BTreeMap<String, u8> = match cells {
            None => BTreeMap::new(),
            Some(cells) => {
                let fields: Vec<(u8, &str)> = cells
                    .iter()
                    .zip(&self.weights)
                    .filter_map(|(cell, weight)| match cell {
                        Cell::Text(text) => Some((*weight, text.as_str())),
                        _ => None,
                    })
                    .collect();
                jazz_index::text::row_impacts(&self.analyzer, &fields)
            }
        };
        let old: BTreeMap<String, (i64, i64)> = conn
            .prepare_cached(&format!(
                "SELECT t.term, p.tid, p.impact FROM {post} AS p JOIN {terms} AS t ON t.tid = p.tid \
                 WHERE p.rid = ?1"
            ))
            .and_then(|mut s| {
                s.query_map([rid], |r| Ok((r.get(0)?, (r.get(1)?, r.get(2)?))))?
                    .collect()
            })
            .map_err(storage)?;
        for (term, (tid, impact)) in &old {
            match new.get(term) {
                Some(new_impact) if i64::from(*new_impact) == *impact => {}
                Some(new_impact) => {
                    conn.prepare_cached(&format!(
                        "UPDATE {post} SET impact = ?3 WHERE tid = ?1 AND rid = ?2"
                    ))
                    .and_then(|mut s| s.execute(rusqlite::params![tid, rid, new_impact]))
                    .map_err(storage)?;
                }
                None => {
                    conn.prepare_cached(&format!("DELETE FROM {post} WHERE tid = ?1 AND rid = ?2"))
                        .and_then(|mut s| s.execute(rusqlite::params![tid, rid]))
                        .map_err(storage)?;
                    conn.prepare_cached(&format!("UPDATE {terms} SET n = n - 1 WHERE tid = ?1"))
                        .and_then(|mut s| s.execute([tid]))
                        .map_err(storage)?;
                    conn.prepare_cached(&format!("DELETE FROM {terms} WHERE tid = ?1 AND n <= 0"))
                        .and_then(|mut s| s.execute([tid]))
                        .map_err(storage)?;
                }
            }
        }
        for (term, impact) in &new {
            if old.contains_key(term) {
                continue;
            }
            conn.prepare_cached(&format!(
                "INSERT INTO {terms} (term, n) VALUES (?1, 1) ON CONFLICT (term) DO UPDATE SET n = n + 1"
            ))
            .and_then(|mut s| s.execute([term]))
            .map_err(storage)?;
            let tid: i64 = conn
                .prepare_cached(&format!("SELECT tid FROM {terms} WHERE term = ?1"))
                .and_then(|mut s| s.query_row([term], |r| r.get(0)))
                .map_err(storage)?;
            conn.prepare_cached(&format!(
                "INSERT INTO {post} (tid, rid, impact) VALUES (?1, ?2, ?3)"
            ))
            .and_then(|mut s| s.execute(rusqlite::params![tid, rid, impact]))
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
            return Err(Error::new(
                ErrorCode::Query,
                "a text index answers `match` only",
            ));
        };
        let query = self
            .analyzer
            .query(text, self.prefix)
            .map_err(|QueryError::Empty| {
                Error::new(
                    ErrorCode::Query,
                    "the search text has no terms after analysis",
                )
            })?;
        let terms = quote(&shadow(prefix, "terms"));
        let post = quote(&shadow(prefix, "post"));
        let slots = query.terms.len() + usize::from(query.prefix.is_some());
        let mut sources = Vec::new();
        if !query.terms.is_empty() {
            let json = bind(Sql::Text(
                serde_json::to_string(&query.terms)
                    .expect("strings serialize")
                    .into_bytes(),
            ));
            sources.push(format!(
                "SELECT t.tid AS tid, j.key AS slot FROM json_each({json}) AS j \
                 JOIN {terms} AS t ON t.term = j.value"
            ));
        }
        if let Some(last) = &query.prefix {
            let slot = bind(Sql::Integer(query.terms.len() as i64));
            let exact = bind(Sql::Text(last.exact.clone().into_bytes()));
            let lo = bind(Sql::Text(last.lo.clone().into_bytes()));
            let range = match &last.hi {
                Some(hi) => {
                    let hi = bind(Sql::Text(hi.clone().into_bytes()));
                    format!("(t.term >= {lo} AND t.term < {hi})")
                }
                None => format!("t.term >= {lo}"),
            };
            sources.push(format!(
                "SELECT t.tid AS tid, {slot} AS slot FROM {terms} AS t \
                 WHERE t.term = {exact} OR {range}"
            ));
        }
        // `Any` needs one slot, `All` every slot (SPEC 20.5).
        let needed = match mode {
            crate::query::MatchMode::Any => 1,
            crate::query::MatchMode::All => slots,
        };
        let n = bind(Sql::Integer(needed as i64));
        // Larger impacts are better: the score is their negated sum, so
        // every kind's score is smaller-better.
        Ok(Candidates::Sql(format!(
            "SELECT p.rid AS rid, -sum(p.impact) AS score FROM ({}) AS q \
             JOIN {post} AS p ON p.tid = q.tid GROUP BY p.rid HAVING count(DISTINCT q.slot) >= {n}",
            sources.join(" UNION ALL ")
        )))
    }
}
