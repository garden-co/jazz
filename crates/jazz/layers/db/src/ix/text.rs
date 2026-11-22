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

use crate::storage::Storage;
use jazz_index::text::{Analyzer, PrefixTerm, QueryError, QueryTerms};

use super::{Candidates, Cell, Kind, shadow};
use crate::options::{Error, ErrorCode};
use crate::query::IndexOp;
use crate::values::{Sql, quote, storage};

pub(super) struct Text {
    pub analyzer: Analyzer,
    /// Each field's weight, in column order.
    /// Each field's `(weight, reference length)`, in column order.
    pub fields: Vec<(u8, u16)>,
    pub prefix: bool,
}

impl Text {
    pub(super) fn new(analyzer: &str, fields: Vec<(u8, u16)>, prefix: bool) -> Result<Self, Error> {
        let analyzer = Analyzer::parse(analyzer)
            .map_err(|error| Error::new(ErrorCode::Schema, error.to_string()))?;
        Ok(Self {
            analyzer,
            fields,
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
        conn: &Storage,
        prefix: &str,
        rid: i64,
        cells: Option<&[Cell]>,
    ) -> Result<(), Error> {
        let terms = quote(&shadow(prefix, "terms"));
        let post = quote(&shadow(prefix, "post"));
        let new: BTreeMap<String, u8> = match cells {
            None => BTreeMap::new(),
            Some(cells) => {
                let fields: Vec<(u8, u16, &str)> = cells
                    .iter()
                    .zip(&self.fields)
                    .filter_map(|(cell, (weight, length))| match cell {
                        Cell::Text(text) => Some((*weight, *length, text.as_str())),
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
                    .and_then(|mut s| s.execute(crate::storage::params![tid, rid, new_impact]))
                    .map_err(storage)?;
                }
                None => {
                    conn.prepare_cached(&format!("DELETE FROM {post} WHERE tid = ?1 AND rid = ?2"))
                        .and_then(|mut s| s.execute(crate::storage::params![tid, rid]))
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
            .and_then(|mut s| s.execute(crate::storage::params![tid, rid, impact]))
            .map_err(storage)?;
        }
        Ok(())
    }

    fn capped(&self) -> bool {
        true
    }

    fn admit(
        &self,
        conn: &Storage,
        prefix: &str,
        op: &IndexOp,
        readable: Option<&super::Readable>,
    ) -> Result<(), Error> {
        let query = self.query(op)?;
        let terms = quote(&shadow(prefix, "terms"));
        let post = quote(&shadow(prefix, "post"));
        let of_reader: Vec<Sql> = readable.map(|r| r.params.clone()).unwrap_or_default();
        // Each cap stops one past it. Under a policy that is one past it in
        // readable rows: what the reader may not read is passed over, at
        // the cost of a policy check each, and never counted (SPEC 20.7,
        // 20.12).
        if let Some(last) = &query.prefix {
            let mut params = of_reader.clone();
            let range = prefix_range("ix_t.term", last, &mut params);
            let held = readable
                .map(|r| {
                    format!(
                        " AND EXISTS (SELECT 1 FROM {post} AS ix_p WHERE ix_p.tid = ix_t.tid AND {})",
                        r.exists("ix_p.rid")
                    )
                })
                .unwrap_or_default();
            let sql = format!(
                "SELECT count(*) FROM (SELECT 1 FROM {terms} AS ix_t WHERE {range}{held} LIMIT {})",
                PREFIX_TERMS_MAX + 1
            );
            if counted(conn, &sql, &params)? > PREFIX_TERMS_MAX {
                return Err(super::too_broad(format!(
                    "the prefix `{}` matches more than {PREFIX_TERMS_MAX} indexed terms; type more of it",
                    last.lo
                )));
            }
        }
        let mut params = of_reader;
        let sources = sources(&terms, &query, &mut |value| {
            params.push(value);
            format!("?{}", params.len())
        });
        let held = readable
            .map(|r| format!(" WHERE {}", r.exists("ix_p.rid")))
            .unwrap_or_default();
        let sql = format!(
            "SELECT count(*) FROM (SELECT 1 FROM (SELECT DISTINCT tid FROM ({sources})) AS ix_q \
             JOIN {post} AS ix_p ON ix_p.tid = ix_q.tid{held} LIMIT {})",
            postings_cap() + 1
        );
        if counted(conn, &sql, &params)? > postings_cap() {
            return Err(super::too_broad(format!(
                "the search would read more than {} postings; use rarer words",
                postings_cap()
            )));
        }
        Ok(())
    }

    fn plan(
        &self,
        prefix: &str,
        op: &IndexOp,
        bind: &mut dyn FnMut(Sql) -> String,
    ) -> Result<Candidates, Error> {
        let IndexOp::Match { mode, .. } = op else {
            return Err(Error::new(
                ErrorCode::Query,
                "a text index answers `match` only",
            ));
        };
        let query = self.query(op)?;
        let terms = quote(&shadow(prefix, "terms"));
        let post = quote(&shadow(prefix, "post"));
        let slots = query.terms.len() + usize::from(query.prefix.is_some());
        // A row's score sums the impacts of the distinct terms it holds, each
        // once, whichever slots it serves (a word that is both a whole term
        // and the prefix counts once, SPEC 20.10.1). Larger impacts are
        // better: the score is their negated sum, so every kind's score is
        // smaller-better.
        let scored = sources(&terms, &query, bind);
        let score = format!(
            "SELECT m.rid AS rid, -sum(m.impact) AS score FROM (SELECT DISTINCT p.rid AS rid, \
             p.tid AS tid, p.impact AS impact FROM ({scored}) AS q JOIN {post} AS p ON p.tid = q.tid) \
             AS m GROUP BY m.rid"
        );
        // `Any` needs one slot, which every scored row has; `All` every slot.
        Ok(Candidates::Sql(match mode {
            crate::query::MatchMode::Any => score,
            crate::query::MatchMode::All => {
                let matched = sources(&terms, &query, bind);
                let n = bind(Sql::Integer(slots as i64));
                format!(
                    "SELECT s.rid AS rid, s.score AS score FROM ({score}) AS s WHERE s.rid IN \
                     (SELECT p.rid FROM ({matched}) AS q JOIN {post} AS p ON p.tid = q.tid \
                     GROUP BY p.rid HAVING count(DISTINCT q.slot) >= {n})"
                )
            }
        }))
    }
}

/// Most indexed terms a prefix may stand for (SPEC 20.12).
pub(super) const PREFIX_TERMS_MAX: usize = 1_000;
/// Most postings a search may read (SPEC 20.12).
pub(super) const POSTINGS_MAX: usize = 200_000;

#[cfg(any(test, feature = "testing"))]
thread_local! {
    static POSTINGS_CAP: std::cell::Cell<usize> = const { std::cell::Cell::new(POSTINGS_MAX) };
}

/// Lower this thread's postings cap, for a test to reach it.
#[cfg(any(test, feature = "testing"))]
pub fn set_postings_cap_for_test(cap: Option<usize>) {
    POSTINGS_CAP.with(|c| c.set(cap.unwrap_or(POSTINGS_MAX)));
}

fn postings_cap() -> usize {
    #[cfg(any(test, feature = "testing"))]
    return POSTINGS_CAP.with(std::cell::Cell::get);
    #[cfg(not(any(test, feature = "testing")))]
    POSTINGS_MAX
}

impl Text {
    fn query(&self, op: &IndexOp) -> Result<QueryTerms, Error> {
        let IndexOp::Match { text, .. } = op else {
            return Err(Error::new(
                ErrorCode::Query,
                "a text index answers `match` only",
            ));
        };
        self.analyzer
            .query(text, self.prefix)
            .map_err(|error| match error {
                QueryError::Empty => Error::new(
                    ErrorCode::Query,
                    "the search text has no terms after analysis",
                ),
                QueryError::TooLong { .. } | QueryError::TooManyTerms => {
                    super::query_too_long(error.to_string())
                }
            })
    }
}

/// The prefix slot's condition on column `column`: the whole-word term or
/// any term in the folded word's range. Its parameters join `params`.
fn prefix_range(column: &str, last: &PrefixTerm, params: &mut Vec<Sql>) -> String {
    let mut bind = |text: &str| {
        params.push(Sql::Text(text.as_bytes().to_vec()));
        format!("?{}", params.len())
    };
    let (exact, lo) = (bind(&last.exact), bind(&last.lo));
    match &last.hi {
        Some(hi) => {
            let hi = bind(hi);
            format!("({column} = {exact} OR ({column} >= {lo} AND {column} < {hi}))")
        }
        None => format!("({column} = {exact} OR {column} >= {lo})"),
    }
}

/// A capped count: `sql`'s one integer.
fn counted(conn: &Storage, sql: &str, params: &[Sql]) -> Result<usize, Error> {
    conn.prepare_cached(sql)
        .and_then(|mut s| {
            s.query_row(crate::storage::params_from_iter(params.iter()), |r| {
                r.get::<_, i64>(0)
            })
        })
        .map(|n| n as usize)
        .map_err(storage)
}

/// `(tid, slot)` of every indexed term a query slot matches: its whole
/// terms by the JSON array of them, then the prefix slot.
fn sources(terms: &str, query: &QueryTerms, bind: &mut dyn FnMut(Sql) -> String) -> String {
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
    sources.join(" UNION ALL ")
}
