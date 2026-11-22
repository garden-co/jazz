//! The vector kind's shadow table and Rust step (SPEC 20.2, 20.5,
//! 20.10.2); encoding, normalization and scores are `jazz_index::vector`.
//!
//! `_vec(rid, v)` holds each row's vector as `f32le/1` bytes, normalized
//! for `cosine`. A search is scored in Rust over the rows the query's
//! other clauses match (see [`super::Candidates::Scored`]): SQL cannot
//! compute the distance on every target.

use crate::storage::Storage;
use jazz_index::vector::{self, Metric};
use rusqlite::types::ValueRef;

use super::{Candidates, Cell, Kind, Scorer, shadow};
use crate::options::{Error, ErrorCode};
use crate::query::IndexOp;
use crate::values::{Sql, quote, storage};

pub(super) struct Vector {
    pub metric: Metric,
    pub dimensions: Option<usize>,
}

impl Vector {
    pub(super) fn new(
        metric: jazz_model::model::public_schema::VectorMetric,
        dimensions: Option<usize>,
    ) -> Self {
        Self {
            metric: match metric {
                jazz_model::model::public_schema::VectorMetric::Cosine => Metric::Cosine,
                jazz_model::model::public_schema::VectorMetric::L2 => Metric::L2,
                jazz_model::model::public_schema::VectorMetric::Dot => Metric::Dot,
            },
            dimensions,
        }
    }
}

impl Kind for Vector {
    fn encoding(&self) -> String {
        format!("jazz-ix-vector/1 {}", vector::ENCODING)
    }

    fn ddl(&self, prefix: &str) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {} (rid INTEGER PRIMARY KEY, v BLOB NOT NULL);\n",
            quote(&shadow(prefix, "vec"))
        )
    }

    fn write(
        &self,
        conn: &Storage,
        prefix: &str,
        rid: i64,
        cells: Option<&[Cell]>,
    ) -> Result<(), Error> {
        let table = quote(&shadow(prefix, "vec"));
        conn.prepare_cached(&format!("DELETE FROM {table} WHERE rid = ?1"))
            .and_then(|mut s| s.execute([rid]))
            .map_err(storage)?;
        let Some([Cell::Bytes(bytes)]) = cells else {
            return Ok(());
        };
        let dimensions = self.dimensions.unwrap_or(bytes.len() / 4);
        // Admission refuses malformed vectors and, for cosine, zero ones;
        // a row that holds one anyway (written before the index existed)
        // is not indexed.
        let Ok(values) = vector::decode_f32le_checked(bytes, dimensions) else {
            return Ok(());
        };
        let Ok(prepared) = vector::prepare(self.metric, &values) else {
            return Ok(());
        };
        conn.prepare_cached(&format!("INSERT INTO {table} (rid, v) VALUES (?1, ?2)"))
            .and_then(|mut s| {
                s.execute(crate::storage::params![
                    rid,
                    vector::encode_f32le(&prepared)
                ])
            })
            .map_err(storage)?;
        Ok(())
    }

    fn plan(
        &self,
        _prefix: &str,
        op: &IndexOp,
        _only: Option<&str>,
        _bind: &mut dyn FnMut(Sql) -> String,
    ) -> Result<Candidates, Error> {
        let IndexOp::Nearest { vector: query, k } = op else {
            return Err(Error::new(
                ErrorCode::Query,
                "a vector index answers `nearest` only",
            ));
        };
        if self.dimensions.is_some_and(|n| n != query.len()) {
            return Err(Error::new(
                ErrorCode::Query,
                format!(
                    "the query vector has {} dimensions; the index has {}",
                    query.len(),
                    self.dimensions.unwrap_or_default()
                ),
            ));
        }
        let query = vector::prepare(self.metric, query)
            .map_err(|error| Error::new(ErrorCode::Query, error.to_string()))?;
        Ok(Candidates::Scored(Box::new(Nearest {
            metric: self.metric,
            query,
            k: *k as usize,
        })))
    }
}

struct Nearest {
    metric: Metric,
    query: Vec<f32>,
    k: usize,
}

impl Scorer for Nearest {
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
                "a vector shadow row is not a blob",
            ));
        };
        let stored = vector::decode_f32le(bytes, self.query.len())
            .map_err(|error| Error::new(ErrorCode::Storage, error.to_string()))?;
        Ok(vector::score(self.metric, &self.query, &stored))
    }
}
