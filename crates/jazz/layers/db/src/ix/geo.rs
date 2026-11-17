//! The geo kind's shadow table and SQL (SPEC 20.2, 20.5, 20.10.3); cells,
//! unit vectors and covers are `jazz_index::geo`.
//!
//! `_geo(rid, cell, lat, lng, x, y, z)` holds one row per indexed point
//! (a row whose `point` cell is null has none), indexed by `cell`. A
//! search reads the cells of its cover and decides exactly: a radius by
//! the squared chord distance (also its score, smaller-better), a box by
//! the stored degrees (score 0, so rows order by id).

use jazz_index::geo::{self, GeoError};
use rusqlite::Connection;

use super::{Candidates, Cell, Kind, shadow};
use crate::options::{Error, ErrorCode};
use crate::query::IndexOp;
use crate::values::{Sql, quote, storage};

pub(super) struct Geo;

fn query_error(error: GeoError) -> Error {
    Error::new(ErrorCode::Query, error.to_string())
}

impl Kind for Geo {
    fn encoding(&self) -> String {
        geo::ENCODING.to_owned()
    }

    fn ddl(&self, prefix: &str) -> String {
        let table = quote(&shadow(prefix, "geo"));
        let by_cell = quote(&format!("{prefix}_geo_cell"));
        format!(
            "CREATE TABLE IF NOT EXISTS {table} (rid INTEGER PRIMARY KEY, cell INTEGER NOT NULL, \
             lat REAL NOT NULL, lng REAL NOT NULL, x REAL NOT NULL, y REAL NOT NULL, z REAL NOT NULL);\n\
             CREATE INDEX IF NOT EXISTS {by_cell} ON {table} (cell);\n"
        )
    }

    fn write(
        &self,
        conn: &Connection,
        prefix: &str,
        rid: i64,
        cells: Option<&[Cell]>,
    ) -> Result<(), Error> {
        let table = quote(&shadow(prefix, "geo"));
        conn.prepare_cached(&format!("DELETE FROM {table} WHERE rid = ?1"))
            .and_then(|mut s| s.execute([rid]))
            .map_err(storage)?;
        let Some([Cell::Bytes(bytes)]) = cells else {
            return Ok(());
        };
        // A point that does not decode (wrong length, non-finite or out of
        // range) leaves the row unindexed (`INV-IX-7`): admission refuses
        // such values, so only a row written before could hold one.
        let Ok([lng, lat]) = crate::records::decode_point_f64le(bytes) else {
            return Ok(());
        };
        let Ok(point) = geo::point(lat, lng) else {
            return Ok(());
        };
        let [x, y, z] = point.unit;
        conn.prepare_cached(&format!(
            "INSERT INTO {table} (rid, cell, lat, lng, x, y, z) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)"
        ))
        .and_then(|mut s| s.execute(rusqlite::params![rid, point.cell, lat, lng, x, y, z]))
        .map_err(storage)?;
        Ok(())
    }

    fn plan(
        &self,
        prefix: &str,
        op: &IndexOp,
        bind: &mut dyn FnMut(Sql) -> String,
    ) -> Result<Candidates, Error> {
        let table = quote(&shadow(prefix, "geo"));
        let ranges = |bind: &mut dyn FnMut(Sql) -> String, ranges: &[(i64, i64)]| {
            if ranges.is_empty() {
                return "0".to_owned();
            }
            let parts: Vec<String> = ranges
                .iter()
                .map(|(lo, hi)| {
                    let lo = bind(Sql::Integer(*lo));
                    let hi = bind(Sql::Integer(*hi));
                    format!("g.cell BETWEEN {lo} AND {hi}")
                })
                .collect();
            format!("({})", parts.join(" OR "))
        };
        Ok(Candidates::Sql(match op {
            IndexOp::WithinRadius { lat, lng, meters } => {
                let query = geo::radius_query(*lat, *lng, *meters).map_err(query_error)?;
                let cover = ranges(bind, &query.ranges);
                let [qx, qy, qz] = query.center.map(|c| bind(Sql::Real(c)));
                let d2 = bind(Sql::Real(query.chord2));
                // The expression `jazz_index::geo::chord2` evaluates, in its
                // order (IEEE-754 binary64 on every target).
                let distance = format!(
                    "(g.x - {qx}) * (g.x - {qx}) + (g.y - {qy}) * (g.y - {qy}) + (g.z - {qz}) * (g.z - {qz})"
                );
                format!(
                    "SELECT g.rid AS rid, {distance} AS score FROM {table} AS g \
                     WHERE {cover} AND {distance} <= {d2}"
                )
            }
            IndexOp::WithinBox {
                south,
                west,
                north,
                east,
            } => {
                let cover = ranges(
                    bind,
                    &geo::cover_box(*south, *west, *north, *east).map_err(query_error)?,
                );
                let (s, n) = (bind(Sql::Real(*south)), bind(Sql::Real(*north)));
                let (w, e) = (bind(Sql::Real(*west)), bind(Sql::Real(*east)));
                let lng = if west > east {
                    format!("(g.lng >= {w} OR g.lng <= {e})")
                } else {
                    format!("g.lng BETWEEN {w} AND {e}")
                };
                format!(
                    "SELECT g.rid AS rid, 0 AS score FROM {table} AS g \
                     WHERE {cover} AND g.lat BETWEEN {s} AND {n} AND {lng}"
                )
            }
            _ => {
                return Err(Error::new(
                    ErrorCode::Query,
                    "a geo index answers `withinRadius` and `withinBox` only",
                ));
            }
        }))
    }
}
