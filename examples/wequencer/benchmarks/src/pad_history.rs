//! One pad toggled over and over while offline: reading its current value over
//! a long chain of locally settled edits, each built on the last.
//!
//! Moved from W1 (`w1_local_ahead_current_history`, formerly
//! `examples/benchmarks/w1`), reframed in Wequencer terms: the edited status row
//! is a pad, each candidate an edit. Storage, history shape, timed read and
//! receipt are unchanged; the store is a file-backed SQLite database.

use std::collections::BTreeMap;

use jazz::block_on;
use jazz::db::{CurrentRow, Db, DbConfig, DbIdentity, InsertOptions, PreparedQuery, UpdateOptions};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

const TABLE: &str = "pads";

/// Pre-seeded edit history for a single logical current row, in a file-backed
/// database.
pub struct PadHistoryFixture {
    db: Db,
    _directory: tempfile::TempDir,
    depth: usize,
    pads: PreparedQuery,
}

impl PadHistoryFixture {
    pub fn new(depth: usize) -> Self {
        assert!(depth > 0, "a pad needs at least one retained edit");
        let directory = tempfile::tempdir().expect("create pad-history fixture directory");
        let db = block_on(Db::open_at(
            DbConfig::new(
                schema(),
                DbIdentity {
                    node: node(),
                    author: AuthorSubject::SYSTEM,
                },
            ),
            &directory.path().join("pads.sqlite"),
            Default::default(),
        ))
        .expect("open pad-history database");

        for index in 0..depth {
            let write = if index == 0 {
                block_on(db.insert(
                    TABLE,
                    cells(index),
                    InsertOptions {
                        row_id: Some(row()),
                        ..Default::default()
                    },
                ))
            } else {
                block_on(db.update(TABLE, row(), cells(index), UpdateOptions::default()))
            }
            .expect("commit pad edit");
            block_on(write.wait(DurabilityTier::Local)).expect("persist pad edit");
        }
        let pads = db
            .prepare_query(&Query::from(TABLE))
            .expect("prepare pad read");

        Self {
            db,
            _directory: directory,
            depth,
            pads,
        }
    }

    /// Untimed correctness receipt for depth and winner identity. Superseded
    /// edits live in the history table, so the current read sees one row
    /// whatever the retained edit depth.
    pub fn assert_receipt(&mut self) {
        let rows = self.current_rows();
        assert_eq!(rows.len(), 1, "pad winner count");
        assert_eq!(rows[0].row_uuid(), row(), "pad winner row");
        assert_eq!(
            rows[0].cell_at(0),
            Some(Value::String(state(self.depth - 1))),
            "the pad must expose its newest edit",
        );
    }

    /// The timed operation: one current-row read over the prepared fixture.
    pub fn current_rows(&mut self) -> Vec<CurrentRow> {
        self.db.read(&self.pads).expect("read current pad")
    }
}

fn schema() -> JazzSchema {
    let source = SchemaBuilder::new()
        .table(TableSchemaBuilder::new(TABLE).column("state", ColumnType::Text))
        .build();
    JazzSchema::new(&source).expect("compile pad-history schema")
}

fn cells(index: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([("state".to_owned(), Value::String(state(index)))])
}

fn state(index: usize) -> String {
    format!("pad-{index:011}")
}

fn node() -> NodeUuid {
    NodeUuid::from_bytes([0x71; 16])
}

fn row() -> RowUuid {
    RowUuid::from_bytes([0x17; 16])
}
