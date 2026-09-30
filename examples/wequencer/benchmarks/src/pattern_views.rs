//! Many bandmates open pattern views of the same shape at once: 100 distinct
//! parameter bindings of one prepared query, each hydrated and consumed.
//!
//! Moved from `crates/jazz/benches/route_subscription_curve.rs`
//! (`attach_route_bindings[100]`), reframed in Wequencer terms. The fixture,
//! query shape, timed work and assertions are unchanged: routes are
//! patterns, documents are pad edits, `updated_at` is `edited_at`. The crate
//! bench keeps the scale receipt (`JAZZ_ROUTE_CURVE_RECEIPT`) for diagnosis.
//!
//! 1,001 patterns. The busy pattern holds 1,000 pad edits; every other pattern
//! holds one. Each view binds pattern = param, newest edit first, limit 100.

use std::collections::{BTreeMap, BTreeSet};

use jazz::db::{
    Db, DbConfig, DbIdentity, LocalUpdates, Propagation, ReadOpts, SeededRowIdSource,
    SubscriptionEvent, SubscriptionStream, block_on,
};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, param};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

const EDITS: &str = "pattern_pad_edits";
const PATTERNS: usize = 1_001;
const BUSY_PATTERN_EDITS: usize = 1_000;
const PAGE_SIZE: usize = 100;
const MAX_OPEN_PATTERNS: usize = 1_000;
const WRITER: AuthorSubject = AuthorSubject::SYSTEM;

/// Seeded patterns; no views open yet.
pub struct PatternViewsFixture {
    db: Db,
    patterns_open: usize,
    query: Query,
}

/// Every view's stream plus the runtime and retained-state receipts the
/// original route curve collected inside the timed attachment.
pub struct OpenPatterns {
    pub streams: Vec<SubscriptionStream>,
    pub runtime: RuntimeReceipt,
    pub retained: RetainedReceipt,
}

#[derive(Debug)]
pub struct RuntimeReceipt {
    pub graph_nodes: usize,
    pub active_subscriptions: usize,
    pub active_prepared_shapes: usize,
    pub active_shape_params: usize,
    pub arrangement_count: usize,
    pub arrangement_rows: usize,
    pub arrangement_encoded_bytes: usize,
    pub logical_nodes_requested: u64,
    pub deduped_graph_nodes: usize,
}

#[derive(Debug)]
pub struct RetainedReceipt {
    pub subscriptions: usize,
    pub root_rows: usize,
    pub result_rows: usize,
    pub version_identities: usize,
    pub replacement_entries: usize,
    pub maintained_heap_bytes: usize,
    pub result_weights_bytes: usize,
    pub result_payloads_bytes: usize,
    pub versions_bytes: usize,
    pub supporting_frontier_bytes: usize,
    pub replacements_bytes: usize,
    pub terminal_schemas_bytes: usize,
    pub control_state_bytes: usize,
    pub maintained_and_control_heap_bytes: usize,
    pub snapshot_bytes: usize,
    pub reset_frame_bytes: usize,
}

impl PatternViewsFixture {
    pub fn seeded(patterns_open: usize) -> Self {
        assert!((1..=MAX_OPEN_PATTERNS).contains(&patterns_open));
        let db = open_db(patterns_open as u64);
        for ordinal in 0..BUSY_PATTERN_EDITS {
            insert_edit(&db, edit_row(0, ordinal), 0, ordinal as u64);
        }
        for pattern in 1..PATTERNS {
            insert_edit(&db, edit_row(pattern, 0), pattern, pattern as u64);
        }
        Self {
            db,
            patterns_open,
            query: pattern_view_query(),
        }
    }

    /// The timed operation: prepare, bind and subscribe every pattern view,
    /// consume its exact initial page, then collect the runtime and retained
    /// receipts and check the storage-backed witness contract.
    pub fn open_all(self) -> OpenPatterns {
        let mut streams = Vec::with_capacity(self.patterns_open);
        let mut shape_id = None;
        for pattern in 0..self.patterns_open {
            let prepared = self
                .db
                .prepare_query_bound(
                    &self.query,
                    BTreeMap::from([("pattern".to_owned(), Value::Uuid(pattern_row(pattern).0))]),
                )
                .expect("prepare pattern view");
            if let Some(expected_shape) = shape_id {
                assert_eq!(prepared.shape().shape_id(), expected_shape);
            } else {
                shape_id = Some(prepared.shape().shape_id());
            }
            let mut stream =
                block_on(self.db.subscribe(&prepared, local_opts())).expect("open pattern view");
            assert_eq!(
                take_initial_reset(&mut stream),
                expected_initial_rows(pattern)
            );
            streams.push(stream);
        }
        let runtime = runtime_receipt(&self.db);
        let retained = retained_receipt(&self.db);
        assert_eq!(runtime.active_subscriptions, self.patterns_open);
        assert_eq!(retained.subscriptions, self.patterns_open);
        // Storage-backed views drop source-version witnesses but must keep
        // replacement witnesses for delete/restore winner delivery.
        assert!(
            retained.version_identities == 0 && retained.versions_bytes == 0,
            "storage-backed pattern views retain source witnesses"
        );
        assert!(
            retained.replacement_entries > 0 && retained.replacements_bytes > 0,
            "storage-backed pattern views must retain replacement witnesses"
        );
        OpenPatterns {
            streams,
            runtime,
            retained,
        }
    }
}

fn runtime_receipt(db: &Db) -> RuntimeReceipt {
    let stats = db.runtime_stats_for_test();
    RuntimeReceipt {
        graph_nodes: stats.graph_nodes,
        active_subscriptions: stats.active_subscriptions,
        active_prepared_shapes: stats.active_prepared_shapes,
        active_shape_params: stats.active_shape_params,
        arrangement_count: stats.arrangement_count,
        arrangement_rows: stats.arrangement_rows,
        arrangement_encoded_bytes: stats.arrangement_encoded_bytes,
        logical_nodes_requested: stats.logical_nodes_requested,
        deduped_graph_nodes: stats.deduped_graph_nodes,
    }
}

fn retained_receipt(db: &Db) -> RetainedReceipt {
    let receipts = db.maintained_subscription_size_receipts_for_test();
    macro_rules! sum {
        ($($field:tt)+) => {
            receipts.iter().map(|receipt| receipt.$($field)+).sum()
        };
    }
    RetainedReceipt {
        subscriptions: receipts.len(),
        root_rows: sum!(root_rows),
        result_rows: sum!(footprint.result_rows),
        version_identities: sum!(footprint.version_identities),
        replacement_entries: sum!(footprint.replacement_entries),
        maintained_heap_bytes: sum!(footprint.maintained_heap_bytes),
        result_weights_bytes: sum!(footprint.result_weights_bytes),
        result_payloads_bytes: sum!(footprint.result_payloads_bytes),
        versions_bytes: sum!(footprint.versions_bytes),
        supporting_frontier_bytes: sum!(footprint.supporting_frontier_bytes),
        replacements_bytes: sum!(footprint.replacements_bytes),
        terminal_schemas_bytes: sum!(footprint.terminal_schemas_bytes),
        control_state_bytes: sum!(footprint.control_state_bytes),
        maintained_and_control_heap_bytes: sum!(footprint.total_heap_bytes),
        snapshot_bytes: sum!(snapshot_bytes),
        reset_frame_bytes: sum!(reset_frame_bytes),
    }
}

fn take_initial_reset(stream: &mut SubscriptionStream) -> BTreeSet<RowUuid> {
    match stream
        .try_next_event()
        .expect("pattern view did not emit an initial reset")
    {
        SubscriptionEvent::Delta {
            reset: true,
            added,
            updated,
            removed,
            ..
        } => {
            assert!(updated.is_empty());
            assert!(removed.is_empty());
            added.into_iter().map(|row| row.row_uuid()).collect()
        }
        other => panic!("expected initial reset, got {other:?}"),
    }
}

fn expected_initial_rows(pattern: usize) -> BTreeSet<RowUuid> {
    if pattern == 0 {
        (BUSY_PATTERN_EDITS - PAGE_SIZE..BUSY_PATTERN_EDITS)
            .map(|ordinal| edit_row(pattern, ordinal))
            .collect()
    } else {
        BTreeSet::from([edit_row(pattern, 0)])
    }
}

fn open_db(seed: u64) -> Db {
    let schema = JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new(EDITS)
                    .column("pattern", ColumnType::Uuid)
                    .column("edited_at", ColumnType::Timestamp)
                    .column("pad", ColumnType::Text),
            )
            .build(),
    )
    .expect("pattern-views schema compiles");
    let families = schema.column_families();
    let family_refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(Db::open(
        DbConfig::new(
            schema,
            MemoryStorage::new(&family_refs).expect("valid memory storage families"),
            DbIdentity {
                node: NodeUuid::from_bytes((0x7600_u128 + seed as u128).to_be_bytes()),
                author: WRITER,
            },
        )
        .with_id_source(SeededRowIdSource::new(0x7600 + seed)),
    ))
    .expect("open pattern-views db")
}

fn insert_edit(db: &Db, row: RowUuid, pattern: usize, edited_at: u64) {
    block_on(db.insert(
        EDITS,
        BTreeMap::from([
            ("pattern".to_owned(), Value::Uuid(pattern_row(pattern).0)),
            ("edited_at".to_owned(), Value::U64(edited_at)),
            (
                "pad".to_owned(),
                Value::String(format!("pattern {pattern} pad edit {edited_at}")),
            ),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    ))
    .expect("insert pattern pad edit");
}

fn pattern_row(pattern: usize) -> RowUuid {
    tagged_row(0x7601, pattern as u64)
}

fn edit_row(pattern: usize, ordinal: usize) -> RowUuid {
    tagged_row(0x7602 + pattern as u64, ordinal as u64)
}

fn tagged_row(namespace: u64, value: u64) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[..8].copy_from_slice(&namespace.to_be_bytes());
    bytes[8..].copy_from_slice(&value.to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn local_opts() -> ReadOpts {
    ReadOpts {
        tier: DurabilityTier::Local,
        local_updates: LocalUpdates::Immediate,
        propagation: Propagation::LocalOnly,
        include_deleted: false,
        ..ReadOpts::default()
    }
}

fn pattern_view_query() -> Query {
    Query::from(EDITS)
        .filter(eq(col("pattern"), param("pattern")))
        .order_by("edited_at", OrderDirection::Desc)
        .limit(PAGE_SIZE)
}
