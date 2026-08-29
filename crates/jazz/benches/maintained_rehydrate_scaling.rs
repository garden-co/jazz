use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

mod schema_fixture;
mod support;

use support::BenchFutureExt as _;

use jazz::groove::records::Value;
use jazz::ids::{NodeUuid, RowUuid};
use jazz::node::{MergeableCommit, NodeState, SKEW_TOLERANCE_MS};
use jazz::peer::PeerState;
use jazz::protocol::{SubscriptionKey, SyncMessage, expand_version_carriers};
use jazz::query::{Query, col, eq, lit};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::{Fate, TxId};
use jazz::wire::encode_sync_message;
use jazz_storage_rocksdb::{Durability, RocksDbStorage};
use serde_json::json;
use sha2::{Digest, Sha256};
use support::{csv_usizes, emit_json_line, phase_fields};

const TABLE: &str = "documents";
const ACTIVE: &str = "active";
const INACTIVE: &str = "inactive";

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();

    for source_rows in csv_usizes("JAZZ_PERF5_ROWS", "100,1000,10000") {
        assert!(source_rows >= 2, "PERF-5 rungs require at least two rows");
        run_rung(source_rows);
    }
}

fn run_rung(source_rows: usize) {
    let seed_started = Instant::now();
    let mut maintained_fixture = Fixture::new();
    let maintained_changed_row = maintained_fixture.seed(source_rows);
    let mut rehydrated_fixture = Fixture::new();
    let rehydrated_changed_row = rehydrated_fixture.seed(source_rows);
    assert!(
        !std::ptr::eq(&maintained_fixture.core, &rehydrated_fixture.core),
        "maintained and rehydrated lanes must use independent core state"
    );
    assert_eq!(maintained_changed_row, rehydrated_changed_row);
    let seed_us = seed_started.elapsed().as_micros();
    let shape = Query::from(TABLE)
        .filter(eq(col("status"), lit(Value::String(ACTIVE.to_owned()))))
        .validate(&schema())
        .expect("validate PERF-5 query");
    let binding = shape.bind(BTreeMap::new()).expect("bind PERF-5 query");
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: Default::default(),
    };

    let mut maintained = PeerState::new();
    maintained
        .rehydrate_query(&mut maintained_fixture.core, &shape, &binding)
        .expect("prime maintained subscription");
    maintained.metrics = Default::default();

    maintained_fixture.update_to_active(maintained_changed_row);
    rehydrated_fixture.update_to_active(rehydrated_changed_row);

    maintained_fixture.core.reset_storage_read_metrics();
    let maintained_started = Instant::now();
    let maintained_update = maintained
        .query_update(&mut maintained_fixture.core, &shape, &binding)
        .expect("serve maintained delta");
    let maintained_us = maintained_started.elapsed().as_micros();
    let maintained_reads = maintained_fixture.core.storage_read_metrics();
    let maintained_bytes = encode_sync_message(&maintained_update)
        .expect("encode maintained delta")
        .len();
    let maintained_result = maintained
        .subscription_result_sets(subscription)
        .expect("maintained result set");
    let maintained_footprint = maintained.maintained_subscription_view_metrics().footprint;

    rehydrated_fixture.core.reset_storage_read_metrics();
    let mut rehydrated = PeerState::new();
    let rehydrate_started = Instant::now();
    let rehydrate_update = rehydrated
        .rehydrate_query(&mut rehydrated_fixture.core, &shape, &binding)
        .expect("serve full rehydrate");
    let rehydrate_us = rehydrate_started.elapsed().as_micros();
    let rehydrate_reads = rehydrated_fixture.core.storage_read_metrics();
    let rehydrate_bytes = encode_sync_message(&rehydrate_update)
        .expect("encode full rehydrate")
        .len();
    let rehydrated_result = rehydrated
        .subscription_result_sets(subscription)
        .expect("rehydrated result set");
    let rehydrated_footprint = rehydrated.maintained_subscription_view_metrics().footprint;

    assert_eq!(maintained_result, rehydrated_result);
    assert_eq!(maintained_footprint, rehydrated_footprint);
    let expected_view_rows = source_rows.div_ceil(2) + 1;
    assert_eq!(maintained_result.len(), expected_view_rows);
    let (maintained_adds, maintained_removes, maintained_bundles) =
        update_counts(&maintained_update);
    let (rehydrate_adds, rehydrate_removes, rehydrate_bundles) = update_counts(&rehydrate_update);
    assert_eq!(maintained_adds, 1);
    assert_eq!(maintained_removes, 0);
    assert_eq!(maintained_bundles, 1);
    assert_eq!(rehydrate_adds, expected_view_rows);
    assert_eq!(rehydrate_removes, 0);
    assert_eq!(rehydrate_bundles, expected_view_rows);

    let digest = result_digest(&maintained_result);
    let mut fields = phase_fields("maintained_vs_rehydrate", maintained_us + rehydrate_us);
    fields.insert("source_rows".to_owned(), json!(source_rows));
    fields.insert("view_rows".to_owned(), json!(expected_view_rows));
    fields.insert("changed_rows".to_owned(), json!(1));
    fields.insert("seed_us".to_owned(), json!(seed_us));
    fields.insert("result_digest".to_owned(), json!(digest));
    fields.insert("maintained_us".to_owned(), json!(maintained_us));
    fields.insert("maintained_bytes".to_owned(), json!(maintained_bytes));
    fields.insert("maintained_adds".to_owned(), json!(maintained_adds));
    fields.insert("maintained_removes".to_owned(), json!(maintained_removes));
    fields.insert("maintained_bundles".to_owned(), json!(maintained_bundles));
    fields.insert(
        "maintained_storage_reads".to_owned(),
        json!(maintained_reads.total.reads),
    );
    fields.insert(
        "maintained_storage_ranges".to_owned(),
        json!(maintained_reads.total.ranges),
    );
    fields.insert(
        "maintained_retained_rows".to_owned(),
        json!(maintained_footprint.result_rows),
    );
    fields.insert(
        "maintained_retained_heap_bytes".to_owned(),
        json!(maintained_footprint.total_heap_bytes),
    );
    fields.insert("rehydrate_us".to_owned(), json!(rehydrate_us));
    fields.insert("rehydrate_bytes".to_owned(), json!(rehydrate_bytes));
    fields.insert("rehydrate_adds".to_owned(), json!(rehydrate_adds));
    fields.insert("rehydrate_removes".to_owned(), json!(rehydrate_removes));
    fields.insert("rehydrate_bundles".to_owned(), json!(rehydrate_bundles));
    fields.insert(
        "rehydrate_storage_reads".to_owned(),
        json!(rehydrate_reads.total.reads),
    );
    fields.insert(
        "rehydrate_storage_ranges".to_owned(),
        json!(rehydrate_reads.total.ranges),
    );
    fields.insert(
        "rehydrate_retained_rows".to_owned(),
        json!(rehydrated_footprint.result_rows),
    );
    fields.insert(
        "rehydrate_retained_heap_bytes".to_owned(),
        json!(rehydrated_footprint.total_heap_bytes),
    );
    emit_json_line("maintained_rehydrate_scaling", fields);
}

fn update_counts(update: &SyncMessage) -> (usize, usize, usize) {
    let SyncMessage::ViewUpdate(jazz::protocol::ViewUpdatePayload {
        version_carriers,
        result_member_adds,
        result_member_removes,
        ..
    }) = update
    else {
        panic!("expected one view update");
    };
    let bundles = expand_version_carriers(version_carriers)
        .expect("expand PERF-5 version carriers")
        .len();
    (
        result_member_adds.len(),
        result_member_removes.len(),
        bundles,
    )
}

fn result_digest(result: &BTreeSet<TxId>) -> String {
    let bytes = postcard::to_allocvec(result).expect("encode result digest input");
    hex::encode(Sha256::digest(bytes))
}

struct Fixture {
    writer: NodeState<RocksDbStorage>,
    core: NodeState<RocksDbStorage>,
    _dirs: Vec<tempfile::TempDir>,
}

impl Fixture {
    fn new() -> Self {
        let schema = schema();
        let (writer_dir, writer) = open_node(node(1), schema.clone());
        let (core_dir, core) = open_node(node(2), schema);
        Self {
            writer,
            core,
            _dirs: vec![writer_dir, core_dir],
        }
    }

    fn seed(&mut self, rows: usize) -> (RowUuid, TxId) {
        let mut changed_row = None;
        for index in 0..rows {
            let row_uuid = row(index);
            let status = if index.is_multiple_of(2) {
                ACTIVE
            } else {
                INACTIVE
            };
            let tx_id = self.commit(
                MergeableCommit::new(TABLE, row_uuid, 1_000 + index as u64)
                    .cells(cells(index, status)),
            );
            if index == 1 {
                changed_row = Some((row_uuid, tx_id));
            }
        }
        changed_row.expect("fixture contains an inactive row")
    }

    fn update_to_active(&mut self, (row_uuid, parent): (RowUuid, TxId)) {
        self.commit(
            MergeableCommit::new(TABLE, row_uuid, 1_000_000)
                .parents(vec![parent])
                .cells(cells(1, ACTIVE)),
        );
    }

    fn commit(&mut self, commit: MergeableCommit) -> TxId {
        let (publication, unit) = self
            .writer
            .commit_mergeable_unit(commit)
            .expect("create fixture commit");
        let tx_id = support::settle_transaction(&mut self.writer, publication);
        let fate = core_ingest(&mut self.core, &unit, u64::MAX - SKEW_TOLERANCE_MS);
        assert!(matches!(
            fate,
            SyncMessage::FateUpdate {
                fate: Fate::Accepted,
                ..
            }
        ));
        tx_id
    }
}

fn core_ingest(
    core: &mut NodeState<RocksDbStorage>,
    message: &SyncMessage,
    now_ms: u64,
) -> SyncMessage {
    let SyncMessage::CommitUnit { tx, versions } = message else {
        panic!("expected commit unit");
    };
    let outcome = core
        .ingest_commit_unit(tx.clone(), versions.clone(), now_ms)
        .expect("core ingest");
    let [fate] = support::settle_outcome(core, outcome)
        .try_into()
        .expect("one fate update");
    fate
}

fn schema() -> JazzSchema {
    schema_fixture::compile(
        SchemaBuilder::new().table(
            TableSchemaBuilder::new(TABLE)
                .column("title", ColumnType::Text)
                .column("status", ColumnType::Text),
        ),
    )
}

fn open_node(
    node_uuid: NodeUuid,
    schema: JazzSchema,
) -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let storage =
        RocksDbStorage::open_with_durability(temp_dir.path(), &refs, Durability::WalNoSync)
            .expect("open RocksDB");
    let node = NodeState::new(node_uuid, schema, storage).expect("open node");
    (temp_dir, node)
}

fn cells(index: usize, status: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "title".to_owned(),
            Value::String(format!("document-{index}")),
        ),
        ("status".to_owned(), Value::String(status.to_owned())),
    ])
}

fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn row(index: usize) -> RowUuid {
    let mut bytes = [9; 16];
    bytes[..8].copy_from_slice(&(index as u64).to_le_bytes());
    RowUuid::from_bytes(bytes)
}
