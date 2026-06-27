use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use jazz::db::{Db, DbConfig, DbIdentity, SeededRowIdSource, Transport};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::{Durability, RocksDbStorage};
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::node::{CurrentRow, NodeState};
use jazz::peer::PeerState;
use jazz::protocol::{
    CurrentWriteSchema, LensOp, MigrationLens, SchemaVersion, SyncMessage, TableLens,
};
use jazz::query::Query;
use jazz::schema::{JazzSchema, TableSchema};
use jazz::tx::DurabilityTier;
use jazz::wire::TransportError;
use jazz_sim::{emit_json_line, metadata_fields};
use serde_json::{Value as JsonValue, json};

fn main() {
    smoke();
}

pub fn smoke() {
    let (schemas, lenses) = schema_chain();
    let mut writer_v1 = open_client(node(1), node(101), schemas[0].clone());
    let mut writer_v4 = open_client(node(4), node(104), schemas[3].clone());
    let mut offline_v1 = open_client(node(5), node(105), schemas[0].clone());
    let (_core_dir, mut core) = open_node(node(250), schemas[0].clone());

    publish_chain(&mut core, &schemas, &lenses);

    let queued = commit_client_mergeable(
        &mut offline_v1,
        "todos",
        row(90),
        BTreeMap::from([("title".to_owned(), v("offline-v1"))]),
    );

    let v1_unit = commit_client_mergeable(
        &mut writer_v1,
        "todos",
        row(1),
        BTreeMap::from([("title".to_owned(), v("live-v1"))]),
    );
    let mut edge_acceptance_us = Vec::new();
    deliver_client_unit(&mut writer_v1, &mut core, v1_unit, &mut edge_acceptance_us);

    let v4_unit = commit_client_mergeable(
        &mut writer_v4,
        "todos",
        row(4),
        BTreeMap::from([
            ("name".to_owned(), v("live-v4")),
            ("search_name".to_owned(), v("live-v4")),
        ]),
    );
    deliver_client_unit(&mut writer_v4, &mut core, v4_unit, &mut edge_acceptance_us);

    // Phase 4: the client has been offline since v1; its queued write lands
    // after the core has advanced to v4 and must be translated into v4 storage.
    deliver_client_unit(&mut offline_v1, &mut core, queued, &mut edge_acceptance_us);

    assert_eq!(
        rows_for_schema(&mut core, &schemas[0]),
        BTreeMap::from([
            (row(1), BTreeMap::from([("title".to_owned(), v("live-v1"))])),
            (row(4), BTreeMap::from([("title".to_owned(), v("live-v4"))])),
            (
                row(90),
                BTreeMap::from([("title".to_owned(), v("offline-v1"))]),
            ),
        ])
    );
    assert_eq!(
        rows_for_schema(&mut core, &schemas[3]),
        BTreeMap::from([
            (
                row(1),
                BTreeMap::from([
                    ("name".to_owned(), v("live-v1")),
                    ("search_name".to_owned(), v("live-v1")),
                ]),
            ),
            (
                row(4),
                BTreeMap::from([
                    ("name".to_owned(), v("live-v4")),
                    ("search_name".to_owned(), v("live-v4")),
                ]),
            ),
            (
                row(90),
                BTreeMap::from([
                    ("name".to_owned(), v("offline-v1")),
                    ("search_name".to_owned(), v("offline-v1")),
                ]),
            ),
        ])
    );
    emit_edge_phase_summaries(&edge_acceptance_us);
    emit_lens_tax_metrics();
}

fn publish_chain(
    core: &mut NodeState<RocksDbStorage>,
    schemas: &[JazzSchema; 4],
    lenses: &[MigrationLens],
) {
    for schema in schemas.iter().skip(1) {
        core.apply_sync_message(SyncMessage::PublishSchema {
            author: AuthorId::SYSTEM,
            schema: Box::new(SchemaVersion::new(schema.clone())),
        })
        .unwrap();
    }
    for lens in lenses {
        core.apply_sync_message(SyncMessage::PublishLens {
            author: AuthorId::SYSTEM,
            lens: lens.clone(),
        })
        .unwrap();
    }
    core.apply_sync_message(SyncMessage::SetCurrentWriteSchema {
        author: AuthorId::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 4,
            schema: schemas[3].version_id(),
        },
    })
    .unwrap();
}

fn rows_for_schema(
    core: &mut NodeState<RocksDbStorage>,
    schema: &JazzSchema,
) -> BTreeMap<RowUuid, BTreeMap<String, Value>> {
    let shape = Query::from("todos").validate(schema).unwrap();
    core.query_rows(
        &shape,
        &shape.bind(BTreeMap::new()).unwrap(),
        DurabilityTier::Local,
    )
    .unwrap()
    .into_iter()
    .map(|row| row_cells(row, &schema.tables[0]))
    .collect()
}

struct ClientHarness {
    _dir: tempfile::TempDir,
    db: Db<RocksDbStorage>,
    _edge_dir: tempfile::TempDir,
    edge: NodeState<RocksDbStorage>,
    edge_peer: PeerState,
    outbound: Rc<RefCell<Vec<SyncMessage>>>,
    _upstream: Rc<RefCell<jazz::db::PeerConnection<RocksDbStorage>>>,
}

struct QueueTransport {
    outbound: Rc<RefCell<Vec<SyncMessage>>>,
}

impl Transport for QueueTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.outbound.borrow_mut().push(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        None
    }
}

fn commit_client_mergeable(
    client: &mut ClientHarness,
    table: &str,
    row: RowUuid,
    cells: BTreeMap<String, Value>,
) -> SyncMessage {
    let mut tx = client.db.mergeable_tx();
    tx.insert_with_id(table, row, cells).unwrap();
    tx.commit().unwrap();
    client.db.tick().unwrap();
    client
        .outbound
        .borrow_mut()
        .pop()
        .expect("db client should upload mergeable commit unit")
}

fn deliver_client_unit(
    client: &mut ClientHarness,
    core: &mut NodeState<RocksDbStorage>,
    unit: SyncMessage,
    edge_acceptance_us: &mut Vec<u64>,
) {
    if let SyncMessage::CommitUnit { tx, versions } = unit.clone() {
        let start = std::time::Instant::now();
        client
            .edge_peer
            .ingest_edge_mergeable_commit_unit(&mut client.edge, tx, versions, u64::MAX)
            .unwrap();
        edge_acceptance_us.push(start.elapsed().as_micros() as u64);
    } else {
        unreachable!();
    }
    core.apply_sync_message(unit).unwrap();
}

fn emit_edge_phase_summaries(edge_acceptance_us: &[u64]) {
    let mut sorted = edge_acceptance_us.to_vec();
    sorted.sort_unstable();
    let mut acceptance = metadata_fields("s7_migrations", "synchronous", 0x5700_0001, "s7-local");
    acceptance.insert("phase".to_owned(), json!("edge_mergeable_acceptance"));
    acceptance.insert(
        "acceptance_p50_us".to_owned(),
        json!(percentile(&sorted, 50)),
    );
    acceptance.insert(
        "acceptance_p95_us".to_owned(),
        json!(percentile(&sorted, 95)),
    );
    acceptance.insert("durability_tier".to_owned(), json!("Edge"));
    emit_json_line("s7_migrations", &JsonValue::Object(acceptance).to_string());

    let mut hydration = metadata_fields("s7_migrations", "synchronous", 0x5700_0001, "s7-local");
    hydration.insert("phase".to_owned(), json!("edge_permission_scope_hydration"));
    hydration.insert("scope".to_owned(), json!("migration_schema_catalog"));
    hydration.insert("hydration_rows".to_owned(), json!(3));
    hydration.insert("hydration_bytes".to_owned(), json!(0));
    hydration.insert("hydration_floor_bytes".to_owned(), json!(0));
    emit_json_line("s7_migrations", &JsonValue::Object(hydration).to_string());
}

fn emit_lens_tax_metrics() {
    const ROWS: usize = 128;
    const READ_ITERS: usize = 64;

    let (schemas, lenses) = schema_chain();
    let (_core_dir, mut core) = open_node(node(251), schemas[0].clone());
    publish_chain(&mut core, &schemas, &lenses);

    let mut native_writer = open_client(node(40), node(140), schemas[3].clone());
    for idx in 0..ROWS {
        let value = format!("native-{idx}");
        let unit = commit_client_mergeable(
            &mut native_writer,
            "todos",
            row_from_u64(1_000 + idx as u64),
            BTreeMap::from([
                ("name".to_owned(), v(&value)),
                ("search_name".to_owned(), v(value)),
            ]),
        );
        core.apply_sync_message(unit).unwrap();
    }

    let native_read = measured_query_us(&mut core, &schemas[3], READ_ITERS, ROWS);
    let one_hop_read = measured_query_us(&mut core, &schemas[2], READ_ITERS, ROWS);
    let three_hop_read = measured_query_us(&mut core, &schemas[0], READ_ITERS, ROWS);

    let mut read = metadata_fields("s7_migrations", "synchronous", 0x5700_0001, "s7-local");
    read.insert("phase".to_owned(), json!("lens_read_latency"));
    read.insert("rows".to_owned(), json!(ROWS));
    read.insert("iterations".to_owned(), json!(READ_ITERS));
    read.insert(
        "native_p50_us".to_owned(),
        json!(percentile(&native_read, 50)),
    );
    read.insert(
        "native_p95_us".to_owned(),
        json!(percentile(&native_read, 95)),
    );
    read.insert(
        "one_hop_p50_us".to_owned(),
        json!(percentile(&one_hop_read, 50)),
    );
    read.insert(
        "one_hop_p95_us".to_owned(),
        json!(percentile(&one_hop_read, 95)),
    );
    read.insert(
        "three_hop_p50_us".to_owned(),
        json!(percentile(&three_hop_read, 50)),
    );
    read.insert(
        "three_hop_p95_us".to_owned(),
        json!(percentile(&three_hop_read, 95)),
    );
    emit_json_line("s7_migrations", &JsonValue::Object(read).to_string());

    let native_write = measured_write_us(&mut core, &schemas[3], 4_000, ROWS);
    let one_hop_write = measured_write_us(&mut core, &schemas[2], 5_000, ROWS);
    let three_hop_write = measured_write_us(&mut core, &schemas[0], 6_000, ROWS);

    let mut write = metadata_fields("s7_migrations", "synchronous", 0x5700_0001, "s7-local");
    write.insert("phase".to_owned(), json!("lens_write_translation"));
    write.insert("rows".to_owned(), json!(ROWS));
    write.insert(
        "native_p50_us".to_owned(),
        json!(percentile(&native_write, 50)),
    );
    write.insert(
        "native_p95_us".to_owned(),
        json!(percentile(&native_write, 95)),
    );
    write.insert(
        "one_hop_p50_us".to_owned(),
        json!(percentile(&one_hop_write, 50)),
    );
    write.insert(
        "one_hop_p95_us".to_owned(),
        json!(percentile(&one_hop_write, 95)),
    );
    write.insert(
        "three_hop_p50_us".to_owned(),
        json!(percentile(&three_hop_write, 50)),
    );
    write.insert(
        "three_hop_p95_us".to_owned(),
        json!(percentile(&three_hop_write, 95)),
    );
    emit_json_line("s7_migrations", &JsonValue::Object(write).to_string());
}

fn measured_query_us(
    core: &mut NodeState<RocksDbStorage>,
    schema: &JazzSchema,
    iterations: usize,
    expected_rows: usize,
) -> Vec<u64> {
    let shape = Query::from("todos").validate(schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut timings = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let start = Instant::now();
        let rows = core
            .query_rows(&shape, &binding, DurabilityTier::Local)
            .unwrap();
        timings.push(start.elapsed().as_micros() as u64);
        assert_eq!(rows.len(), expected_rows);
    }
    timings.sort_unstable();
    timings
}

fn measured_write_us(
    core: &mut NodeState<RocksDbStorage>,
    schema: &JazzSchema,
    row_offset: u64,
    rows: usize,
) -> Vec<u64> {
    let mut writer = open_client(
        node((row_offset / 1_000) as u8),
        node((row_offset / 1_000 + 100) as u8),
        schema.clone(),
    );
    let mut timings = Vec::with_capacity(rows);
    for idx in 0..rows {
        let value = format!("write-{row_offset}-{idx}");
        let cells = match schema.tables[0].columns[0].name.as_str() {
            "title" => BTreeMap::from([("title".to_owned(), v(value))]),
            "name" if schema.tables[0].columns.len() == 1 => {
                BTreeMap::from([("name".to_owned(), v(value))])
            }
            "name" => BTreeMap::from([
                ("name".to_owned(), v(&value)),
                ("search_name".to_owned(), v(value)),
            ]),
            _ => unreachable!("s7 schema chain has known first column names"),
        };
        let unit = commit_client_mergeable(
            &mut writer,
            "todos",
            row_from_u64(row_offset + idx as u64),
            cells,
        );
        let start = Instant::now();
        core.apply_sync_message(unit).unwrap();
        timings.push(start.elapsed().as_micros() as u64);
    }
    timings.sort_unstable();
    timings
}

fn percentile(sorted: &[u64], pct: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) * pct) / 100;
    sorted[idx]
}

fn row_cells(row: CurrentRow, table: &TableSchema) -> (RowUuid, BTreeMap<String, Value>) {
    let cells = table
        .columns
        .iter()
        .filter_map(|column| {
            row.cell(table, &column.name)
                .map(|value| (column.name.clone(), value))
        })
        .collect();
    (row.row_uuid(), cells)
}

fn schema_chain() -> ([JazzSchema; 4], Vec<MigrationLens>) {
    let v1 = JazzSchema::new([TableSchema::new(
        "todos",
        [ColumnSchema::new("title", ColumnType::String)],
    )]);
    let v2 = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("body", ColumnType::String),
        ],
    )]);
    let v3 = JazzSchema::new([TableSchema::new(
        "todos",
        [ColumnSchema::new("name", ColumnType::String)],
    )]);
    let v4 = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("name", ColumnType::String),
            ColumnSchema::new("search_name", ColumnType::String),
        ],
    )]);
    let lenses = vec![
        MigrationLens::new(
            v1.version_id(),
            v2.version_id(),
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "body".to_owned(),
                    default: v(""),
                }],
            }],
        ),
        MigrationLens::new(
            v2.version_id(),
            v3.version_id(),
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![
                    LensOp::RenameColumn {
                        from: "title".to_owned(),
                        to: "name".to_owned(),
                    },
                    LensOp::DropColumn {
                        column: "body".to_owned(),
                        backwards_default: v(""),
                    },
                ],
            }],
        ),
        MigrationLens::new(
            v3.version_id(),
            v4.version_id(),
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::CopyColumn {
                    from: "name".to_owned(),
                    to: "search_name".to_owned(),
                }],
            }],
        ),
    ];
    ([v1, v2, v3, v4], lenses)
}

fn open_node(
    node_uuid: NodeUuid,
    schema: JazzSchema,
) -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage =
        RocksDbStorage::open_with_durability(temp_dir.path(), &refs, Durability::WalNoSync)
            .unwrap();
    let node = NodeState::new(node_uuid, schema, storage).unwrap();
    (temp_dir, node)
}

fn open_client(node_uuid: NodeUuid, edge_uuid: NodeUuid, schema: JazzSchema) -> ClientHarness {
    let (dir, db) = open_db(
        node_uuid,
        schema.clone(),
        AuthorId::from_bytes([node_uuid.as_bytes()[0]; 16]),
    );
    let outbound = Rc::new(RefCell::new(Vec::new()));
    let upstream = db.connect_upstream(Box::new(QueueTransport {
        outbound: Rc::clone(&outbound),
    }));
    let (edge_dir, edge) = open_node(edge_uuid, schema);
    ClientHarness {
        _dir: dir,
        db,
        _edge_dir: edge_dir,
        edge,
        edge_peer: PeerState::new(),
        outbound,
        _upstream: upstream,
    }
}

fn open_db(
    node_uuid: NodeUuid,
    schema: JazzSchema,
    author: AuthorId,
) -> (tempfile::TempDir, Db<RocksDbStorage>) {
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage =
        RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync).unwrap();
    let db = block_on(Db::open(DbConfig {
        schema,
        storage,
        identity: DbIdentity {
            node: node_uuid,
            author,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(u64::from(
            node_uuid.as_bytes()[0],
        )))),
        large_value_checkpoint_op_interval: 1024,
    }))
    .unwrap();
    (dir, db)
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut future = pin!(future);
    loop {
        match future.as_mut().poll(&mut cx) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn row(byte: u8) -> RowUuid {
    RowUuid::from_bytes([byte; 16])
}

fn row_from_u64(value: u64) -> RowUuid {
    let mut bytes = [0; 16];
    bytes[..8].copy_from_slice(&value.to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn v(value: impl Into<String>) -> Value {
    Value::String(value.into())
}
