use std::collections::{BTreeMap, BTreeSet};

use jazz::block_on;
use jazz::db::{
    Db, DbConfig, DbIdentity, LocalUpdates, Propagation, ReadOpts, RemovedRow, SeededRowIdSource,
    SubscriptionEvent,
};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::RocksDbStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::node::{CurrentRow, RelationEdge, RelationSnapshot};
use jazz::query::{ArraySubquery, OrderDirection, Query};
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::tx::DurabilityTier;

#[derive(Clone, Debug, PartialEq, Eq)]
struct CanonicalRow {
    table: String,
    row: RowUuid,
    cells: Vec<(String, Option<String>)>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CanonicalSnapshot {
    roots: Vec<CanonicalRow>,
    related: Vec<CanonicalRow>,
    edges: BTreeSet<RelationEdge>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum CanonicalEvent {
    Delta {
        reset: bool,
        added: Vec<CanonicalRow>,
        updated: Vec<CanonicalRow>,
        removed: Vec<(String, RowUuid)>,
        added_related: Vec<CanonicalRow>,
        added_edges: BTreeSet<RelationEdge>,
        removed_edges: BTreeSet<RelationEdge>,
        settled: bool,
        tier: DurabilityTier,
    },
    Closed,
}

fn row(seed: u64) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[..8].copy_from_slice(&0x019e_0000_0000_7000_u64.to_be_bytes());
    bytes[8..].copy_from_slice(&seed.to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn author() -> AuthorId {
    AuthorId::from_bytes([0xa7; 16])
}

fn schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new(
            "parents",
            [
                ColumnSchema::new("label", ColumnType::String),
                ColumnSchema::new("rank", ColumnType::U32),
            ],
        )
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "children",
            [
                ColumnSchema::new("parent_id", ColumnType::Uuid),
                ColumnSchema::new("label", ColumnType::String),
                ColumnSchema::new("rank", ColumnType::U32),
            ],
        )
        .with_reference("parent_id", "parents")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
    ])
}

fn query() -> Query {
    Query::from("parents")
        .order_by("rank", OrderDirection::Asc)
        .array_subquery(
            ArraySubquery::new("children", "children", "parent_id", "id")
                .select(["label", "rank"])
                .order_by("rank", OrderDirection::Asc),
        )
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

fn open_db(
    dir: &tempfile::TempDir,
    schema: &JazzSchema,
    node_byte: u8,
    seed: u64,
) -> Db<RocksDbStorage> {
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).expect("open rocks storage");
    block_on(Db::open(
        DbConfig::new(
            schema.clone(),
            storage,
            DbIdentity {
                node: NodeUuid::from_bytes([node_byte; 16]),
                author: author(),
            },
        )
        .with_id_source(SeededRowIdSource::new(seed)),
    ))
    .expect("open db")
}

fn parent_cells(label: &str, rank: u32) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("label".to_owned(), Value::String(label.to_owned())),
        ("rank".to_owned(), Value::U32(rank)),
    ])
}

fn child_cells(parent: RowUuid, label: &str, rank: u32) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("parent_id".to_owned(), Value::Uuid(parent.0)),
        ("label".to_owned(), Value::String(label.to_owned())),
        ("rank".to_owned(), Value::U32(rank)),
    ])
}

fn seed_store(dir: &tempfile::TempDir, schema: &JazzSchema, node_byte: u8) {
    let db = open_db(dir, schema, node_byte, node_byte as u64);
    db.insert_with_id("parents", row(1), parent_cells("alpha", 1))
        .expect("insert alpha");
    db.insert_with_id("parents", row(2), parent_cells("beta", 2))
        .expect("insert beta");
    db.insert_with_id("children", row(101), child_cells(row(1), "a-1", 1))
        .expect("insert a-1");
    db.insert_with_id("children", row(102), child_cells(row(1), "a-2", 2))
        .expect("insert a-2");
    db.insert_with_id("children", row(201), child_cells(row(2), "b-1", 1))
        .expect("insert b-1");
    db.close().expect("close seeded db");
}

fn table_schema<'a>(schema: &'a JazzSchema, table: &str) -> &'a TableSchema {
    schema
        .tables
        .iter()
        .find(|candidate| candidate.name == table)
        .unwrap_or_else(|| panic!("missing schema table {table}"))
}

fn canonical_row(schema: &JazzSchema, row: &CurrentRow) -> CanonicalRow {
    let table = table_schema(schema, row.table());
    CanonicalRow {
        table: row.table().to_owned(),
        row: row.row_uuid(),
        cells: table
            .columns
            .iter()
            .map(|column| {
                (
                    column.name.clone(),
                    row.cell(table, &column.name)
                        .map(|value| format!("{value:?}")),
                )
            })
            .collect(),
    }
}

fn sort_rows(rows: &mut [CanonicalRow]) {
    rows.sort_by(|left, right| {
        (&left.table, left.row.0, &left.cells).cmp(&(&right.table, right.row.0, &right.cells))
    });
}

fn canonical_snapshot(schema: &JazzSchema, snapshot: &RelationSnapshot) -> CanonicalSnapshot {
    let mut related = snapshot
        .rows
        .iter()
        .skip(snapshot.root_count)
        .map(|row| canonical_row(schema, row))
        .collect::<Vec<_>>();
    sort_rows(&mut related);
    CanonicalSnapshot {
        roots: snapshot
            .rows
            .iter()
            .take(snapshot.root_count)
            .map(|row| canonical_row(schema, row))
            .collect(),
        related,
        edges: snapshot.edges.iter().cloned().collect(),
    }
}

fn canonical_removed(rows: Vec<RemovedRow>) -> Vec<(String, RowUuid)> {
    let mut rows = rows
        .into_iter()
        .map(|row| (row.table, row.row_uuid))
        .collect::<Vec<_>>();
    rows.sort();
    rows
}

fn canonical_event(schema: &JazzSchema, event: &SubscriptionEvent) -> CanonicalEvent {
    match event {
        SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            added_related,
            added_edges,
            removed_edges,
            settled,
            tier,
            positioned: _,
        } => {
            let mut added_related = added_related
                .iter()
                .map(|row| canonical_row(schema, row))
                .collect::<Vec<_>>();
            sort_rows(&mut added_related);
            CanonicalEvent::Delta {
                reset: *reset,
                added: added.iter().map(|row| canonical_row(schema, row)).collect(),
                updated: updated
                    .iter()
                    .map(|row| canonical_row(schema, row))
                    .collect(),
                removed: canonical_removed(removed.clone()),
                added_related,
                added_edges: added_edges.iter().cloned().collect(),
                removed_edges: removed_edges.iter().cloned().collect(),
                settled: *settled,
                tier: *tier,
            }
        }
        SubscriptionEvent::Closed => CanonicalEvent::Closed,
    }
}

fn apply_subscription_event(snapshot: &mut RelationSnapshot, event: SubscriptionEvent) {
    match event {
        SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            added_related,
            added_edges,
            removed_edges,
            ..
        } => {
            if reset {
                snapshot.rows.clear();
                snapshot.edges.clear();
                snapshot.root_count = 0;
                let related_keys = added_edges
                    .iter()
                    .map(|edge| (edge.target_table.as_str(), edge.target_row))
                    .collect::<BTreeSet<_>>();
                let mut roots = Vec::new();
                let mut related = Vec::new();
                for row in added {
                    if related_keys.contains(&(row.table(), row.row_uuid())) {
                        related.push(row);
                    } else {
                        roots.push(row);
                    }
                }
                snapshot.root_count = roots.len();
                snapshot.rows = roots;
                snapshot.rows.extend(related);
                snapshot.edges = added_edges;
                return;
            }

            for removed in removed {
                if let Some(position) =
                    snapshot
                        .rows
                        .iter()
                        .take(snapshot.root_count)
                        .position(|row| {
                            row.table() == removed.table && row.row_uuid() == removed.row_uuid
                        })
                {
                    snapshot.rows.remove(position);
                    snapshot.root_count -= 1;
                }
            }

            for row in updated {
                if let Some(position) = snapshot.rows.iter().position(|current| {
                    current.table() == row.table() && current.row_uuid() == row.row_uuid()
                }) {
                    snapshot.rows[position] = row;
                }
            }

            for row in added {
                if let Some(position) =
                    snapshot
                        .rows
                        .iter()
                        .take(snapshot.root_count)
                        .position(|current| {
                            current.table() == row.table() && current.row_uuid() == row.row_uuid()
                        })
                {
                    snapshot.rows[position] = row;
                } else {
                    snapshot.rows.insert(snapshot.root_count, row);
                    snapshot.root_count += 1;
                }
            }

            for row in added_related {
                if snapshot
                    .rows
                    .iter()
                    .take(snapshot.root_count)
                    .any(|root| root.table() == row.table() && root.row_uuid() == row.row_uuid())
                {
                    continue;
                }
                if let Some(position) =
                    snapshot
                        .rows
                        .iter()
                        .skip(snapshot.root_count)
                        .position(|current| {
                            current.table() == row.table() && current.row_uuid() == row.row_uuid()
                        })
                {
                    snapshot.rows[snapshot.root_count + position] = row;
                } else {
                    snapshot.rows.push(row);
                }
            }

            snapshot
                .edges
                .retain(|edge| !removed_edges.iter().any(|removed| removed == edge));
            for edge in added_edges {
                if !snapshot.edges.iter().any(|current| current == &edge) {
                    snapshot.edges.push(edge);
                }
            }

            let mut index = snapshot.root_count;
            while index < snapshot.rows.len() {
                let row = &snapshot.rows[index];
                let still_referenced = snapshot.edges.iter().any(|edge| {
                    edge.target_table == row.table() && edge.target_row == row.row_uuid()
                });
                if still_referenced {
                    index += 1;
                } else {
                    snapshot.rows.remove(index);
                }
            }
        }
        SubscriptionEvent::Closed => {}
    }
}

fn assert_one_shot_matches_subscription(
    schema: &JazzSchema,
    db: &Db<RocksDbStorage>,
    snapshot: &RelationSnapshot,
    label: &str,
) {
    let prepared = db.prepare_query(&query()).expect("prepare one-shot query");
    let one_shot = block_on(db.all_relation_snapshot(&prepared, local_opts()))
        .unwrap_or_else(|err| panic!("{label}: one-shot relation snapshot failed: {err}"));
    assert_eq!(
        canonical_snapshot(schema, snapshot),
        canonical_snapshot(schema, &one_shot),
        "{label}: folded subscription snapshot diverged from one-shot relation snapshot"
    );
}

fn apply_to_pair(
    rebuild: &Db<RocksDbStorage>,
    persisted_placeholder: &Db<RocksDbStorage>,
    mutation: impl Fn(&Db<RocksDbStorage>),
) {
    mutation(rebuild);
    mutation(persisted_placeholder);
}

fn next_event_after(
    db: &Db<RocksDbStorage>,
    stream: &mut jazz::db::SubscriptionStream,
    label: &str,
) -> SubscriptionEvent {
    for _ in 0..100 {
        if let Some(event) = stream.try_next_event() {
            return event;
        }
        db.tick()
            .unwrap_or_else(|err| panic!("{label}: tick while waiting for event failed: {err}"));
        std::thread::yield_now();
    }
    panic!("{label}: subscription did not emit a bounded post-mutation event");
}

#[test]
fn reopen_from_rebuild_and_persisted_placeholder_are_incrementally_equivalent() {
    let schema = schema();
    let rebuild_dir = tempfile::tempdir().expect("rebuild tempdir");
    let persisted_dir = tempfile::tempdir().expect("persisted placeholder tempdir");
    seed_store(&rebuild_dir, &schema, 0x21);
    seed_store(&persisted_dir, &schema, 0x22);

    let rebuild = open_db(&rebuild_dir, &schema, 0x31, 0x31);
    let persisted_placeholder = open_db(&persisted_dir, &schema, 0x32, 0x32);
    let rebuild_prepared = rebuild.prepare_query(&query()).expect("prepare rebuild");
    let persisted_prepared = persisted_placeholder
        .prepare_query(&query())
        .expect("prepare persisted placeholder");
    let mut rebuild_stream =
        block_on(rebuild.subscribe(&rebuild_prepared, local_opts())).expect("subscribe rebuild");
    let mut persisted_stream =
        block_on(persisted_placeholder.subscribe(&persisted_prepared, local_opts()))
            .expect("subscribe persisted placeholder");

    let rebuild_open = block_on(rebuild_stream.next_event()).expect("rebuild reset");
    let persisted_open =
        block_on(persisted_stream.next_event()).expect("persisted placeholder reset");
    assert_eq!(
        canonical_event(&schema, &rebuild_open),
        canonical_event(&schema, &persisted_open),
        "initial reopen reset events diverged"
    );

    let mut rebuild_snapshot = RelationSnapshot::default();
    let mut persisted_snapshot = RelationSnapshot::default();
    apply_subscription_event(&mut rebuild_snapshot, rebuild_open);
    apply_subscription_event(&mut persisted_snapshot, persisted_open);
    assert_eq!(
        canonical_snapshot(&schema, &rebuild_snapshot),
        canonical_snapshot(&schema, &persisted_snapshot),
        "initial reopened subscription snapshots diverged"
    );
    assert_one_shot_matches_subscription(&schema, &rebuild, &rebuild_snapshot, "rebuild open");
    assert_one_shot_matches_subscription(
        &schema,
        &persisted_placeholder,
        &persisted_snapshot,
        "persisted placeholder open",
    );

    let mutations: Vec<(&str, Box<dyn Fn(&Db<RocksDbStorage>)>)> = vec![
        (
            "related row insert",
            Box::new(|db| {
                db.insert_with_id("children", row(103), child_cells(row(1), "a-3", 3))
                    .expect("insert child");
            }),
        ),
        (
            "related row delete",
            Box::new(|db| {
                db.delete("children", row(201)).expect("delete child");
            }),
        ),
    ];

    for (label, mutation) in mutations {
        apply_to_pair(&rebuild, &persisted_placeholder, mutation);
        let rebuild_event = next_event_after(&rebuild, &mut rebuild_stream, label);
        let persisted_event =
            next_event_after(&persisted_placeholder, &mut persisted_stream, label);
        assert_eq!(
            canonical_event(&schema, &rebuild_event),
            canonical_event(&schema, &persisted_event),
            "{label}: post-reopen delta event diverged"
        );
        apply_subscription_event(&mut rebuild_snapshot, rebuild_event);
        apply_subscription_event(&mut persisted_snapshot, persisted_event);
        assert_eq!(
            canonical_snapshot(&schema, &rebuild_snapshot),
            canonical_snapshot(&schema, &persisted_snapshot),
            "{label}: folded subscription snapshots diverged"
        );
        assert_one_shot_matches_subscription(&schema, &rebuild, &rebuild_snapshot, label);
        assert_one_shot_matches_subscription(
            &schema,
            &persisted_placeholder,
            &persisted_snapshot,
            label,
        );
    }

    rebuild.close().expect("close rebuild");
    persisted_placeholder
        .close()
        .expect("close persisted placeholder");
}
