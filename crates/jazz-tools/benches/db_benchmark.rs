//! core facade benchmarks.
//!
//! These benchmarks intentionally exercise the replacement core path directly
//! instead of the legacy jazz-tools RuntimeCore stack.

use std::collections::BTreeMap;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use jazz::db::{
    Db, DbConfig, DbIdentity, ReadOpts, SeededRowIdSource, SubscriptionEvent, block_on,
};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::query::{Query, all_of, col, eq, lit};
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::tx::DurabilityTier;

type DirectDb = Db<MemoryStorage>;

const AUTHOR: AuthorId = AuthorId(uuid::uuid!("00000000-0000-0000-0000-0000000000a1"));
const OTHER_AUTHOR: AuthorId = AuthorId(uuid::uuid!("00000000-0000-0000-0000-0000000000b2"));
const USER_TEAM: RowUuid = RowUuid(uuid::uuid!("00000000-0000-0000-0000-0000000000c3"));
const PARENT_TEAM: RowUuid = RowUuid(uuid::uuid!("00000000-0000-0000-0000-0000000000d4"));

fn schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "documents",
        [
            ColumnSchema::new("folder", ColumnType::Uuid),
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("content", ColumnType::String),
            ColumnSchema::new("author", ColumnType::Uuid),
            ColumnSchema::new("created_at", ColumnType::U64),
            ColumnSchema::new("done", ColumnType::Bool),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::public())])
}

fn owner_write_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "documents",
        [
            ColumnSchema::new("folder", ColumnType::Uuid),
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("content", ColumnType::String),
            ColumnSchema::new("author", ColumnType::Uuid),
            ColumnSchema::new("created_at", ColumnType::U64),
            ColumnSchema::new("done", ColumnType::Bool),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::owner_only("documents", "author"))])
}

fn reachable_policy_schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new("teams", [ColumnSchema::new("name", ColumnType::String)])
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
        TableSchema::new(
            "team_edges",
            [
                ColumnSchema::new("member", ColumnType::Uuid),
                ColumnSchema::new("parent", ColumnType::Uuid),
            ],
        )
        .with_reference("member", "teams")
        .with_reference("parent", "teams")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "document_access",
            [
                ColumnSchema::new("document", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
            ],
        )
        .with_reference("document", "documents")
        .with_reference("team", "teams")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "documents",
            [
                ColumnSchema::new("folder", ColumnType::Uuid),
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("content", ColumnType::String),
                ColumnSchema::new("author", ColumnType::Uuid),
                ColumnSchema::new("created_at", ColumnType::U64),
                ColumnSchema::new("done", ColumnType::Bool),
            ],
        )
        .with_read_policy(Policy::shape(Query::from("documents").reachable_via(
            "document_access",
            "document",
            "team",
            lit(Value::Uuid(USER_TEAM.0)),
            "team_edges",
            "member",
            "parent",
            [],
        )))
        .with_write_policy(Policy::public()),
    ])
}

fn open_db(seed: u64) -> DirectDb {
    open_db_with_schema(seed, schema())
}

fn open_db_with_schema(seed: u64, schema: JazzSchema) -> DirectDb {
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    block_on(Db::open(
        DbConfig::new(
            schema,
            MemoryStorage::new(&refs),
            DbIdentity {
                node: NodeUuid::from_bytes([seed as u8; 16]),
                author: AUTHOR,
            },
        )
        .with_id_source(SeededRowIdSource::new(seed)),
    ))
    .expect("open core benchmark db")
}

fn row_uuid(index: usize) -> RowUuid {
    RowUuid::from_bytes([(index % 251 + 1) as u8; 16])
}

fn cells(index: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("folder".to_owned(), Value::Uuid(row_uuid(index % 32).0)),
        (
            "title".to_owned(),
            Value::String(format!("Document {index}")),
        ),
        (
            "content".to_owned(),
            Value::String(format!("Content body for document {index}")),
        ),
        ("author".to_owned(), Value::Uuid(AUTHOR.0)),
        ("created_at".to_owned(), Value::U64(index as u64)),
        ("done".to_owned(), Value::Bool(index % 2 == 0)),
    ])
}

fn filtered_cells(index: usize) -> BTreeMap<String, Value> {
    let mut cells = cells(index);
    let author = if index % 2 == 0 { AUTHOR } else { OTHER_AUTHOR };
    cells.insert("author".to_owned(), Value::Uuid(author.0));
    cells.insert("folder".to_owned(), Value::Uuid(row_uuid(index % 2).0));
    cells
}

fn seed_documents(db: &DirectDb, count: usize) -> Vec<RowUuid> {
    (0..count)
        .map(|index| {
            let write = db
                .insert("documents", cells(index))
                .expect("seed core benchmark row");
            block_on(write.wait(DurabilityTier::Local)).expect("seed row should be local");
            write.row_uuid()
        })
        .collect()
}

fn seed_filtered_documents(db: &DirectDb, count: usize) -> Vec<RowUuid> {
    (0..count)
        .map(|index| {
            let write = db
                .insert("documents", filtered_cells(index))
                .expect("seed core benchmark row");
            block_on(write.wait(DurabilityTier::Local)).expect("seed row should be local");
            write.row_uuid()
        })
        .collect()
}

fn seed_reachable_policy_fixture(db: &DirectDb, count: usize) -> Vec<RowUuid> {
    for (row, name) in [(USER_TEAM, "user team"), (PARENT_TEAM, "parent team")] {
        let write = db
            .insert_with_id(
                "teams",
                row,
                BTreeMap::from([("name".to_owned(), Value::String(name.to_owned()))]),
            )
            .expect("seed team");
        block_on(write.wait(DurabilityTier::Local)).expect("seed team should be local");
    }

    let write = db
        .insert(
            "team_edges",
            BTreeMap::from([
                ("member".to_owned(), Value::Uuid(USER_TEAM.0)),
                ("parent".to_owned(), Value::Uuid(PARENT_TEAM.0)),
            ]),
        )
        .expect("seed team edge");
    block_on(write.wait(DurabilityTier::Local)).expect("seed edge should be local");

    let rows = seed_documents(db, count);
    for row in &rows {
        let write = db
            .insert(
                "document_access",
                BTreeMap::from([
                    ("document".to_owned(), Value::Uuid(row.0)),
                    ("team".to_owned(), Value::Uuid(PARENT_TEAM.0)),
                ]),
            )
            .expect("seed document access");
        block_on(write.wait(DurabilityTier::Local)).expect("seed access should be local");
    }
    rows
}

fn all_documents_query(db: &DirectDb) -> jazz::db::PreparedQuery {
    db.prepare_query(&Query::from("documents"))
        .expect("prepare documents query")
}

fn filtered_documents_query(db: &DirectDb) -> jazz::db::PreparedQuery {
    db.prepare_query(&Query::from("documents").filter(all_of([
        eq(col("author"), lit(AUTHOR.0)),
        eq(col("folder"), lit(row_uuid(0).0)),
        eq(col("done"), lit(true)),
    ])))
    .expect("prepare filtered documents query")
}

fn core_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("db/insert");

    for initial_rows in [1_000usize] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("documents", initial_rows),
            &initial_rows,
            |b, &initial_rows| {
                let db = open_db(1);
                seed_documents(&db, initial_rows);
                let mut next = initial_rows;

                b.iter(|| {
                    next += 1;
                    let write = db
                        .insert("documents", cells(next))
                        .expect("core insert should succeed");
                    block_on(write.wait(DurabilityTier::Local)).expect("insert should be local");
                    write.row_uuid()
                });
            },
        );
    }

    group.finish();
}

fn core_update_and_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("db/update_read");

    for row_count in [1_000usize] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("documents", row_count),
            &row_count,
            |b, &row_count| {
                let db = open_db(2);
                let rows = seed_documents(&db, row_count);
                let query = all_documents_query(&db);
                let mut index = 0usize;

                b.iter(|| {
                    let row = rows[index % rows.len()];
                    index += 1;
                    db.update(
                        "documents",
                        row,
                        BTreeMap::from([(
                            "content".to_owned(),
                            Value::String(format!("Updated content {index}")),
                        )]),
                    )
                    .expect("core update should succeed");
                    db.read(&query).expect("core read should succeed").len()
                });
            },
        );
    }

    group.finish();
}

fn core_filtered_prepared_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("db/filtered_prepared_read");

    for row_count in [1_000usize] {
        group.throughput(Throughput::Elements(row_count as u64));
        group.bench_with_input(
            BenchmarkId::new("documents", row_count),
            &row_count,
            |b, &row_count| {
                let db = open_db(5);
                seed_filtered_documents(&db, row_count);
                let query = filtered_documents_query(&db);

                b.iter(|| {
                    db.read(&query)
                        .expect("core filtered read should succeed")
                        .len()
                });
            },
        );
    }

    group.finish();
}

fn core_reachable_policy_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("db/reachable_policy_read");

    for row_count in [1_000usize] {
        group.throughput(Throughput::Elements(row_count as u64));
        group.bench_with_input(
            BenchmarkId::new("documents", row_count),
            &row_count,
            |b, &row_count| {
                let db = open_db_with_schema(6, reachable_policy_schema());
                seed_reachable_policy_fixture(&db, row_count);
                let query = all_documents_query(&db);

                b.iter(|| {
                    db.read(&query)
                        .expect("core reachable policy read should succeed")
                        .len()
                });
            },
        );
    }

    group.finish();
}

fn core_subscribed_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("db/subscribed_write");

    for row_count in [1_000usize] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("documents", row_count),
            &row_count,
            |b, &row_count| {
                let db = open_db(3);
                seed_documents(&db, row_count);
                let query = all_documents_query(&db);
                let mut subscription =
                    block_on(db.subscribe(&query, ReadOpts::default())).expect("subscribe");
                match block_on(subscription.next_event()) {
                    Some(SubscriptionEvent::Opened { current, .. }) => {
                        assert_eq!(current.rows.len(), row_count);
                    }
                    other => panic!("expected opened subscription event, got {other:?}"),
                }
                let mut next = row_count;

                b.iter(|| {
                    next += 1;
                    db.insert("documents", cells(next))
                        .expect("core subscribed insert should succeed");
                    match block_on(subscription.next_event()) {
                        Some(SubscriptionEvent::Delta { added, .. }) => added.len(),
                        other => panic!("expected subscription delta event, got {other:?}"),
                    }
                });
            },
        );
    }

    group.finish();
}

fn core_owner_policy_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("db/owner_policy_insert");

    for initial_rows in [1_000usize] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("documents", initial_rows),
            &initial_rows,
            |b, &initial_rows| {
                let db = open_db_with_schema(4, owner_write_schema());
                seed_documents(&db, initial_rows);
                let mut next = initial_rows;

                b.iter(|| {
                    next += 1;
                    let candidate = cells(next);
                    assert!(
                        db.can_insert("documents", candidate.clone())
                            .expect("owner policy dry run should succeed")
                    );
                    let write = db
                        .insert("documents", candidate)
                        .expect("owner policy insert should succeed");
                    block_on(write.wait(DurabilityTier::Local))
                        .expect("owner policy insert should be local");
                    write.row_uuid()
                });
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = core_insert, core_update_and_read, core_filtered_prepared_read, core_subscribed_write, core_owner_policy_insert, core_reachable_policy_read
}
criterion_main!(benches);
