//! Direct-core authorization-scope benchmarks.
//!
//! Ports the old server authorization-scope scenario onto the public
//! `jazz::db` facade. The original benchmark measured a downstream
//! `useAll(...).limit(n)` subscription with row-level select policies; direct
//! core has no public server sync-scope API, so these cases exercise the same
//! owner-filtered query and initial subscription path through `Db`.

use std::collections::BTreeMap;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use jazz::db::{
    Db, DbConfig, DbIdentity, ReadOpts, SeededRowIdSource, SubscriptionEvent, block_on,
};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid};
use jazz::query::Query;
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::tx::DurabilityTier;

type DirectDb = Db<MemoryStorage>;

const AUTHOR: AuthorId = AuthorId(uuid::uuid!("00000000-0000-0000-0000-0000000000a1"));
const OTHER_AUTHOR: AuthorId = AuthorId(uuid::uuid!("00000000-0000-0000-0000-0000000000b2"));

fn authorization_schema(extra_columns: usize) -> JazzSchema {
    let mut columns = vec![
        ColumnSchema::new("owner_id", ColumnType::Uuid),
        ColumnSchema::new("name", ColumnType::String),
        ColumnSchema::new("score", ColumnType::U64),
    ];

    columns.extend((0..extra_columns).map(|index| {
        ColumnSchema::new(format!("metadata_{index}"), ColumnType::String)
    }));

    JazzSchema::new([TableSchema::new("items", columns)
        .with_read_policy(Policy::owner_only("items", "owner_id"))
        .with_write_policy(Policy::public())])
}

fn open_db(seed: u64, extra_columns: usize) -> DirectDb {
    let schema = authorization_schema(extra_columns);
    let column_families = schema.column_families();
    let refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();

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
    .expect("open direct authorization benchmark db")
}

fn item_cells(index: usize, extra_columns: usize) -> BTreeMap<String, Value> {
    let owner = if index % 2 == 0 { AUTHOR } else { OTHER_AUTHOR };
    let mut cells = BTreeMap::from([
        ("owner_id".to_owned(), Value::Uuid(owner.0)),
        ("name".to_owned(), Value::String(format!("Item {index}"))),
        ("score".to_owned(), Value::U64(index as u64)),
    ]);

    cells.extend((0..extra_columns).map(|column_index| {
        (
            format!("metadata_{column_index}"),
            Value::String(format!("item-{index}-metadata-{column_index}")),
        )
    }));

    cells
}

fn setup(row_count: usize, extra_columns: usize, seed: u64) -> DirectDb {
    let db = open_db(seed, extra_columns);

    for index in 0..row_count {
        let write = db
            .insert("items", item_cells(index, extra_columns))
            .expect("insert benchmark item");
        block_on(write.wait(DurabilityTier::Local)).expect("seed row should be local");
    }

    db
}

fn limited_items_query(db: &DirectDb, limit: usize) -> jazz::db::PreparedQuery {
    db.prepare_query(&Query::from("items").limit(limit))
        .expect("prepare limited items query")
}

fn read_limit(db: &DirectDb, limit: usize) -> usize {
    let query = limited_items_query(db, limit);
    db.read(&query)
        .expect("direct authorized limit read should succeed")
        .len()
}

fn subscribe_limit(db: &DirectDb, limit: usize) -> usize {
    let query = limited_items_query(db, limit);
    let mut subscription =
        block_on(db.subscribe(&query, ReadOpts::default())).expect("subscribe to limited items");

    match block_on(subscription.next_event()) {
        Some(SubscriptionEvent::Opened { current, .. }) => current.len(),
        other => panic!("expected opened subscription event, got {other:?}"),
    }
}

fn initial_authorized_scope(c: &mut Criterion) {
    let mut group = c.benchmark_group("direct_authorization_scope/initial_limit");

    for row_count in [1_000usize, 2_000, 10_000] {
        group.throughput(Throughput::Elements(row_count as u64));
        group.bench_with_input(
            BenchmarkId::new("owner_read_policy", row_count),
            &row_count,
            |b, &row_count| {
                b.iter_batched(
                    || setup(row_count, 0, row_count as u64),
                    |db| black_box(read_limit(&db, row_count)),
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

fn initial_authorized_scope_with_wide_schema(c: &mut Criterion) {
    let mut group = c.benchmark_group("direct_authorization_scope/wide_schema");

    let (row_count, extra_columns) = (2_000usize, 256usize);
    group.throughput(Throughput::Elements(row_count as u64));
    group.bench_with_input(
        BenchmarkId::new(format!("{extra_columns}_extra_columns"), row_count),
        &(row_count, extra_columns),
        |b, &(row_count, extra_columns)| {
            b.iter_batched(
                || setup(row_count, extra_columns, 20),
                |db| black_box(read_limit(&db, row_count)),
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.finish();
}

fn many_initial_authorized_scopes_share_schema_context(c: &mut Criterion) {
    let mut group = c.benchmark_group("direct_authorization_scope/many_subscriptions");

    let (row_count, extra_columns, subscription_count) = (1_000usize, 256usize, 25usize);
    group.throughput(Throughput::Elements(
        (row_count * subscription_count) as u64,
    ));
    group.bench_with_input(
        BenchmarkId::new(
            format!("{subscription_count}_subscriptions_{extra_columns}_extra_columns"),
            row_count,
        ),
        &(row_count, extra_columns, subscription_count),
        |b, &(row_count, extra_columns, subscription_count)| {
            b.iter_batched(
                || setup(row_count, extra_columns, 30),
                |db| {
                    let opened_rows = (0..subscription_count)
                        .map(|_| subscribe_limit(&db, row_count))
                        .sum::<usize>();
                    black_box(opened_rows)
                },
                criterion::BatchSize::SmallInput,
            );
        },
    );

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = initial_authorized_scope, initial_authorized_scope_with_wide_schema, many_initial_authorized_scopes_share_schema_context
}
criterion_main!(benches);
