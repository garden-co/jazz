//! Baseline receipts for complementary one-shot query shapes.
//!
//! This benchmark intentionally measures the current implementation only. It
//! does not claim that logical storage reads are physical I/O, unique rows, or
//! source decode counts. A future source-reuse implementation can compare its
//! receipts with these independent fixture lifecycles.

mod schema_fixture;
mod support;

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use jazz::db::{Db, DbConfig, DbIdentity, PreparedQuery, SeededRowIdSource};
use jazz::groove::db::{StorageReadBucket, StorageReadMetrics};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{NodeUuid, RowUuid};
use jazz::node::{CurrentRow, QueryReadProfile};
use jazz::query::{Query, col, eq, lit};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

const TABLE: &str = "todos";
const DEFAULT_ROWS: usize = 1_000;

type DirectDb = Db<MemoryStorage>;

struct PreparedQueries {
    all: PreparedQuery,
    done_true: PreparedQuery,
    done_false: PreparedQuery,
}

struct ReadSample {
    rows: Vec<CurrentRow>,
    profile: QueryReadProfile,
    storage: StorageReadMetrics,
    wall: Duration,
}

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();

    let row_count = support::env_usize("JAZZ_QUERY_SOURCE_ROWS", DEFAULT_ROWS);
    assert!(
        row_count > 0,
        "JAZZ_QUERY_SOURCE_ROWS must be greater than zero"
    );

    run_first_read(row_count);
    run_repeated_read(row_count);
    run_complementary_sequence(row_count);
}

fn schema() -> JazzSchema {
    schema_fixture::compile(
        SchemaBuilder::new().table(
            TableSchemaBuilder::new(TABLE)
                .column("done", ColumnType::Boolean)
                .column("title", ColumnType::Text),
        ),
    )
}

fn open_db(seed: u64) -> DirectDb {
    let schema = schema();
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();

    jazz::block_on(Db::open(
        DbConfig::new(
            schema,
            MemoryStorage::new(&refs).expect("valid memory storage families"),
            DbIdentity {
                node: NodeUuid::from_bytes([seed as u8; 16]),
                author: schema_fixture::account_author_uuid(uuid::uuid!(
                    "00000000-0000-0000-0000-0000000000a1"
                )),
            },
        )
        .with_id_source(SeededRowIdSource::new(seed)),
    ))
    .expect("open query-source measurement db")
}

fn seed_todos(db: &DirectDb, row_count: usize) {
    for index in 0..row_count {
        let write = jazz::block_on(db.insert(
            TABLE,
            BTreeMap::from([
                ("done".to_owned(), Value::Bool(index.is_multiple_of(2))),
                ("title".to_owned(), Value::String(format!("Todo {index}"))),
            ]),
            Default::default(),
        ))
        .expect("insert query-source measurement row");
        jazz::block_on(write.wait(jazz::tx::DurabilityTier::Local))
            .expect("settle query-source measurement row");
    }
}

fn prepare_queries(db: &DirectDb) -> PreparedQueries {
    PreparedQueries {
        all: db
            .prepare_query(&Query::from(TABLE))
            .expect("prepare all-todos query"),
        done_true: db
            .prepare_query(&Query::from(TABLE).filter(eq(col("done"), lit(true))))
            .expect("prepare done-true query"),
        done_false: db
            .prepare_query(&Query::from(TABLE).filter(eq(col("done"), lit(false))))
            .expect("prepare done-false query"),
    }
}

fn setup(seed: u64, row_count: usize) -> (DirectDb, PreparedQueries) {
    let db = open_db(seed);
    seed_todos(&db, row_count);
    let queries = prepare_queries(&db);
    (db, queries)
}

fn run_first_read(row_count: usize) {
    let (db, queries) = setup(0x30, row_count);
    let sample = read(&db, &queries.all);
    assert_row_count("first_read_q3", &sample.rows, row_count);
    emit("first_read_q3", row_count, &sample);
}

fn run_repeated_read(row_count: usize) {
    let (db, queries) = setup(0x31, row_count);
    let warmup = read(&db, &queries.all);
    assert_row_count("warmup_q3", &warmup.rows, row_count);

    let sample = read(&db, &queries.all);
    assert_row_count("repeated_read_q3", &sample.rows, row_count);
    emit("repeated_read_q3", row_count, &sample);
}

fn run_complementary_sequence(row_count: usize) {
    let (db, queries) = setup(0x32, row_count);
    let done_true = read(&db, &queries.done_true);
    let done_false = read(&db, &queries.done_false);
    let all = read(&db, &queries.all);

    let expected_true = row_count.div_ceil(2);
    let expected_false = row_count / 2;
    assert_row_count("sequence_q1_done_true", &done_true.rows, expected_true);
    assert_row_count("sequence_q2_done_false", &done_false.rows, expected_false);
    assert_row_count("sequence_q3_all", &all.rows, row_count);

    let mut partition = row_ids(&done_true.rows);
    partition.extend(row_ids(&done_false.rows));
    assert_eq!(
        partition,
        row_ids(&all.rows),
        "done=true and done=false must partition the seeded non-nullable table"
    );

    emit("sequence_q1_done_true", row_count, &done_true);
    emit("sequence_q2_done_false", row_count, &done_false);
    emit("sequence_q3_all", row_count, &all);
}

fn read(db: &DirectDb, query: &PreparedQuery) -> ReadSample {
    db.reset_storage_read_metrics_for_test();
    let started = Instant::now();
    let (rows, profile) = db
        .read_profiled(query)
        .expect("run query-source measurement read");
    let wall = started.elapsed();
    let storage = db.take_storage_read_metrics_for_test();

    ReadSample {
        rows,
        profile,
        storage,
        wall,
    }
}

fn assert_row_count(case: &str, rows: &[CurrentRow], expected: usize) {
    assert_eq!(
        rows.len(),
        expected,
        "{case} returned an unexpected number of rows"
    );
}

fn row_ids(rows: &[CurrentRow]) -> BTreeSet<RowUuid> {
    rows.iter().map(CurrentRow::row_uuid).collect()
}

fn emit(case: &str, table_rows: usize, sample: &ReadSample) {
    let mut fields = BTreeMap::new();
    fields.insert(
        "measurement".to_owned(),
        serde_json::json!("query_source_baseline"),
    );
    fields.insert("case".to_owned(), serde_json::json!(case));
    fields.insert("table_rows".to_owned(), serde_json::json!(table_rows));
    fields.insert(
        "result_rows".to_owned(),
        serde_json::json!(sample.rows.len()),
    );
    fields.insert(
        "wall_us".to_owned(),
        serde_json::json!(sample.wall.as_micros()),
    );
    insert_profile(&mut fields, &sample.profile);
    insert_storage(&mut fields, &sample.storage);
    support::emit_json_line("query_source_measurement", fields.into_iter().collect());
}

fn insert_profile(fields: &mut BTreeMap<String, serde_json::Value>, profile: &QueryReadProfile) {
    fields.insert(
        "profile_resolve_view_us".to_owned(),
        serde_json::json!(profile.resolve_view.as_micros()),
    );
    fields.insert(
        "profile_compile_program_us".to_owned(),
        serde_json::json!(profile.compile_program.as_micros()),
    );
    fields.insert(
        "profile_select_plan_us".to_owned(),
        serde_json::json!(profile.select_plan.as_micros()),
    );
    fields.insert(
        "profile_execute_plan_us".to_owned(),
        serde_json::json!(profile.execute_plan.as_micros()),
    );
    fields.insert(
        "profile_decode_materialize_us".to_owned(),
        serde_json::json!(profile.decode_materialize.as_micros()),
    );
    fields.insert(
        "profile_finish_rows_us".to_owned(),
        serde_json::json!(profile.finish_rows.as_micros()),
    );
    fields.insert(
        "profile_apply_projection_us".to_owned(),
        serde_json::json!(profile.apply_projection.as_micros()),
    );
    fields.insert(
        "profile_total_us".to_owned(),
        serde_json::json!(profile.total.as_micros()),
    );
}

fn insert_storage(fields: &mut BTreeMap<String, serde_json::Value>, metrics: &StorageReadMetrics) {
    insert_bucket(fields, "logical_storage_total", metrics.total);
    insert_bucket(
        fields,
        "logical_storage_ahead_current_rows",
        metrics.ahead_current_rows,
    );
    insert_bucket(
        fields,
        "logical_storage_global_current_rows",
        metrics.global_current_rows,
    );
    insert_bucket(
        fields,
        "logical_storage_global_current_indexes",
        metrics.global_current_indexes,
    );
    insert_bucket(fields, "logical_storage_other", metrics.other);
}

fn insert_bucket(
    fields: &mut BTreeMap<String, serde_json::Value>,
    prefix: &str,
    bucket: StorageReadBucket,
) {
    fields.insert(format!("{prefix}_reads"), serde_json::json!(bucket.reads));
    fields.insert(format!("{prefix}_ranges"), serde_json::json!(bucket.ranges));
}
