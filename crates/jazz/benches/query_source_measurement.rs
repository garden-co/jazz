//! Receipts for overlapping one-shot query shapes across realistic row widths.
//!
//! The benchmark varies seeded row cardinality and title payload width through
//! environment variables, then measures cold Q3 and Q1/Q2-to-Q3 reuse levels.
//! It asserts row membership before emitting timings. Logical storage reads
//! remain visible: reuse avoids decode/materialization work, not Groove scans or
//! physical I/O.

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
const DEFAULT_TITLE_PAYLOAD_BYTES: usize = 32;

type DirectDb = Db<MemoryStorage>;

struct PreparedQueries {
    all: PreparedQuery,
    status_done: PreparedQuery,
    status_not_done: PreparedQuery,
    status_blocked: PreparedQuery,
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
    let title_payload_bytes =
        support::env_usize("JAZZ_QUERY_SOURCE_TITLE_BYTES", DEFAULT_TITLE_PAYLOAD_BYTES);
    assert!(
        row_count > 0,
        "JAZZ_QUERY_SOURCE_ROWS must be greater than zero"
    );
    assert!(
        title_payload_bytes > 0,
        "JAZZ_QUERY_SOURCE_TITLE_BYTES must be greater than zero"
    );

    run_first_read(row_count, title_payload_bytes);
    run_one_partition_sequence(row_count, title_payload_bytes);
    run_two_partition_sequence(row_count, title_payload_bytes);
    run_all_partition_sequence(row_count, title_payload_bytes);
}

fn schema() -> JazzSchema {
    schema_fixture::compile(
        SchemaBuilder::new().table(
            TableSchemaBuilder::new(TABLE)
                .column("status", ColumnType::Text)
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

fn seed_todos(db: &DirectDb, row_count: usize, title_payload_bytes: usize) {
    for index in 0..row_count {
        let status = match index % 3 {
            0 => "done",
            1 => "not_done",
            _ => "blocked",
        };
        let title = format!("Todo {index} {}", "x".repeat(title_payload_bytes));
        let write = jazz::block_on(db.insert(
            TABLE,
            BTreeMap::from([
                ("status".to_owned(), Value::String(status.to_owned())),
                ("title".to_owned(), Value::String(title)),
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
        status_done: db
            .prepare_query(&Query::from(TABLE).filter(eq(col("status"), lit("done"))))
            .expect("prepare done-status query"),
        status_not_done: db
            .prepare_query(&Query::from(TABLE).filter(eq(col("status"), lit("not_done"))))
            .expect("prepare not-done-status query"),
        status_blocked: db
            .prepare_query(&Query::from(TABLE).filter(eq(col("status"), lit("blocked"))))
            .expect("prepare blocked-status query"),
    }
}

fn setup(seed: u64, row_count: usize, title_payload_bytes: usize) -> (DirectDb, PreparedQueries) {
    let db = open_db(seed);
    seed_todos(&db, row_count, title_payload_bytes);
    let queries = prepare_queries(&db);
    (db, queries)
}

fn run_first_read(row_count: usize, title_payload_bytes: usize) {
    let (db, queries) = setup(0x30, row_count, title_payload_bytes);
    let sample = read(&db, &queries.all);
    assert_row_count("first_read_q3", &sample.rows, row_count);
    emit("first_read_q3", row_count, title_payload_bytes, 0, &sample);
}

fn run_one_partition_sequence(row_count: usize, title_payload_bytes: usize) {
    let (db, queries) = setup(0x31, row_count, title_payload_bytes);
    let done = read(&db, &queries.status_done);
    let all = read(&db, &queries.all);

    assert_row_count("sequence_33_q1_done", &done.rows, (row_count + 2) / 3);
    assert_row_count("sequence_33_q3_all", &all.rows, row_count);
    assert_subset("sequence_33_done_subset", &done.rows, &all.rows);

    emit(
        "sequence_33_q1_done",
        row_count,
        title_payload_bytes,
        0,
        &done,
    );
    emit(
        "sequence_33_q3_all",
        row_count,
        title_payload_bytes,
        done.rows.len(),
        &all,
    );
}

fn run_two_partition_sequence(row_count: usize, title_payload_bytes: usize) {
    let (db, queries) = setup(0x32, row_count, title_payload_bytes);
    let done = read(&db, &queries.status_done);
    let not_done = read(&db, &queries.status_not_done);
    let all = read(&db, &queries.all);

    assert_row_count("sequence_67_q1_done", &done.rows, (row_count + 2) / 3);
    assert_row_count(
        "sequence_67_q2_not_done",
        &not_done.rows,
        (row_count + 1) / 3,
    );
    assert_row_count("sequence_67_q3_all", &all.rows, row_count);
    assert_disjoint("sequence_67_status_disjoint", &done.rows, &not_done.rows);
    assert_subset("sequence_67_done_subset", &done.rows, &all.rows);
    assert_subset("sequence_67_not_done_subset", &not_done.rows, &all.rows);

    emit(
        "sequence_67_q1_done",
        row_count,
        title_payload_bytes,
        0,
        &done,
    );
    emit(
        "sequence_67_q2_not_done",
        row_count,
        title_payload_bytes,
        0,
        &not_done,
    );
    emit(
        "sequence_67_q3_all",
        row_count,
        title_payload_bytes,
        done.rows.len() + not_done.rows.len(),
        &all,
    );
}

fn run_all_partition_sequence(row_count: usize, title_payload_bytes: usize) {
    let (db, queries) = setup(0x33, row_count, title_payload_bytes);
    let done = read(&db, &queries.status_done);
    let not_done = read(&db, &queries.status_not_done);
    let blocked = read(&db, &queries.status_blocked);
    let all = read(&db, &queries.all);

    assert_row_count("sequence_100_q1_done", &done.rows, (row_count + 2) / 3);
    assert_row_count(
        "sequence_100_q2_not_done",
        &not_done.rows,
        (row_count + 1) / 3,
    );
    assert_row_count("sequence_100_q3_blocked", &blocked.rows, row_count / 3);
    assert_row_count("sequence_100_q4_all", &all.rows, row_count);
    assert_partition(
        "sequence_100_status_partition",
        &[&done.rows, &not_done.rows, &blocked.rows],
        &all.rows,
    );

    emit(
        "sequence_100_q1_done",
        row_count,
        title_payload_bytes,
        0,
        &done,
    );
    emit(
        "sequence_100_q2_not_done",
        row_count,
        title_payload_bytes,
        0,
        &not_done,
    );
    emit(
        "sequence_100_q3_blocked",
        row_count,
        title_payload_bytes,
        0,
        &blocked,
    );
    emit(
        "sequence_100_q4_all",
        row_count,
        title_payload_bytes,
        done.rows.len() + not_done.rows.len() + blocked.rows.len(),
        &all,
    );
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
fn assert_subset(case: &str, subset: &[CurrentRow], all: &[CurrentRow]) {
    assert!(
        row_ids(subset).is_subset(&row_ids(all)),
        "{case} contains a row outside the all-rows result"
    );
}

fn assert_disjoint(case: &str, left: &[CurrentRow], right: &[CurrentRow]) {
    assert!(
        row_ids(left).is_disjoint(&row_ids(right)),
        "{case} contains an overlapping row"
    );
}

fn assert_partition(case: &str, parts: &[&[CurrentRow]], all: &[CurrentRow]) {
    let mut partition = BTreeSet::new();
    for part in parts {
        partition.extend(row_ids(part));
    }
    assert_eq!(partition, row_ids(all), "{case} does not cover all rows");
}

fn emit(
    case: &str,
    table_rows: usize,
    title_payload_bytes: usize,
    expected_reusable_rows: usize,
    sample: &ReadSample,
) {
    let mut fields = BTreeMap::new();
    fields.insert(
        "measurement".to_owned(),
        serde_json::json!("query_source_baseline"),
    );
    fields.insert("case".to_owned(), serde_json::json!(case));
    fields.insert("table_rows".to_owned(), serde_json::json!(table_rows));
    fields.insert(
        "title_payload_bytes".to_owned(),
        serde_json::json!(title_payload_bytes),
    );
    fields.insert(
        "result_rows".to_owned(),
        serde_json::json!(sample.rows.len()),
    );
    fields.insert(
        "expected_reusable_rows".to_owned(),
        serde_json::json!(expected_reusable_rows),
    );
    fields.insert(
        "expected_reuse_per_mille".to_owned(),
        serde_json::json!(expected_reusable_rows.saturating_mul(1_000) / table_rows),
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
