mod schema_fixture;
mod support;

use support::BenchFutureExt as _;

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Instant;

use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, LocalUpdates, MergeableTxOps, Propagation, ReadOpts,
    SeededRowIdSource, block_on,
};
use jazz::groove::db::StorageReadMetrics;
use jazz::groove::records::Value;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::schema::JazzSchema;
use jazz::tools::public_schema::{CmpOp, PolicyValue};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use jazz_storage_rocksdb::RocksDbStorage;
use serde_json::{Map, json};

const TABLE: &str = "documents";
const SCOPE_TABLE: &str = "document_scopes";
const SHARED_TABLE: &str = "shared_documents";
const ACCESS_TABLE: &str = "document_access";
const AUTHOR_UUID: uuid::Uuid = uuid::uuid!("00000000-0000-0000-0000-0000000000a1");
const OTHER_AUTHOR_UUID: uuid::Uuid = uuid::uuid!("00000000-0000-0000-0000-0000000000b2");

fn author() -> AuthorSubject {
    AuthorSubject::for_test_uuid(AUTHOR_UUID)
}

fn other_author() -> AuthorSubject {
    AuthorSubject::for_test_uuid(OTHER_AUTHOR_UUID)
}

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();

    let table_rows = support::csv_usizes("JAZZ_OWNER_FILTER_ROWS", "10000,100000");
    let owned_rows = support::env_usize("JAZZ_OWNER_FILTER_OWNED_ROWS", 500);
    let result_rows = support::env_usize("JAZZ_OWNER_FILTER_RESULT_ROWS", 500);
    let batch_rows = support::env_usize("JAZZ_OWNER_FILTER_BATCH_ROWS", 1000);

    for rows in table_rows {
        assert!(rows >= owned_rows);
        run_rung(rows, owned_rows, result_rows, batch_rows);
    }
}

fn run_rung(table_rows: usize, owned_rows: usize, result_rows: usize, batch_rows: usize) {
    let temp = tempfile::tempdir().expect("create owner-filter RocksDB directory");
    let schema = schema();
    let db = open_db(temp.path(), schema.clone());
    db.set_identity_claims(
        author(),
        BTreeMap::from([("user_id".to_owned(), Value::Uuid(AUTHOR_UUID))]),
    );

    let seed_started = Instant::now();
    seed_rows(&db, table_rows, owned_rows, batch_rows);
    let seed_us = seed_started.elapsed().as_micros();
    db.reset_storage_read_metrics_for_test();

    let cases = [
        (
            "system_point_local",
            point_query(row(0)),
            1,
            DurabilityTier::Local,
            AuthorSubject::SYSTEM,
        ),
        (
            "policy_point_local",
            point_query(row(0)),
            1,
            DurabilityTier::Local,
            author(),
        ),
        (
            "system_exists_point_local",
            shared_point_query(),
            1,
            DurabilityTier::Local,
            AuthorSubject::SYSTEM,
        ),
        (
            "policy_exists_point_local",
            shared_point_query(),
            1,
            DurabilityTier::Local,
            author(),
        ),
        (
            "policy_only",
            policy_only_query(),
            owned_rows,
            DurabilityTier::Local,
            author(),
        ),
        (
            "owner_predicate_all",
            owner_predicate_query(),
            owned_rows,
            DurabilityTier::Local,
            author(),
        ),
        (
            "owner_predicate_ordered_limit",
            owner_predicate_query()
                .order_by("updated_at", OrderDirection::Desc)
                .limit(result_rows),
            result_rows,
            DurabilityTier::Local,
            author(),
        ),
    ];

    for (case, query, expected_rows, tier, identity) in cases {
        let open_metrics = db.take_storage_read_metrics_for_test();
        db.reset_storage_read_metrics_for_test();
        let prepare_started = Instant::now();
        let prepared = db
            .prepare_query(&query)
            .expect("prepare owner-filter query");
        let prepare_us = prepare_started.elapsed().as_micros();
        let prepare_metrics = db.take_storage_read_metrics_for_test();

        db.reset_storage_read_metrics_for_test();
        let query_started = Instant::now();
        let rows = block_on(db.all_for_identity(&prepared, read_opts(tier), identity))
            .expect("run owner-filter query");
        let query_us = query_started.elapsed().as_micros();
        let query_metrics = db.take_storage_read_metrics_for_test();

        assert_eq!(rows.len(), expected_rows, "{case} row count changed");
        emit_case(
            case,
            table_rows,
            owned_rows,
            expected_rows,
            seed_us,
            prepare_us,
            query_us,
            &open_metrics,
            &prepare_metrics,
            &query_metrics,
        );
    }
    db.close().expect("close owner-filter db");
}

fn schema() -> JazzSchema {
    let shared_access = PolicyExpr::Exists {
        table: ACCESS_TABLE.to_owned(),
        condition: Box::new(PolicyExpr::And(vec![
            PolicyExpr::Cmp {
                column: "scope".to_owned(),
                op: CmpOp::Eq,
                value: PolicyValue::SessionRef(vec![
                    "__jazz_outer_row".to_owned(),
                    "scope".to_owned(),
                ]),
            },
            schema_fixture::session_user_id_column("user"),
        ])),
    };
    schema_fixture::compile(
        SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new(TABLE)
                    .column("owner", ColumnType::Uuid)
                    .column("active", ColumnType::Boolean)
                    .column("updated_at", ColumnType::Timestamp)
                    .column("title", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(schema_fixture::session_user_id_column("owner")),
                    )
                    .index_only(["owner", "updated_at", "title"]),
            )
            .table(TableSchemaBuilder::new(SCOPE_TABLE).column("name", ColumnType::Text))
            .table(
                TableSchemaBuilder::new(SHARED_TABLE)
                    .fk_column("scope", SCOPE_TABLE)
                    .column("title", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(shared_access))
                    .index_only(["scope", "title"]),
            )
            .table(
                TableSchemaBuilder::new(ACCESS_TABLE)
                    .fk_column("scope", SCOPE_TABLE)
                    .column("user", ColumnType::Uuid)
                    .index_only(["scope", "user"]),
            ),
    )
}

fn open_db(path: &Path, schema: JazzSchema) -> Db<RocksDbStorage> {
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();

    block_on(Db::open_history_complete(
        DbConfig::new(
            schema,
            RocksDbStorage::open(path, &refs).expect("open owner-filter RocksDB"),
            DbIdentity {
                node: NodeUuid::from_bytes([0x51; 16]),
                author: author(),
            },
        )
        .with_id_source(SeededRowIdSource::new(0x51)),
    ))
    .expect("open owner-filter Jazz db")
}

fn seed_rows(db: &Db<RocksDbStorage>, table_rows: usize, owned_rows: usize, batch_rows: usize) {
    for batch_start in (0..table_rows).step_by(batch_rows) {
        let batch_end = table_rows.min(batch_start + batch_rows);
        block_on(db.transaction(async |tx| {
            for index in batch_start..batch_end {
                let owner = if index < owned_rows {
                    author()
                } else {
                    other_author()
                };
                tx.insert(
                    TABLE,
                    BTreeMap::from([
                        ("owner".to_owned(), Value::Uuid(owner.test_uuid())),
                        ("active".to_owned(), Value::Bool(true)),
                        ("updated_at".to_owned(), Value::U64(index as u64)),
                        (
                            "title".to_owned(),
                            Value::String(format!("document-{index}")),
                        ),
                    ]),
                    InsertOptions {
                        row_id: Some(row(index)),
                        ..Default::default()
                    },
                )
                .await?;
                tx.insert(
                    SCOPE_TABLE,
                    BTreeMap::from([("name".to_owned(), Value::String(format!("scope-{index}")))]),
                    InsertOptions {
                        row_id: Some(row(index)),
                        ..Default::default()
                    },
                )
                .await?;
                tx.insert(
                    SHARED_TABLE,
                    BTreeMap::from([
                        ("scope".to_owned(), Value::Uuid(row(index).0)),
                        (
                            "title".to_owned(),
                            Value::String(format!("shared-document-{index}")),
                        ),
                    ]),
                    InsertOptions {
                        row_id: Some(row(index)),
                        ..Default::default()
                    },
                )
                .await?;
                if index < owned_rows {
                    tx.insert(
                        ACCESS_TABLE,
                        BTreeMap::from([
                            ("scope".to_owned(), Value::Uuid(row(index).0)),
                            ("user".to_owned(), Value::Uuid(AUTHOR_UUID)),
                        ]),
                        InsertOptions {
                            row_id: Some(access_row(index)),
                            ..Default::default()
                        },
                    )
                    .await?;
                }
            }
            Ok(())
        }))
        .expect("seed owner-filter transaction");
    }
}

fn row(index: usize) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[0] = 0x61;
    bytes[8..].copy_from_slice(&(index as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn access_row(index: usize) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[0] = 0x62;
    bytes[8..].copy_from_slice(&(index as u64).to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn policy_only_query() -> Query {
    Query::from(TABLE)
}

fn owner_predicate_query() -> Query {
    Query::from(TABLE)
        .filter(eq(col("owner"), lit(AUTHOR_UUID)))
        .filter(eq(col("active"), lit(true)))
}

fn point_query(_row: RowUuid) -> Query {
    Query::from(TABLE).filter(eq(col("title"), lit("document-0")))
}

fn shared_point_query() -> Query {
    Query::from(SHARED_TABLE).filter(eq(col("title"), lit("shared-document-0")))
}

fn read_opts(tier: DurabilityTier) -> ReadOpts {
    ReadOpts {
        tier,
        local_updates: LocalUpdates::Deferred,
        propagation: Propagation::LocalOnly,
        include_deleted: false,
        ..ReadOpts::default()
    }
}

#[allow(clippy::too_many_arguments)]
fn emit_case(
    case: &str,
    table_rows: usize,
    owned_rows: usize,
    result_rows: usize,
    seed_us: u128,
    prepare_us: u128,
    query_us: u128,
    open_metrics: &StorageReadMetrics,
    prepare_metrics: &StorageReadMetrics,
    query_metrics: &StorageReadMetrics,
) {
    let mut fields = Map::new();
    fields.insert("phase".to_owned(), json!("owner_filter_diagnostic"));
    fields.insert("case".to_owned(), json!(case));
    fields.insert("table_rows".to_owned(), json!(table_rows));
    fields.insert("owned_rows".to_owned(), json!(owned_rows));
    fields.insert("result_rows".to_owned(), json!(result_rows));
    fields.insert("seed_us".to_owned(), json!(seed_us));
    fields.insert("prepare_us".to_owned(), json!(prepare_us));
    fields.insert("query_us".to_owned(), json!(query_us));
    insert_metrics(&mut fields, "open", open_metrics);
    insert_metrics(&mut fields, "prepare", prepare_metrics);
    insert_metrics(&mut fields, "query", query_metrics);
    support::emit_json_line("owner_filter_diagnostic", fields);
}

fn insert_metrics(
    fields: &mut Map<String, serde_json::Value>,
    prefix: &str,
    metrics: &StorageReadMetrics,
) {
    fields.insert(
        format!("{prefix}_logical_reads"),
        json!(metrics.total.reads),
    );
    fields.insert(
        format!("{prefix}_logical_ranges"),
        json!(metrics.total.ranges),
    );
    fields.insert(
        format!("{prefix}_global_current_row_reads"),
        json!(metrics.global_current_rows.reads),
    );
    fields.insert(
        format!("{prefix}_global_current_index_reads"),
        json!(metrics.global_current_indexes.reads),
    );
}
