#![cfg(feature = "test")]

mod support;

use std::time::Duration;
use std::{path::Path, process::Stdio};

use jazz::row_input;
use jazz::tools::server::JazzServer;
use jazz::tools::{
    ColumnDescriptor, ColumnType, DurabilityTier, LargeValueKind, ObjectId, QueryBuilder,
    RowDescriptor, Schema, SchemaBuilder, TableName, TableSchema, Value,
};
use support::{
    TestingClient, collect_stream_deltas, has_added, has_any_change, has_removed, has_updated,
    wait_for_subscription_update,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_postgres::{NoTls, types::Type};

const READY_TIMEOUT: Duration = Duration::from_secs(25);
const PRODUCT_APP_ID: &str = "00000000-0000-0000-0000-000000000123";

fn documents_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("documents")
                .column("team_id", ColumnType::Text)
                .column("title", ColumnType::Text)
                .nullable_column("optional_note", ColumnType::Text)
                .column("created_at", ColumnType::BigInt)
                .index_only(["team_id", "title", "optional_note", "created_at"]),
        )
        .build()
}

fn postgres_large_values_schema() -> Schema {
    [(
        TableName::new("assets"),
        TableSchema::new(RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("body", ColumnType::Bytea).large_value(LargeValueKind::Text),
            ColumnDescriptor::new("data", ColumnType::Bytea).large_value(LargeValueKind::Blob),
        ])),
    )]
    .into_iter()
    .collect()
}

fn postgres_nullable_large_values_schema() -> Schema {
    [(
        TableName::new("nullable_assets"),
        TableSchema::new(RowDescriptor::new(vec![
            ColumnDescriptor::new("data", ColumnType::Bytea)
                .nullable()
                .large_value(LargeValueKind::Blob),
        ])),
    )]
    .into_iter()
    .collect()
}

fn handle_shaped_blob_payload(row: uuid::Uuid) -> Vec<u8> {
    fn push_string(bytes: &mut Vec<u8>, value: &str) {
        bytes.extend_from_slice(&(value.len() as u32).to_be_bytes());
        bytes.extend_from_slice(value.as_bytes());
    }

    let mut bytes = b"JLVH1".to_vec();
    push_string(&mut bytes, "assets");
    bytes.extend_from_slice(row.as_bytes());
    push_string(&mut bytes, "data");
    bytes.extend_from_slice(&77_u64.to_be_bytes());
    bytes.extend_from_slice(
        uuid::Uuid::parse_str("00000000-0000-4000-8000-0000000000ff")
            .expect("handle-shaped payload node UUID")
            .as_bytes(),
    );
    bytes.push(2); // LargeValueKind::Blob
    bytes.extend_from_slice(&123_u64.to_be_bytes());
    bytes.extend_from_slice(&0_u64.to_be_bytes());
    bytes
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_driver_lists_catalogue_and_paginates_jazz_rows() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = documents_schema();
            let server = JazzServer::builder()
                .with_schema(schema.clone())
                .with_postgres_port(0)
                .with_persistent_storage()
                .start()
                .await;
            let writer = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("00000000-0000-4000-8000-000000000099")
                .as_admin()
                .ready_on("documents", READY_TIMEOUT)
                .connect()
                .await;
            for created_at in 0_i64..5 {
                let (_, _, batch) = writer
                    .insert(
                        "documents",
                        row_input!(
                            "team_id" => "team-a",
                            "title" => format!("document-{created_at}"),
                            "optional_note" => format!("document-{created_at}"),
                            "created_at" => created_at,
                        ),
                    )
                    .expect("insert document");
                writer
                    .wait_for_batch(batch, DurabilityTier::GlobalServer)
                    .await
                    .expect("document settles on core server");
            }
            let (_, _, batch) = writer
                .insert(
                    "documents",
                    row_input!(
                        "team_id" => "team-b",
                        "title" => "other-team-document",
                        "optional_note" => Value::Null,
                        "created_at" => 99_i64,
                    ),
                )
                .expect("insert other team document");
            writer
                .wait_for_batch(batch, DurabilityTier::GlobalServer)
                .await
                .expect("other team document settles on core server");

            let url = server.postgres_url().expect("PostgreSQL URL");
            let (mut client, connection) = tokio_postgres::connect(&url, NoTls)
                .await
                .expect("connect with a PostgreSQL driver");
            let connection_task = tokio::spawn(async move {
                connection
                    .await
                    .expect("PostgreSQL connection remains healthy")
            });

            let database_rows = client
                .query("SELECT datname FROM pg_database", &[])
                .await
                .expect("list databases through extended query protocol");
            assert_eq!(database_rows.len(), 1);
            assert_eq!(
                database_rows[0].get::<_, String>(0),
                server.app_id().to_string()
            );

            let table_rows = client
                .query(
                    "SELECT table_name FROM information_schema.tables \
                     WHERE table_schema = $1 ORDER BY table_name",
                    &[&"public"],
                )
                .await
                .expect("list tables through a parameterized catalogue query");
            assert_eq!(table_rows.len(), 1);
            assert_eq!(table_rows[0].get::<_, String>(0), "documents");

            let column_rows = client
                .query(
                    "SELECT column_name, data_type FROM information_schema.columns \
                     WHERE table_schema = 'public' AND table_name = $1 \
                     ORDER BY ordinal_position",
                    &[&"documents"],
                )
                .await
                .expect("list table columns");
            let column_names = column_rows
                .iter()
                .map(|row| row.get::<_, String>(0))
                .collect::<Vec<_>>();
            assert_eq!(
                column_names,
                vec!["id", "team_id", "title", "optional_note", "created_at"]
            );

            let rows = client
                .query(
                    "SELECT id, title, created_at FROM documents \
                     WHERE team_id = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3",
                    &[&"team-a", &2_i64, &1_i64],
                )
                .await
                .expect("filter and paginate documents");
            assert_eq!(rows.len(), 2);
            let titles = rows
                .iter()
                .map(|row| row.get::<_, String>(1))
                .collect::<Vec<_>>();
            assert_eq!(titles, vec!["document-3", "document-2"]);
            assert_eq!(rows[0].get::<_, i64>(2), 3);
            let _: uuid::Uuid = rows[0].get(0);

            let first_keyset_page = client
                .query(
                    "SELECT id, title, created_at FROM documents \
                     WHERE team_id = $1 \
                     ORDER BY created_at DESC, id DESC LIMIT 2",
                    &[&"team-a"],
                )
                .await
                .expect("read the first keyset page");
            let cursor_id = first_keyset_page[1].get::<_, uuid::Uuid>(0);
            let cursor_created_at = first_keyset_page[1].get::<_, i64>(2);
            let second_keyset_page = client
                .query(
                    "SELECT id, title, created_at FROM documents \
                     WHERE team_id = $1 AND \
                       (created_at < $2 OR (created_at = $2 AND id < $3)) \
                     ORDER BY created_at DESC, id DESC LIMIT 2",
                    &[&"team-a", &cursor_created_at, &cursor_id],
                )
                .await
                .expect("read the next keyset page");
            assert_eq!(
                second_keyset_page
                    .iter()
                    .map(|row| row.get::<_, String>(1))
                    .collect::<Vec<_>>(),
                vec!["document-2", "document-1"]
            );

            let reused_parameter_rows = client
                .query(
                    "SELECT title FROM documents \
                     WHERE title = $1 OR optional_note = $1 LIMIT 10",
                    &[&"document-3"],
                )
                .await
                .expect("one PostgreSQL parameter binds independently to nullable and non-null Jazz columns");
            assert_eq!(reused_parameter_rows.len(), 1);
            assert_eq!(reused_parameter_rows[0].get::<_, String>(0), "document-3");

            let non_null_is_null = client
                .query("SELECT id FROM documents WHERE title IS NULL LIMIT 10", &[])
                .await
                .expect("IS NULL on a non-null column is valid PostgreSQL");
            assert!(non_null_is_null.is_empty());
            let non_null_is_not_null = client
                .query(
                    "SELECT id FROM documents WHERE title IS NOT NULL LIMIT 10",
                    &[],
                )
                .await
                .expect("IS NOT NULL on a non-null column is valid PostgreSQL");
            assert_eq!(non_null_is_not_null.len(), 6);
            let nullable_is_null = client
                .query(
                    "SELECT id FROM documents WHERE optional_note IS NULL LIMIT 10",
                    &[],
                )
                .await
                .expect("IS NULL on a nullable column uses Jazz nullable semantics");
            assert_eq!(nullable_is_null.len(), 1);
            let nullable_is_not_null = client
                .query(
                    "SELECT id FROM documents WHERE optional_note IS NOT NULL LIMIT 10",
                    &[],
                )
                .await
                .expect("IS NOT NULL on a nullable column uses Jazz nullable semantics");
            assert_eq!(nullable_is_not_null.len(), 5);

            let session = client
                .simple_query("SELECT current_database(), version(); SELECT 2")
                .await
                .expect("simple query protocol supports safe statement batches");
            assert!(session.len() >= 4);

            let table_batch_error = tokio::time::timeout(
                Duration::from_secs(2),
                client.simple_query(
                    "SELECT id FROM documents LIMIT 1; SELECT id FROM documents LIMIT 1",
                ),
            )
            .await
            .expect("multiple table reads must fail without waiting on the query gate")
            .expect_err("one simple-query batch cannot retain two table responses");
            assert_eq!(
                table_batch_error.code().map(|code| code.code()),
                Some("0A000")
            );
            assert!(
                table_batch_error
                    .as_db_error()
                    .is_some_and(|error| error.message().contains("at most one application-table"))
            );

            let response_batch_error = client
                .simple_query("SELECT 1; SELECT 2; SELECT 3; SELECT 4; SELECT 5")
                .await
                .expect_err("one simple batch cannot retain more than four row responses");
            assert_eq!(
                response_batch_error.code().map(|code| code.code()),
                Some("54000")
            );

            let health = client
                .query_one("SELECT 1", &[])
                .await
                .expect("ordinary PostgreSQL health check works");
            assert_eq!(health.get::<_, i32>(0), 1);

            let oversized_sql = format!("SELECT 1{}", " ".repeat(70_000));
            let oversized_error = client
                .simple_query(&oversized_sql)
                .await
                .expect_err("oversized SQL must be rejected before parsing");
            assert_eq!(
                oversized_error.code().map(|code| code.code()),
                Some("54000")
            );

            let huge_placeholder_error = client
                .prepare("SELECT id FROM documents WHERE title = $1025 LIMIT 1")
                .await
                .expect_err("placeholder positions are bounded during parsing");
            assert_eq!(
                huge_placeholder_error.code().map(|code| code.code()),
                Some("0A000")
            );

            let repeated_wildcards = std::iter::repeat_n("*", 334)
                .collect::<Vec<_>>()
                .join(", ");
            let wide_result_error = client
                .prepare(&format!(
                    "SELECT {repeated_wildcards} FROM documents LIMIT 1"
                ))
                .await
                .expect_err("expanded wildcard projections must respect PostgreSQL's column cap");
            assert_eq!(
                wide_result_error.code().map(|code| code.code()),
                Some("54000")
            );

            let repeated_filter = std::iter::repeat_n("title = $1", 40)
                .collect::<Vec<_>>()
                .join(" OR ");
            let repeated_parameter_sql =
                format!("SELECT id FROM documents WHERE {repeated_filter} LIMIT 1");
            let repeated_parameter = "x".repeat(1024 * 1024);
            let repeated_parameter_rows = client
                .query(&repeated_parameter_sql, &[&repeated_parameter])
                .await
                .expect("repeated references reuse one converted Jazz binding");
            assert!(repeated_parameter_rows.is_empty());

            let oversized_parameter = "x".repeat(4 * 1024 * 1024 + 1);
            let oversized_parameter_error = client
                .query(
                    "SELECT id FROM documents WHERE title = $1 LIMIT 1",
                    &[&oversized_parameter],
                )
                .await
                .expect_err("decoded parameter bytes are bounded");
            assert_eq!(
                oversized_parameter_error.code().map(|code| code.code()),
                Some("54000")
            );

            let null_parameter: Option<&str> = None;
            let null_parameter_error = client
                .query(
                    "SELECT id FROM documents WHERE optional_note = $1 LIMIT 1",
                    &[&null_parameter],
                )
                .await
                .expect_err("NULL filter parameters require explicit IS NULL syntax");
            assert_eq!(
                null_parameter_error.code().map(|code| code.code()),
                Some("22004")
            );

            let declared_type_error = client
                .prepare_typed(
                    "SELECT title FROM documents WHERE team_id = $1 LIMIT 1",
                    &[Type::INT4],
                )
                .await
                .expect_err("declared parameter OIDs must match inferred column types");
            assert_eq!(
                declared_type_error.code().map(|code| code.code()),
                Some("42804")
            );

            let partially_typed = client
                .prepare_typed(
                    "SELECT title FROM documents WHERE team_id = $1 LIMIT $2",
                    &[Type::TEXT],
                )
                .await
                .expect("PostgreSQL permits a typed parameter prefix");
            let partially_typed_rows = client
                .query(&partially_typed, &[&"team-a", &1_i64])
                .await
                .expect("remaining parameter types are inferred");
            assert_eq!(partially_typed_rows.len(), 1);

            let null_comparison_rows = client
                .query(
                    "SELECT column_name FROM information_schema.columns \
                     WHERE column_default <> 'x'",
                    &[],
                )
                .await
                .expect("catalogue filters preserve SQL NULL semantics");
            assert!(null_comparison_rows.is_empty());

            let unsafe_delete_error = client
                .execute("DELETE FROM documents", &[])
                .await
                .expect_err("DELETE must identify exactly one row by id");
            assert_eq!(
                unsafe_delete_error.code().map(|code| code.code()),
                Some("0A000")
            );

            let unbounded_error = client
                .query("SELECT * FROM documents", &[])
                .await
                .expect_err("application-table reads must be explicitly bounded");
            assert_eq!(
                unbounded_error.code().map(|code| code.code()),
                Some("54000")
            );

            let deep_offset_error = client
                .query(
                    "SELECT id FROM documents LIMIT $1 OFFSET $2",
                    &[&1_i64, &10_001_i64],
                )
                .await
                .expect_err("deep offsets must use keyset pagination");
            assert_eq!(
                deep_offset_error.code().map(|code| code.code()),
                Some("54000")
            );

            let set_error = client
                .execute("SET statement_timeout = '1s'", &[])
                .await
                .expect_err("unsupported session settings must not report success");
            assert_eq!(set_error.code().map(|code| code.code()), Some("0A000"));

            let transaction_mode_error = client
                .simple_query("BEGIN READ WRITE")
                .await
                .expect_err("unsupported transaction modes must not report success");
            assert_eq!(
                transaction_mode_error.code().map(|code| code.code()),
                Some("0A000")
            );
            let transaction_chain_error = client
                .simple_query("COMMIT AND CHAIN")
                .await
                .expect_err("unsupported transaction chaining must not report success");
            assert_eq!(
                transaction_chain_error.code().map(|code| code.code()),
                Some("0A000")
            );
            let transaction_batch_error = client
                .simple_query("COMMIT; SELECT 1")
                .await
                .expect_err("transaction control must not be mixed into a simple-query batch");
            assert_eq!(
                transaction_batch_error.code().map(|code| code.code()),
                Some("0A000")
            );
            assert_eq!(
                client
                    .query_one("SELECT 1", &[])
                    .await
                    .expect("a rejected transaction batch leaves the session healthy")
                    .get::<_, i32>(0),
                1
            );

            client
                .batch_execute("BEGIN")
                .await
                .expect("begin explicit failed-transaction regression");
            let in_transaction_error = client
                .query("SELECT id FROM documents", &[])
                .await
                .expect_err("unbounded table read fails inside a transaction");
            assert_eq!(
                in_transaction_error.code().map(|code| code.code()),
                Some("54000")
            );
            let aborted_transaction_error = client
                .query_one("SELECT 1", &[])
                .await
                .expect_err("failed transaction rejects later reads");
            assert_eq!(
                aborted_transaction_error.code().map(|code| code.code()),
                Some("25P02")
            );
            client
                .batch_execute("ROLLBACK")
                .await
                .expect("rollback clears failed transaction state");
            let recovered_health = client
                .query_one("SELECT 1", &[])
                .await
                .expect("reads resume after rollback");
            assert_eq!(recovered_health.get::<_, i32>(0), 1);

            let transaction = client
                .transaction()
                .await
                .expect("start PostgreSQL read transaction");
            let portal_statement = transaction
                .prepare("SELECT id FROM documents LIMIT 2")
                .await
                .expect("prepare bounded portal query");
            let portal = transaction
                .bind(&portal_statement, &[])
                .await
                .expect("bind portal query");
            let portal_error = transaction
                .query_portal(&portal, 1)
                .await
                .expect_err("incremental portal fetch must not silently ignore max_rows");
            assert_eq!(portal_error.code().map(|code| code.code()), Some("0A000"));
            transaction
                .rollback()
                .await
                .expect("rollback read-only transaction");

            drop(client);
            connection_task.await.expect("join PostgreSQL connection");

            let (mut resource_client, resource_connection) =
                tokio_postgres::connect(&url, NoTls)
                    .await
                    .expect("connect for prepared-object resource checks");
            let resource_connection_task = tokio::spawn(async move {
                resource_connection
                    .await
                    .expect("resource-check connection remains healthy")
            });
            let mut retained_statements = Vec::new();
            for index in 0..64 {
                retained_statements.push(
                    resource_client
                        .prepare(&format!("SELECT {index}"))
                        .await
                        .expect("retain a bounded prepared statement"),
                );
            }
            let statement_limit_error = resource_client
                .prepare("SELECT 64")
                .await
                .expect_err("the 65th retained statement must be rejected");
            assert_eq!(
                statement_limit_error.code().map(|code| code.code()),
                Some("54000")
            );

            let resource_transaction = resource_client
                .transaction()
                .await
                .expect("start portal resource check");
            let mut retained_portals = Vec::new();
            for _ in 0..64 {
                retained_portals.push(
                    resource_transaction
                        .bind(&retained_statements[0], &[])
                        .await
                        .expect("retain a bounded portal"),
                );
            }
            let portal_limit_error = match resource_transaction
                .bind(&retained_statements[0], &[])
                .await
            {
                Ok(_) => panic!("the 65th retained portal must be rejected"),
                Err(error) => error,
            };
            assert_eq!(
                portal_limit_error.code().map(|code| code.code()),
                Some("54000")
            );
            drop(retained_portals);
            resource_transaction
                .rollback()
                .await
                .expect("rollback resource-check transaction");
            drop(retained_statements);
            let health = resource_client
                .query_one("SELECT 1", &[])
                .await
                .expect("connection recovers after releasing prepared objects");
            assert_eq!(health.get::<_, i32>(0), 1);
            drop(resource_client);
            resource_connection_task
                .await
                .expect("join resource-check connection");

            let wrong_url = url.replace(JazzServer::POSTGRES_SECRET, "wrong-secret");
            assert!(tokio_postgres::connect(&wrong_url, NoTls).await.is_err());
            let admin_url = url.replace(JazzServer::POSTGRES_SECRET, JazzServer::ADMIN_SECRET);
            assert!(tokio_postgres::connect(&admin_url, NoTls).await.is_err());
            let backend_url = url.replace(JazzServer::POSTGRES_SECRET, JazzServer::BACKEND_SECRET);
            assert!(tokio_postgres::connect(&backend_url, NoTls).await.is_err());

            raw_extended_protocol_lifecycle(
                server.postgres_port().expect("PostgreSQL port"),
                &server.app_id().to_string(),
                JazzServer::POSTGRES_SECRET,
            )
            .await;

            server.shutdown().await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_driver_mutates_globally_settled_rows_in_autocommit_mode() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = documents_schema();
            let server = JazzServer::builder()
                .with_schema(schema.clone())
                .with_postgres_port(0)
                .with_persistent_storage()
                .start()
                .await;
            let observer = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("00000000-0000-4000-8000-000000000077")
                .as_admin()
                .ready_on("documents", READY_TIMEOUT)
                .connect()
                .await;
            let mut subscription = observer
                .subscribe(QueryBuilder::new("documents").build())
                .await
                .expect("open an active Jazz subscription before PostgreSQL mutations");
            let mut subscription_log = Vec::new();

            let url = server.postgres_url().expect("PostgreSQL URL");
            let (mut client, connection) = tokio_postgres::connect(&url, NoTls)
                .await
                .expect("connect PostgreSQL mutation client");
            let connection_task = tokio::spawn(async move {
                connection
                    .await
                    .expect("PostgreSQL mutation connection remains healthy")
            });

            let read_only = client
                .query_one("SHOW default_transaction_read_only", &[])
                .await
                .expect("inspect advertised transaction mode")
                .get::<_, String>(0);
            assert_eq!(read_only, "off");

            let no_note: Option<&str> = None;
            let inserted = client
                .query_one(
                    "INSERT INTO documents \
                     (team_id, title, optional_note, created_at) \
                     VALUES ($1, $2, $3, $4) RETURNING id AS document_id",
                    &[&"team-pg", &"created-through-postgres", &no_note, &7_i64],
                )
                .await
                .expect("insert through PostgreSQL with a generated Jazz id");
            let document_id = inserted.get::<_, uuid::Uuid>(0);
            let document_object_id = ObjectId::from_uuid(document_id);

            let visible = client
                .query_one(
                    "SELECT title, optional_note, created_at FROM documents \
                     WHERE id = $1 LIMIT 1",
                    &[&document_id],
                )
                .await
                .expect("the committed insert is immediately globally readable");
            assert_eq!(visible.get::<_, String>(0), "created-through-postgres");
            assert_eq!(visible.get::<_, Option<String>>(1), None);
            assert_eq!(visible.get::<_, i64>(2), 7);

            wait_for_subscription_update(
                &mut subscription,
                &mut subscription_log,
                Duration::from_secs(5),
                "PostgreSQL insert to reach an active Jazz observer",
                |log| has_added(log, document_object_id),
            )
            .await;

            let updated = client
                .execute(
                    "UPDATE documents SET title = $1, optional_note = $2 WHERE id = $3",
                    &[
                        &"updated-through-postgres",
                        &Some("now-present"),
                        &document_id,
                    ],
                )
                .await
                .expect("update through PostgreSQL");
            assert_eq!(updated, 1);
            let updated_row = client
                .query_one(
                    "SELECT team_id, title, optional_note, created_at FROM documents \
                     WHERE id = $1 LIMIT 1",
                    &[&document_id],
                )
                .await
                .expect("read updated row");
            assert_eq!(updated_row.get::<_, String>(0), "team-pg");
            assert_eq!(updated_row.get::<_, String>(1), "updated-through-postgres");
            assert_eq!(
                updated_row.get::<_, Option<String>>(2).as_deref(),
                Some("now-present")
            );
            assert_eq!(updated_row.get::<_, i64>(3), 7);

            wait_for_subscription_update(
                &mut subscription,
                &mut subscription_log,
                Duration::from_secs(5),
                "PostgreSQL update to reach an active Jazz observer",
                |log| has_updated(log, document_object_id),
            )
            .await;

            let missing_id = uuid::Uuid::now_v7();
            assert_eq!(
                client
                    .execute(
                        "UPDATE documents SET title = $1 WHERE id = $2",
                        &[&"missing", &missing_id],
                    )
                    .await
                    .expect("updating a missing row is a successful no-op"),
                0
            );
            assert_eq!(
                client
                    .execute("DELETE FROM documents WHERE id = $1", &[&missing_id])
                    .await
                    .expect("deleting a missing row is a successful no-op"),
                0
            );

            let missing_required = client
                .execute("INSERT INTO documents (team_id) VALUES ($1)", &[&"team-pg"])
                .await
                .expect_err("missing required columns must be rejected before commit");
            assert_eq!(
                missing_required.code().map(|code| code.code()),
                Some("23502")
            );

            let duplicate = client
                .execute(
                    "INSERT INTO documents \
                     (id, team_id, title, optional_note, created_at) \
                     VALUES ($1, $2, $3, $4, $5)",
                    &[&document_id, &"team-pg", &"duplicate", &no_note, &8_i64],
                )
                .await
                .expect_err("an explicit duplicate Jazz id must not become an update");
            assert_eq!(duplicate.code().map(|code| code.code()), Some("23505"));

            let oversized_note = "x".repeat(3 * 1024 * 1024);
            let oversized = client
                .execute(
                    "UPDATE documents SET optional_note = $1 WHERE id = $2",
                    &[&oversized_note, &document_id],
                )
                .await
                .expect_err("an oversized commit must be rejected as a PostgreSQL limit error");
            assert_eq!(oversized.code().map(|code| code.code()), Some("54000"));
            let unchanged = client
                .query_one(
                    "SELECT optional_note FROM documents WHERE id = $1 LIMIT 1",
                    &[&document_id],
                )
                .await
                .expect("oversized rejected update leaves the row unchanged");
            assert_eq!(
                unchanged.get::<_, Option<String>>(0).as_deref(),
                Some("now-present")
            );

            let transaction = client
                .transaction()
                .await
                .expect("begin explicit PostgreSQL transaction");
            let transaction_id = uuid::Uuid::now_v7();
            assert_eq!(
                transaction
                    .execute(
                        "INSERT INTO documents \
                         (id, team_id, title, optional_note, created_at) \
                         VALUES ($1, $2, $3, $4, $5)",
                        &[
                            &transaction_id,
                            &"team-pg",
                            &"must-not-commit",
                            &no_note,
                            &9_i64,
                        ],
                    )
                    .await
                    .expect("stage DML inside BEGIN"),
                1
            );
            assert_eq!(
                transaction
                    .query(
                        "SELECT id FROM documents WHERE id = $1 LIMIT 1",
                        &[&transaction_id],
                    )
                    .await
                    .expect("the transaction reads its staged insert")
                    .len(),
                1
            );
            transaction
                .rollback()
                .await
                .expect("rollback explicit DML transaction");
            assert!(
                client
                    .query(
                        "SELECT id FROM documents WHERE id = $1 LIMIT 1",
                        &[&transaction_id],
                    )
                    .await
                    .expect("rolled-back staged insert remains absent")
                    .is_empty()
            );

            let mixed_id = uuid::Uuid::now_v7();
            let mixed_batch = client
                .simple_query(&format!(
                    "INSERT INTO documents (id, team_id, title, created_at) \
                     VALUES ('{mixed_id}', 'team-pg', 'must-not-partially-commit', 10); \
                     SELECT 1"
                ))
                .await
                .expect_err("mixed DML simple-query batches are rejected before execution");
            assert_eq!(mixed_batch.code().map(|code| code.code()), Some("0A000"));
            assert!(
                client
                    .query(
                        "SELECT id FROM documents WHERE id = $1 LIMIT 1",
                        &[&mixed_id],
                    )
                    .await
                    .expect("verify rejected batch did not commit")
                    .is_empty()
            );

            assert_eq!(
                client
                    .execute("DELETE FROM documents WHERE id = $1", &[&document_id])
                    .await
                    .expect("delete through PostgreSQL"),
                1
            );
            assert!(
                client
                    .query(
                        "SELECT id FROM documents WHERE id = $1 LIMIT 1",
                        &[&document_id],
                    )
                    .await
                    .expect("deleted row is no longer visible")
                    .is_empty()
            );

            wait_for_subscription_update(
                &mut subscription,
                &mut subscription_log,
                Duration::from_secs(5),
                "PostgreSQL delete to reach an active Jazz observer",
                |log| has_removed(log, document_object_id),
            )
            .await;

            drop(client);
            connection_task.await.expect("join mutation connection");
            observer.shutdown().await.expect("shutdown Jazz observer");
            server.shutdown().await;
        })
        .await;
}

/// Contract: PostgreSQL transactions stage multi-row inserts, predicate updates,
/// and exact-id deletes in one Jazz transaction with snapshot isolation,
/// read-your-writes, atomic subscription delivery, rollback, and durable commit.
///
/// Actors: alice is the PostgreSQL writer, bob is an independent PostgreSQL
/// reader, carol is an active Jazz subscriber, and dave is a raw PostgreSQL
/// client that disconnects with an open transaction.
///
/// ```text
/// alice --BEGIN/stage--> server tx overlay --read-own-writes--> alice
/// bob   ------read-----> committed snapshot (staged rows absent)
/// alice ----COMMIT-----> RocksDB --one add/update/remove delta--> carol
/// alice --ROLLBACK/error--> discard --no rows or delta---------> bob/carol
/// dave  --BEGIN/stage/disconnect--> abandon --no row or delta--> bob/carol
///                              |
///                              `--restart--> only committed state survives
/// ```
#[tokio::test(flavor = "current_thread")]
async fn postgres_transactions_are_atomic_and_read_their_own_writes() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let data_dir = tempfile::tempdir().expect("temporary transaction data dir");
            let app_id = jazz::tools::AppId::random();
            let schema = documents_schema();
            let server = JazzServer::builder()
                .with_app_id(app_id)
                .with_schema(schema.clone())
                .with_data_dir(data_dir.path())
                .with_postgres_port(0)
                .with_persistent_storage()
                .start()
                .await;
            let carol = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("00000000-0000-4000-8000-000000000076")
                .as_admin()
                .ready_on("documents", READY_TIMEOUT)
                .connect()
                .await;
            let mut carol_subscription = carol
                .subscribe(QueryBuilder::new("documents").build())
                .await
                .expect("open a Jazz subscription before transactional mutations");
            let mut carol_subscription_log = Vec::new();

            let url = server.postgres_url().expect("PostgreSQL URL");
            let (mut alice, alice_connection) = tokio_postgres::connect(&url, NoTls)
                .await
                .expect("connect alice as the transactional PostgreSQL writer");
            let alice_connection_task = tokio::spawn(async move {
                alice_connection
                    .await
                    .expect("alice's PostgreSQL connection remains healthy")
            });
            let (bob, bob_connection) = tokio_postgres::connect(&url, NoTls)
                .await
                .expect("connect bob as the independent PostgreSQL reader");
            let bob_connection_task = tokio::spawn(async move {
                bob_connection
                    .await
                    .expect("bob's PostgreSQL connection remains healthy")
            });

            let seeded = alice
                .query(
                    "INSERT INTO documents (team_id, title, optional_note, created_at) VALUES \
                     ($1, $2, $3, $4), ($1, $5, $6, $7), ($8, $9, $10, $11) \
                     RETURNING id",
                    &[
                        &"team-seed",
                        &"seed-one",
                        &Option::<&str>::None,
                        &11_i64,
                        &"seed-two",
                        &Option::<&str>::None,
                        &12_i64,
                        &"team-other",
                        &"other-team",
                        &Option::<&str>::None,
                        &13_i64,
                    ],
                )
                .await
                .expect("autocommit a multi-row INSERT with generated ids");
            assert_eq!(seeded.len(), 3);
            let seeded_ids = seeded
                .iter()
                .map(|row| row.get::<_, uuid::Uuid>(0))
                .collect::<Vec<_>>();
            assert_eq!(
                seeded_ids
                    .iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    .len(),
                3,
                "every VALUES row receives a distinct Jazz id"
            );
            wait_for_subscription_update(
                &mut carol_subscription,
                &mut carol_subscription_log,
                Duration::from_secs(5),
                "alice's seed insert to reach carol's Jazz subscription",
                |log| {
                    seeded_ids
                        .iter()
                        .all(|id| has_added(log, ObjectId::from_uuid(*id)))
                },
            )
            .await;
            carol_subscription_log.clear();

            let inserted_ids = [
                uuid::Uuid::parse_str("00000000-0000-4000-8000-0000000000d1")
                    .expect("first transaction UUID"),
                uuid::Uuid::parse_str("00000000-0000-4000-8000-0000000000d2")
                    .expect("second transaction UUID"),
                uuid::Uuid::parse_str("00000000-0000-4000-8000-0000000000d3")
                    .expect("third transaction UUID"),
            ];
            let alice_tx = alice
                .transaction()
                .await
                .expect("alice begins a write transaction");
            let returned = alice_tx
                .query(
                    "INSERT INTO documents \
                     (id, team_id, title, optional_note, created_at) VALUES \
                     ($1, $2, $3, $4, $5), \
                     ($6, $2, $7, $8, $9), \
                     ($10, $2, $11, $12, $13) \
                     RETURNING id AS document_id",
                    &[
                        &inserted_ids[0],
                        &"team-transaction",
                        &"transaction-one",
                        &Option::<&str>::None,
                        &21_i64,
                        &inserted_ids[1],
                        &"transaction-two",
                        &Some("has-note"),
                        &22_i64,
                        &inserted_ids[2],
                        &"transaction-three",
                        &Option::<&str>::None,
                        &23_i64,
                    ],
                )
                .await
                .expect("stage a multi-row INSERT in the transaction");
            assert_eq!(
                returned
                    .iter()
                    .map(|row| row.get::<_, uuid::Uuid>(0))
                    .collect::<Vec<_>>(),
                inserted_ids,
                "INSERT RETURNING preserves VALUES order"
            );
            assert_eq!(
                alice_tx
                    .execute(
                        "UPDATE documents SET title = $1, optional_note = $2 \
                         WHERE team_id = $3 AND created_at IN ($4, $5)",
                        &[
                            &"predicate-updated",
                            &Some("updated-note"),
                            &"team-seed",
                            &11_i64,
                            &12_i64,
                        ],
                    )
                    .await
                    .expect("stage a predicate UPDATE in the transaction"),
                2
            );
            assert_eq!(
                alice_tx
                    .execute("DELETE FROM documents WHERE id = $1", &[&seeded_ids[2]],)
                    .await
                    .expect("stage an exact-id DELETE in the transaction"),
                1
            );

            let own_inserted_rows = alice_tx
                .query(
                    "SELECT id FROM documents WHERE team_id = $1 \
                     ORDER BY created_at LIMIT 10",
                    &[&"team-transaction"],
                )
                .await
                .expect("transaction reads its staged inserts");
            assert_eq!(own_inserted_rows.len(), 3);
            let own_updated_rows = alice_tx
                .query(
                    "SELECT title, optional_note FROM documents \
                     WHERE team_id = $1 ORDER BY created_at LIMIT 10",
                    &[&"team-seed"],
                )
                .await
                .expect("transaction reads its staged predicate update");
            assert_eq!(own_updated_rows.len(), 2);
            assert!(own_updated_rows.iter().all(|row| {
                row.get::<_, String>(0) == "predicate-updated"
                    && row.get::<_, Option<String>>(1).as_deref() == Some("updated-note")
            }));
            assert!(
                alice_tx
                    .query(
                        "SELECT id FROM documents WHERE id = $1 LIMIT 1",
                        &[&seeded_ids[2]],
                    )
                    .await
                    .expect("transaction reads its staged delete")
                    .is_empty()
            );

            assert!(
                bob.query(
                    "SELECT id FROM documents WHERE team_id = $1 LIMIT 10",
                    &[&"team-transaction"],
                )
                .await
                .expect("another session reads the committed snapshot")
                .is_empty(),
                "another PostgreSQL session must not see uncommitted inserts"
            );
            let external_seed_rows = bob
                .query(
                    "SELECT title FROM documents WHERE team_id = $1 \
                     ORDER BY created_at LIMIT 10",
                    &[&"team-seed"],
                )
                .await
                .expect("another session does not see the staged update");
            assert_eq!(
                external_seed_rows
                    .iter()
                    .map(|row| row.get::<_, String>(0))
                    .collect::<Vec<_>>(),
                ["seed-one".to_owned(), "seed-two".to_owned()]
            );
            assert_eq!(
                bob.query(
                    "SELECT id FROM documents WHERE id = $1 LIMIT 1",
                    &[&seeded_ids[2]],
                )
                .await
                .expect("another session does not see the staged delete")
                .len(),
                1
            );
            collect_stream_deltas(
                &mut carol_subscription,
                &mut carol_subscription_log,
                Duration::from_millis(150),
            )
            .await;
            assert!(
                inserted_ids.iter().all(|id| {
                    !has_any_change(&carol_subscription_log, ObjectId::from_uuid(*id))
                })
            );
            assert!(
                seeded_ids.iter().all(|id| {
                    !has_any_change(&carol_subscription_log, ObjectId::from_uuid(*id))
                })
            );

            alice_tx
                .commit()
                .await
                .expect("atomically commit staged PostgreSQL mutations");

            let committed_rows = bob
                .query(
                    "SELECT id FROM documents WHERE team_id = $1 \
                     ORDER BY created_at LIMIT 10",
                    &[&"team-transaction"],
                )
                .await
                .expect("another session sees inserts after COMMIT");
            assert_eq!(committed_rows.len(), 3);
            let committed_updates = bob
                .query(
                    "SELECT title, optional_note FROM documents WHERE team_id = $1 \
                     ORDER BY created_at LIMIT 10",
                    &[&"team-seed"],
                )
                .await
                .expect("another session sees predicate updates after COMMIT");
            assert_eq!(committed_updates.len(), 2);
            assert!(committed_updates.iter().all(|row| {
                row.get::<_, String>(0) == "predicate-updated"
                    && row.get::<_, Option<String>>(1).as_deref() == Some("updated-note")
            }));
            assert!(
                bob.query(
                    "SELECT id FROM documents WHERE id = $1 LIMIT 1",
                    &[&seeded_ids[2]],
                )
                .await
                .expect("committed exact-id DELETE becomes visible")
                .is_empty()
            );

            wait_for_subscription_update(
                &mut carol_subscription,
                &mut carol_subscription_log,
                Duration::from_secs(5),
                "alice's commit to reach carol atomically",
                |log| {
                    log.iter().any(|delta| {
                        inserted_ids.iter().all(|id| {
                            delta
                                .added
                                .iter()
                                .any(|change| change.id == ObjectId::from_uuid(*id))
                        }) && seeded_ids[..2].iter().all(|id| {
                            delta
                                .updated
                                .iter()
                                .any(|change| change.id == ObjectId::from_uuid(*id))
                        }) && delta
                            .removed
                            .iter()
                            .any(|change| change.id == ObjectId::from_uuid(seeded_ids[2]))
                    })
                },
            )
            .await;

            let rollback_ids = [
                uuid::Uuid::parse_str("00000000-0000-4000-8000-0000000000e1")
                    .expect("first rollback UUID"),
                uuid::Uuid::parse_str("00000000-0000-4000-8000-0000000000e2")
                    .expect("second rollback UUID"),
            ];
            carol_subscription_log.clear();
            let alice_rollback_tx = alice
                .transaction()
                .await
                .expect("alice begins a transaction to roll back");
            assert_eq!(
                alice_rollback_tx
                    .execute(
                        "INSERT INTO documents \
                         (id, team_id, title, optional_note, created_at) VALUES \
                         ($1, $2, $3, $4, $5), ($6, $2, $7, $4, $8)",
                        &[
                            &rollback_ids[0],
                            &"team-rollback",
                            &"rollback-one",
                            &Option::<&str>::None,
                            &31_i64,
                            &rollback_ids[1],
                            &"rollback-two",
                            &32_i64,
                        ],
                    )
                    .await
                    .expect("stage rows that will be rolled back"),
                2
            );
            assert_eq!(
                alice_rollback_tx
                    .execute(
                        "UPDATE documents SET title = $1 WHERE team_id = $2",
                        &[&"must-roll-back", &"team-seed"],
                    )
                    .await
                    .expect("stage predicate update that will be rolled back"),
                2
            );
            assert_eq!(
                alice_rollback_tx
                    .query(
                        "SELECT id FROM documents WHERE team_id = $1 LIMIT 10",
                        &[&"team-rollback"],
                    )
                    .await
                    .expect("transaction reads rows before rollback")
                    .len(),
                2
            );
            alice_rollback_tx
                .rollback()
                .await
                .expect("discard all staged mutations");
            assert!(
                bob.query(
                    "SELECT id FROM documents WHERE team_id = $1 LIMIT 10",
                    &[&"team-rollback"],
                )
                .await
                .expect("rolled-back rows stay invisible")
                .is_empty()
            );
            let after_rollback = bob
                .query(
                    "SELECT title FROM documents WHERE team_id = $1 LIMIT 10",
                    &[&"team-seed"],
                )
                .await
                .expect("rolled-back predicate update stays invisible");
            assert_eq!(after_rollback.len(), 2);
            assert!(
                after_rollback
                    .iter()
                    .all(|row| { row.get::<_, String>(0) == "predicate-updated" })
            );

            let failed_id = uuid::Uuid::parse_str("00000000-0000-4000-8000-0000000000f1")
                .expect("failed transaction UUID");
            let alice_failed_tx = alice
                .transaction()
                .await
                .expect("alice begins a transaction that will fail");
            assert_eq!(
                alice_failed_tx
                    .execute(
                        "INSERT INTO documents \
                         (id, team_id, title, optional_note, created_at) \
                         VALUES ($1, $2, $3, $4, $5)",
                        &[
                            &failed_id,
                            &"team-failed",
                            &"must-not-commit",
                            &Option::<&str>::None,
                            &41_i64,
                        ],
                    )
                    .await
                    .expect("stage a row before a later statement fails"),
                1
            );
            let duplicate_error = alice_failed_tx
                .execute(
                    "INSERT INTO documents \
                     (id, team_id, title, optional_note, created_at) \
                     VALUES ($1, $2, $3, $4, $5)",
                    &[
                        &seeded_ids[0],
                        &"team-failed",
                        &"duplicate-id",
                        &Option::<&str>::None,
                        &42_i64,
                    ],
                )
                .await
                .expect_err("duplicate id aborts the explicit transaction");
            assert_eq!(
                duplicate_error.code().map(|code| code.code()),
                Some("23505")
            );
            let aborted_error = alice_failed_tx
                .query_one("SELECT 1", &[])
                .await
                .expect_err("an aborted transaction rejects later statements");
            assert_eq!(aborted_error.code().map(|code| code.code()), Some("25P02"));
            alice_failed_tx
                .commit()
                .await
                .expect("COMMIT on a failed transaction performs PostgreSQL rollback");
            assert!(
                bob.query(
                    "SELECT id FROM documents WHERE id = $1 LIMIT 1",
                    &[&failed_id],
                )
                .await
                .expect("failed transaction has no partial effects")
                .is_empty()
            );

            let atomic_ids = [
                uuid::Uuid::parse_str("00000000-0000-4000-8000-000000000101")
                    .expect("first atomic-statement UUID"),
                uuid::Uuid::parse_str("00000000-0000-4000-8000-000000000102")
                    .expect("second atomic-statement UUID"),
            ];
            let null_title: Option<&str> = None;
            let atomic_error = alice
                .execute(
                    "INSERT INTO documents \
                     (id, team_id, title, optional_note, created_at) VALUES \
                     ($1, $2, $3, $4, $5), ($6, $2, $7, $4, $8)",
                    &[
                        &atomic_ids[0],
                        &"team-atomic-error",
                        &Some("valid-row"),
                        &Option::<&str>::None,
                        &51_i64,
                        &atomic_ids[1],
                        &null_title,
                        &52_i64,
                    ],
                )
                .await
                .expect_err("one invalid VALUES row rejects the whole INSERT statement");
            assert_eq!(atomic_error.code().map(|code| code.code()), Some("23502"));
            assert!(
                bob.query(
                    "SELECT id FROM documents WHERE id IN ($1, $2) LIMIT 10",
                    &[&atomic_ids[0], &atomic_ids[1]],
                )
                .await
                .expect("failed multi-row INSERT leaves no partial rows")
                .is_empty()
            );

            collect_stream_deltas(
                &mut carol_subscription,
                &mut carol_subscription_log,
                Duration::from_millis(150),
            )
            .await;
            for id in rollback_ids
                .iter()
                .chain(std::iter::once(&failed_id))
                .chain(atomic_ids.iter())
            {
                assert!(
                    !has_any_change(&carol_subscription_log, ObjectId::from_uuid(*id)),
                    "discarded row {id} must not reach an active Jazz subscription"
                );
            }
            assert!(
                seeded_ids[..2].iter().all(|id| {
                    !has_any_change(&carol_subscription_log, ObjectId::from_uuid(*id))
                }),
                "a rolled-back predicate update must not reach an active Jazz subscription"
            );

            let disconnected_id = uuid::Uuid::parse_str("00000000-0000-4000-8000-000000000110")
                .expect("dave's disconnected transaction UUID");
            carol_subscription_log.clear();
            let database = server.app_id().to_string();
            let mut dave = raw_authenticated_postgres_socket(
                server.postgres_port().expect("PostgreSQL port for dave"),
                &database,
                JazzServer::POSTGRES_SECRET,
            )
            .await;
            let dave_begin = raw_simple_query(&mut dave, "BEGIN").await;
            assert_eq!(command_tags(&dave_begin), ["BEGIN"]);
            assert_eq!(ready_status(&dave_begin), Some(b'T'));
            let dave_insert = raw_simple_query(
                &mut dave,
                &format!(
                    "INSERT INTO documents (id, team_id, title, created_at) VALUES \
                     ('{disconnected_id}', 'team-disconnect', 'dave-staged', 61)"
                ),
            )
            .await;
            assert_eq!(command_tags(&dave_insert), ["INSERT 0 1"]);
            assert_eq!(ready_status(&dave_insert), Some(b'T'));

            // Close the transport without a PostgreSQL Terminate, COMMIT, or
            // ROLLBACK message. EOF confirms the server observed the disconnect.
            dave.shutdown()
                .await
                .expect("dave disconnects with an open transaction");
            let mut eof = [0_u8; 1];
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(3), dave.read(&mut eof))
                    .await
                    .expect("server closes dave's disconnected session")
                    .expect("read dave's disconnected session EOF"),
                0
            );
            assert!(
                bob.query(
                    "SELECT id FROM documents WHERE id = $1 LIMIT 1",
                    &[&disconnected_id],
                )
                .await
                .expect("bob cannot observe dave's abandoned insert")
                .is_empty()
            );
            collect_stream_deltas(
                &mut carol_subscription,
                &mut carol_subscription_log,
                Duration::from_millis(150),
            )
            .await;
            assert!(
                !has_any_change(
                    &carol_subscription_log,
                    ObjectId::from_uuid(disconnected_id)
                ),
                "carol must not receive dave's abandoned insert"
            );

            drop(bob);
            bob_connection_task
                .await
                .expect("join bob's PostgreSQL connection");
            drop(alice);
            alice_connection_task
                .await
                .expect("join alice's PostgreSQL connection");
            carol
                .shutdown()
                .await
                .expect("shutdown carol's Jazz client");
            server.shutdown().await;

            let restarted = JazzServer::builder()
                .with_app_id(app_id)
                .with_schema(schema)
                .with_data_dir(data_dir.path())
                .with_postgres_port(0)
                .with_persistent_storage()
                .start()
                .await;
            let restarted_url = restarted
                .postgres_url()
                .expect("PostgreSQL URL after transaction restart");
            let (restarted_client, restarted_connection) =
                tokio_postgres::connect(&restarted_url, NoTls)
                    .await
                    .expect("connect after transaction restart");
            let restarted_connection_task = tokio::spawn(async move {
                restarted_connection
                    .await
                    .expect("post-restart transaction connection remains healthy")
            });
            assert_eq!(
                restarted_client
                    .query(
                        "SELECT id FROM documents WHERE team_id = $1 LIMIT 10",
                        &[&"team-transaction"],
                    )
                    .await
                    .expect("committed transaction rows survive restart")
                    .len(),
                3
            );
            let restarted_updates = restarted_client
                .query(
                    "SELECT title, optional_note FROM documents WHERE team_id = $1 LIMIT 10",
                    &[&"team-seed"],
                )
                .await
                .expect("committed predicate update survives restart");
            assert_eq!(restarted_updates.len(), 2);
            assert!(restarted_updates.iter().all(|row| {
                row.get::<_, String>(0) == "predicate-updated"
                    && row.get::<_, Option<String>>(1).as_deref() == Some("updated-note")
            }));
            assert!(
                restarted_client
                    .query(
                        "SELECT id FROM documents WHERE id = $1 LIMIT 1",
                        &[&seeded_ids[2]],
                    )
                    .await
                    .expect("committed transaction delete survives restart")
                    .is_empty()
            );
            for team in [
                "team-rollback",
                "team-failed",
                "team-atomic-error",
                "team-disconnect",
            ] {
                assert!(
                    restarted_client
                        .query(
                            "SELECT id FROM documents WHERE team_id = $1 LIMIT 10",
                            &[&team],
                        )
                        .await
                        .expect("discarded transaction rows remain absent after restart")
                        .is_empty(),
                    "discarded rows for {team} must not reappear after restart"
                );
            }
            drop(restarted_client);
            restarted_connection_task
                .await
                .expect("join post-restart transaction connection");
            restarted.shutdown().await;
        })
        .await;
}

/// Contract: PostgreSQL exclusive transactions encode large Text and Blob
/// inserts as ordinary extent-backed values, expose exact logical bytes to
/// read-your-writes, let a partial update inherit untouched large columns, and
/// never mistake authored handle-shaped Blob bytes for a stored handle.
///
/// Actor: alice writes and reads the asset through PostgreSQL.
///
/// ```text
/// alice --BEGIN/INSERT--> tx overlay --SELECT exact bytes--> alice
/// alice ------COMMIT----> RocksDB
/// alice --BEGIN/UPDATE name only--> parent large values --SELECT--> alice
/// alice ------COMMIT/new BEGIN/restart----------------------> same bytes
/// alice --UPDATE handle-shaped Blob/SELECT/ROLLBACK--------> exact authored bytes
/// ```
#[tokio::test(flavor = "current_thread")]
async fn postgres_transactions_preserve_large_values_across_partial_updates_and_restart() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let data_dir = tempfile::tempdir().expect("temporary large-value data dir");
            let app_id = jazz::tools::AppId::random();
            let schema = postgres_large_values_schema();
            let asset_id =
                uuid::Uuid::parse_str("00000000-0000-4000-8000-0000000000b1").expect("asset UUID");
            let body = "transactional-large-text-".repeat(4_096);
            let data = (0..128 * 1024)
                .map(|index| ((index * 31 + 7) % 251) as u8)
                .collect::<Vec<_>>();

            let server = JazzServer::builder()
                .with_app_id(app_id)
                .with_schema(schema.clone())
                .with_data_dir(data_dir.path())
                .with_postgres_port(0)
                .with_persistent_storage()
                .start()
                .await;
            let url = server.postgres_url().expect("PostgreSQL URL");
            let (mut alice, alice_connection) = tokio_postgres::connect(&url, NoTls)
                .await
                .expect("connect alice for large-value writes");
            let alice_connection_task = tokio::spawn(async move {
                alice_connection
                    .await
                    .expect("alice's large-value connection remains healthy")
            });

            let insert = alice
                .transaction()
                .await
                .expect("begin large-value insert transaction");
            assert_eq!(
                insert
                    .execute(
                        "INSERT INTO assets (id, name, body, data) VALUES ($1, $2, $3, $4)",
                        &[&asset_id, &"original", &body, &data],
                    )
                    .await
                    .expect("stage large Text and Blob values"),
                1
            );
            let staged_insert = insert
                .query_one(
                    "SELECT name, body, data FROM assets WHERE id = $1 LIMIT 1",
                    &[&asset_id],
                )
                .await
                .expect("read staged large values before commit");
            assert_eq!(staged_insert.get::<_, String>(0), "original");
            assert_eq!(staged_insert.get::<_, String>(1), body);
            assert_eq!(staged_insert.get::<_, Vec<u8>>(2), data);
            insert
                .commit()
                .await
                .expect("commit extent-backed large-value insert");

            let update = alice
                .transaction()
                .await
                .expect("begin partial large-value row update");
            assert_eq!(
                update
                    .execute(
                        "UPDATE assets SET name = $1 WHERE id = $2",
                        &[&"renamed", &asset_id],
                    )
                    .await
                    .expect("stage ordinary-column-only update"),
                1
            );
            let staged_update = update
                .query_one(
                    "SELECT name, body, data FROM assets WHERE id = $1 LIMIT 1",
                    &[&asset_id],
                )
                .await
                .expect("read inherited large values before update commit");
            assert_eq!(staged_update.get::<_, String>(0), "renamed");
            assert_eq!(staged_update.get::<_, String>(1), body);
            assert_eq!(staged_update.get::<_, Vec<u8>>(2), data);
            update
                .commit()
                .await
                .expect("commit partial update without rewriting large values");

            // A fresh transaction sees the large values inherited through the
            // sparse name-only winner. Read the id first so this also exercises
            // the projection that must not resolve any large-value history.
            let inherited = alice
                .transaction()
                .await
                .expect("begin fresh transaction after sparse update");
            assert_eq!(
                inherited
                    .query("SELECT id FROM assets WHERE id = $1 LIMIT 1", &[&asset_id],)
                    .await
                    .expect("read only id without resolving large values")
                    .len(),
                1
            );
            let inherited_row = inherited
                .query_one(
                    "SELECT name, body, data FROM assets WHERE id = $1 LIMIT 1",
                    &[&asset_id],
                )
                .await
                .expect("read large values inherited through sparse winner");
            assert_eq!(inherited_row.get::<_, String>(0), "renamed");
            assert_eq!(inherited_row.get::<_, String>(1), body);
            assert_eq!(inherited_row.get::<_, Vec<u8>>(2), data);
            inherited
                .rollback()
                .await
                .expect("close inherited-value read transaction");

            let handle_shaped_data = handle_shaped_blob_payload(asset_id);
            let handle_shaped = alice
                .transaction()
                .await
                .expect("begin handle-shaped Blob transaction");
            assert_eq!(
                handle_shaped
                    .execute(
                        "UPDATE assets SET data = $1 WHERE id = $2",
                        &[&handle_shaped_data, &asset_id],
                    )
                    .await
                    .expect("stage a Blob whose bytes encode as a valid Jazz handle"),
                1
            );
            assert_eq!(
                handle_shaped
                    .query_one(
                        "SELECT data FROM assets WHERE id = $1 LIMIT 1",
                        &[&asset_id],
                    )
                    .await
                    .expect("read handle-shaped authored bytes without hydrating them")
                    .get::<_, Vec<u8>>(0),
                handle_shaped_data
            );
            handle_shaped
                .rollback()
                .await
                .expect("discard handle-shaped Blob update");

            let committed = alice
                .query_one(
                    "SELECT name, body, data FROM assets WHERE id = $1 LIMIT 1",
                    &[&asset_id],
                )
                .await
                .expect("read committed inherited large values");
            assert_eq!(committed.get::<_, String>(0), "renamed");
            assert_eq!(committed.get::<_, String>(1), body);
            assert_eq!(committed.get::<_, Vec<u8>>(2), data);

            drop(alice);
            alice_connection_task
                .await
                .expect("join alice's large-value connection");
            server.shutdown().await;

            let restarted = JazzServer::builder()
                .with_app_id(app_id)
                .with_schema(schema)
                .with_data_dir(data_dir.path())
                .with_postgres_port(0)
                .with_persistent_storage()
                .start()
                .await;
            let restarted_url = restarted
                .postgres_url()
                .expect("PostgreSQL URL after large-value restart");
            let (alice, restarted_connection) = tokio_postgres::connect(&restarted_url, NoTls)
                .await
                .expect("reconnect alice after large-value restart");
            let restarted_connection_task = tokio::spawn(async move {
                restarted_connection
                    .await
                    .expect("restarted large-value connection remains healthy")
            });
            let durable = alice
                .query_one(
                    "SELECT name, body, data FROM assets WHERE id = $1 LIMIT 1",
                    &[&asset_id],
                )
                .await
                .expect("read durable large values after restart");
            assert_eq!(durable.get::<_, String>(0), "renamed");
            assert_eq!(durable.get::<_, String>(1), body);
            assert_eq!(durable.get::<_, Vec<u8>>(2), data);

            drop(alice);
            restarted_connection_task
                .await
                .expect("join restarted large-value connection");
            restarted.shutdown().await;
        })
        .await;
}

/// Contract: PostgreSQL rejects a table containing nullable large-value
/// columns before any read or write can reach Jazz's non-null large-value
/// storage path.
///
/// Actor: alice probes the unsupported table, receives `0A000`, then reuses the
/// same connection successfully.
///
/// ```text
/// alice --SELECT/INSERT nullable large value--> 0A000
/// alice ----------------SELECT 1-----------> healthy connection
/// ```
#[tokio::test(flavor = "current_thread")]
async fn postgres_rejects_nullable_large_value_tables_without_writing() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let server = JazzServer::builder()
                .with_schema(postgres_nullable_large_values_schema())
                .with_postgres_port(0)
                .with_persistent_storage()
                .start()
                .await;
            let url = server.postgres_url().expect("PostgreSQL URL");
            let (alice, connection) = tokio_postgres::connect(&url, NoTls)
                .await
                .expect("connect nullable large-value probe");
            let connection_task = tokio::spawn(async move {
                connection
                    .await
                    .expect("nullable large-value probe connection remains healthy")
            });

            let read_error = alice
                .query("SELECT id FROM nullable_assets LIMIT 1", &[])
                .await
                .expect_err("nullable large-value table reads are rejected explicitly");
            assert_eq!(read_error.code().map(|code| code.code()), Some("0A000"));
            let insert_error = alice
                .execute(
                    "INSERT INTO nullable_assets (data) VALUES ($1)",
                    &[&Some(vec![1_u8, 2, 3])],
                )
                .await
                .expect_err("nullable large-value table writes are rejected explicitly");
            assert_eq!(insert_error.code().map(|code| code.code()), Some("0A000"));
            assert_eq!(
                alice
                    .query_one("SELECT 1", &[])
                    .await
                    .expect("connection remains usable after explicit rejection")
                    .get::<_, i32>(0),
                1
            );

            drop(alice);
            connection_task
                .await
                .expect("join nullable large-value probe connection");
            server.shutdown().await;
        })
        .await;
}

/// Contract: a serialization failure while committing an exclusive PostgreSQL
/// transaction closes that transaction and returns ReadyForQuery(I), for both
/// the simple and extended protocols, so the same connection is immediately
/// reusable.
///
/// Actors: alice and bob conflict through raw simple-query connections; carol
/// and dave repeat the conflict through prepared/extended execution.
///
/// ```text
/// alice/carol --BEGIN/update----> same snapshot
/// bob/dave   --BEGIN/update----> same snapshot
/// alice/carol --COMMIT---------> accepted
/// bob/dave   --COMMIT---------> 40001 + ReadyForQuery(I) --SELECT 1--> healthy
/// ```
#[tokio::test(flavor = "current_thread")]
async fn postgres_commit_conflicts_return_connections_to_idle() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let data_dir = tempfile::tempdir().expect("temporary conflict data dir");
            let app_id = jazz::tools::AppId::random();
            let server = JazzServer::builder()
                .with_app_id(app_id)
                .with_schema(documents_schema())
                .with_data_dir(data_dir.path())
                .with_postgres_port(0)
                .with_persistent_storage()
                .start()
                .await;
            let url = server.postgres_url().expect("PostgreSQL URL");
            let row_id = uuid::Uuid::parse_str("00000000-0000-4000-8000-0000000000c1")
                .expect("conflict row UUID");
            let (seed, seed_connection) = tokio_postgres::connect(&url, NoTls)
                .await
                .expect("connect conflict seed writer");
            let seed_connection_task = tokio::spawn(async move {
                seed_connection
                    .await
                    .expect("conflict seed connection remains healthy")
            });
            seed.execute(
                "INSERT INTO documents (id, team_id, title, created_at) \
                 VALUES ($1, $2, $3, $4)",
                &[&row_id, &"team-conflict", &"base", &1_i64],
            )
            .await
            .expect("seed the row used by both conflict canaries");

            let database = server.app_id().to_string();
            let port = server.postgres_port().expect("PostgreSQL conflict port");
            let mut alice =
                raw_authenticated_postgres_socket(port, &database, JazzServer::POSTGRES_SECRET)
                    .await;
            let mut bob =
                raw_authenticated_postgres_socket(port, &database, JazzServer::POSTGRES_SECRET)
                    .await;
            assert_eq!(
                ready_status(&raw_simple_query(&mut alice, "BEGIN").await),
                Some(b'T')
            );
            assert_eq!(
                ready_status(&raw_simple_query(&mut bob, "BEGIN").await),
                Some(b'T')
            );
            assert_eq!(
                command_tags(
                    &raw_simple_query(
                        &mut alice,
                        &format!("UPDATE documents SET title = 'alice' WHERE id = '{row_id}'"),
                    )
                    .await,
                ),
                ["UPDATE 1"]
            );
            assert_eq!(
                command_tags(
                    &raw_simple_query(
                        &mut bob,
                        &format!("UPDATE documents SET title = 'bob' WHERE id = '{row_id}'"),
                    )
                    .await,
                ),
                ["UPDATE 1"]
            );
            let alice_commit = raw_simple_query(&mut alice, "COMMIT").await;
            assert_eq!(command_tags(&alice_commit), ["COMMIT"]);
            assert_eq!(ready_status(&alice_commit), Some(b'I'));
            let bob_conflict = raw_simple_query(&mut bob, "COMMIT").await;
            assert_eq!(
                first_error_sqlstate(&bob_conflict).as_deref(),
                Some("40001")
            );
            assert_eq!(ready_status(&bob_conflict), Some(b'I'));
            let bob_health = raw_simple_query(&mut bob, "SELECT 1").await;
            assert_eq!(first_data_row_text(&bob_health), Some("1"));
            assert_eq!(ready_status(&bob_health), Some(b'I'));
            drop(alice);
            drop(bob);

            let (carol, carol_connection) = tokio_postgres::connect(&url, NoTls)
                .await
                .expect("connect extended-protocol winner");
            let carol_connection_task = tokio::spawn(async move {
                carol_connection
                    .await
                    .expect("extended-protocol winner remains healthy")
            });
            let (dave, dave_connection) = tokio_postgres::connect(&url, NoTls)
                .await
                .expect("connect extended-protocol loser");
            let dave_connection_task = tokio::spawn(async move {
                dave_connection
                    .await
                    .expect("extended-protocol loser remains healthy")
            });
            carol
                .execute("BEGIN", &[])
                .await
                .expect("begin extended winner transaction");
            dave.execute("BEGIN", &[])
                .await
                .expect("begin extended loser transaction");
            carol
                .execute(
                    "UPDATE documents SET title = $1 WHERE id = $2",
                    &[&"carol", &row_id],
                )
                .await
                .expect("stage extended winner update");
            dave.execute(
                "UPDATE documents SET title = $1 WHERE id = $2",
                &[&"dave", &row_id],
            )
            .await
            .expect("stage extended loser update");
            carol
                .execute("COMMIT", &[])
                .await
                .expect("commit extended winner");
            let dave_conflict = dave
                .execute("COMMIT", &[])
                .await
                .expect_err("second extended commit conflicts");
            assert_eq!(dave_conflict.code().map(|code| code.code()), Some("40001"));
            assert_eq!(
                dave.query_one("SELECT 1", &[])
                    .await
                    .expect("extended loser connection immediately returns to idle")
                    .get::<_, i32>(0),
                1
            );

            drop(dave);
            dave_connection_task
                .await
                .expect("join extended loser connection");
            drop(carol);
            carol_connection_task
                .await
                .expect("join extended winner connection");
            drop(seed);
            seed_connection_task
                .await
                .expect("join conflict seed connection");
            server.shutdown().await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn postgres_mutations_survive_persistent_server_restart() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let data_dir = tempfile::tempdir().expect("temporary PostgreSQL server data dir");
            let app_id = jazz::tools::AppId::random();
            let schema = documents_schema();
            let survivor_id = uuid::Uuid::parse_str("00000000-0000-4000-8000-0000000000a1")
                .expect("survivor UUID");
            let deleted_id = uuid::Uuid::parse_str("00000000-0000-4000-8000-0000000000a2")
                .expect("deleted UUID");

            {
                let server = JazzServer::builder()
                    .with_app_id(app_id)
                    .with_schema(schema.clone())
                    .with_data_dir(data_dir.path())
                    .with_postgres_port(0)
                    .with_persistent_storage()
                    .start()
                    .await;
                let url = server
                    .postgres_url()
                    .expect("PostgreSQL URL before restart");
                let (client, connection) = tokio_postgres::connect(&url, NoTls)
                    .await
                    .expect("connect PostgreSQL client before restart");
                let connection_task = tokio::spawn(async move {
                    connection
                        .await
                        .expect("pre-restart PostgreSQL connection remains healthy")
                });

                let survivor_note = Some("must-survive-untouched");
                assert_eq!(
                    client
                        .execute(
                            "INSERT INTO documents \
                             (id, team_id, title, optional_note, created_at) \
                             VALUES ($1, $2, $3, $4, $5)",
                            &[
                                &survivor_id,
                                &"team-stable",
                                &"before-update",
                                &survivor_note,
                                &101_i64,
                            ],
                        )
                        .await
                        .expect("insert survivor before restart"),
                    1
                );
                assert_eq!(
                    client
                        .execute(
                            "INSERT INTO documents \
                             (id, team_id, title, optional_note, created_at) \
                             VALUES ($1, $2, $3, $4, $5)",
                            &[
                                &deleted_id,
                                &"team-delete",
                                &"delete-before-restart",
                                &Option::<&str>::None,
                                &202_i64,
                            ],
                        )
                        .await
                        .expect("insert row to delete before restart"),
                    1
                );
                assert_eq!(
                    client
                        .execute(
                            "UPDATE documents SET title = $1 WHERE id = $2",
                            &[&"after-update", &survivor_id],
                        )
                        .await
                        .expect("update survivor before restart"),
                    1
                );
                assert_eq!(
                    client
                        .execute("DELETE FROM documents WHERE id = $1", &[&deleted_id])
                        .await
                        .expect("delete row before restart"),
                    1
                );

                drop(client);
                connection_task
                    .await
                    .expect("join pre-restart PostgreSQL connection");
                server.shutdown().await;
            }

            let restarted = JazzServer::builder()
                .with_app_id(app_id)
                .with_schema(schema)
                .with_data_dir(data_dir.path())
                .with_postgres_port(0)
                .with_persistent_storage()
                .start()
                .await;
            let url = restarted
                .postgres_url()
                .expect("PostgreSQL URL after restart");
            let (client, connection) = tokio_postgres::connect(&url, NoTls)
                .await
                .expect("connect PostgreSQL client after restart");
            let connection_task = tokio::spawn(async move {
                connection
                    .await
                    .expect("post-restart PostgreSQL connection remains healthy")
            });

            let survivor = client
                .query_one(
                    "SELECT team_id, title, optional_note, created_at FROM documents \
                     WHERE id = $1 LIMIT 1",
                    &[&survivor_id],
                )
                .await
                .expect("survivor remains readable after restart");
            assert_eq!(survivor.get::<_, String>(0), "team-stable");
            assert_eq!(survivor.get::<_, String>(1), "after-update");
            assert_eq!(
                survivor.get::<_, Option<String>>(2).as_deref(),
                Some("must-survive-untouched")
            );
            assert_eq!(survivor.get::<_, i64>(3), 101);

            assert!(
                client
                    .query(
                        "SELECT id FROM documents WHERE id = $1 LIMIT 1",
                        &[&deleted_id],
                    )
                    .await
                    .expect("query deleted row after restart")
                    .is_empty(),
                "deleted row must not reappear after persistent restart"
            );

            drop(client);
            connection_task
                .await
                .expect("join post-restart PostgreSQL connection");
            restarted.shutdown().await;
        })
        .await;
}

#[tokio::test]
async fn product_server_cli_exposes_the_postgres_connection_url() {
    let temp = tempfile::tempdir().expect("temporary product server directory");
    let http_port_file = temp.path().join("http-port");
    let postgres_port_file = temp.path().join("postgres-port");
    let data_dir = temp.path().join("data");
    let child = std::process::Command::new(env!("CARGO_BIN_EXE_jazz-tools"))
        .args([
            "server",
            PRODUCT_APP_ID,
            "--port",
            "0",
            "--data-dir",
            data_dir.to_str().expect("UTF-8 data path"),
            "--admin-secret",
            "product-test-secret",
            "--postgres-secret",
            "product-postgres-secret",
            "--postgres-port",
            "0",
            "--bound-port-file",
            http_port_file.to_str().expect("UTF-8 HTTP port path"),
            "--postgres-bound-port-file",
            postgres_port_file
                .to_str()
                .expect("UTF-8 PostgreSQL port path"),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("start product jazz-tools server");
    let mut child = ChildGuard(Some(child));

    let http_port = wait_for_port_file(&http_port_file).await;
    let postgres_port = wait_for_port_file(&postgres_port_file).await;
    let url = format!(
        "postgresql://jazz:product-postgres-secret@127.0.0.1:{postgres_port}/{PRODUCT_APP_ID}?sslmode=disable"
    );
    let (client, connection) = tokio_postgres::connect(&url, NoTls)
        .await
        .expect("connect to product server PostgreSQL listener");
    let connection_task = tokio::spawn(async move {
        let _ = connection.await;
    });
    let row = client
        .query_one("SELECT current_database(), current_user", &[])
        .await
        .expect("query product server through extended protocol");
    assert_eq!(row.get::<_, String>(0), PRODUCT_APP_ID);
    assert_eq!(row.get::<_, String>(1), "jazz");

    let databases = client
        .query("SELECT datname FROM pg_catalog.pg_database", &[])
        .await
        .expect("list the app before its first schema is published");
    assert_eq!(databases[0].get::<_, String>(0), PRODUCT_APP_ID);
    let tables = client
        .query("SELECT table_name FROM information_schema.tables", &[])
        .await
        .expect("a fresh app has an empty table catalogue");
    assert!(tables.is_empty());

    let deep_filter = std::iter::repeat_n("title = 'x'", 3_500)
        .collect::<Vec<_>>()
        .join(" OR ");
    let deep_sql = format!("SELECT id FROM documents WHERE {deep_filter} LIMIT 1");
    assert!(deep_sql.len() < 64 * 1024);
    let deep_error = client
        .simple_query(&deep_sql)
        .await
        .expect_err("pathologically complex SQL must be rejected before building a deep AST");
    assert_eq!(deep_error.code().map(|code| code.code()), Some("0A000"));
    assert_eq!(
        client
            .query_one("SELECT 1", &[])
            .await
            .expect("the product server remains healthy after rejecting pathological SQL")
            .get::<_, i32>(0),
        1
    );

    let response = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{http_port}/internal/shutdown"))
        .header("X-Jazz-Admin-Secret", "product-test-secret")
        .send()
        .await
        .expect("request controlled product shutdown");
    assert_eq!(response.status(), reqwest::StatusCode::ACCEPTED);
    drop(client);
    connection_task.await.expect("join product connection task");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(status) = child
            .0
            .as_mut()
            .expect("child")
            .try_wait()
            .expect("poll child")
        {
            assert!(status.success(), "product server exited with {status}");
            child.0.take();
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "product server did not shut down"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_port_file(path: &Path) -> u16 {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if let Ok(value) = std::fs::read_to_string(path)
            && let Ok(port) = value.parse::<u16>()
        {
            return port;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "server did not write {}",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

struct ChildGuard(Option<std::process::Child>);

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(child) = self.0.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

async fn raw_extended_protocol_lifecycle(port: u16, database: &str, password: &str) {
    let mut socket = raw_authenticated_postgres_socket(port, database, password).await;
    raw_extended_protocol_lifecycle_body(&mut socket).await;
}

async fn raw_authenticated_postgres_socket(port: u16, database: &str, password: &str) -> TcpStream {
    let mut socket = TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect raw PostgreSQL client");
    let mut startup = Vec::new();
    startup.extend_from_slice(&196_608_u32.to_be_bytes());
    push_c_string(&mut startup, "user");
    push_c_string(&mut startup, "jazz");
    push_c_string(&mut startup, "database");
    push_c_string(&mut startup, database);
    startup.push(0);
    socket
        .write_all(&((startup.len() + 4) as u32).to_be_bytes())
        .await
        .expect("write PostgreSQL startup length");
    socket
        .write_all(&startup)
        .await
        .expect("write PostgreSQL startup body");

    loop {
        let (tag, payload) = read_backend_frame(&mut socket).await;
        match tag {
            b'R' if payload.get(..4) == Some(3_u32.to_be_bytes().as_slice()) => {
                let mut body = Vec::new();
                push_c_string(&mut body, password);
                write_frontend_frame(&mut socket, b'p', &body).await;
            }
            b'E' => panic!("raw PostgreSQL authentication failed: {payload:?}"),
            b'Z' => break,
            _ => {}
        }
    }

    socket
}

async fn raw_extended_protocol_lifecycle_body(mut socket: &mut TcpStream) {
    let returning_sql = "INSERT INTO documents (id, team_id, title, created_at) \
                         VALUES ('00000000-0000-4000-8000-0000000000b1', \
                         'team-wire', 'returning-wire-one', 301), \
                         ('00000000-0000-4000-8000-0000000000b2', \
                         'team-wire', 'returning-wire-two', 302) RETURNING id";
    let mut returning_query = Vec::new();
    push_c_string(&mut returning_query, returning_sql);
    write_frontend_frame(&mut socket, b'Q', &returning_query).await;
    let returning_messages = read_until_ready(&mut socket).await;
    assert_eq!(
        command_tags(&returning_messages),
        ["INSERT 0 2"],
        "INSERT RETURNING must emit a valid PostgreSQL command tag"
    );
    assert_eq!(
        returning_messages
            .iter()
            .filter(|(tag, _)| *tag == b'D')
            .count(),
        2,
        "multi-row INSERT RETURNING emits one DataRow per VALUES row"
    );

    let mutation_sql = "INSERT INTO documents (id, team_id, title, created_at) \
                        VALUES ('00000000-0000-4000-8000-0000000000b3', \
                        'team-wire', 'execute-once-wire-one', 303), \
                        ('00000000-0000-4000-8000-0000000000b4', \
                        'team-wire', 'execute-once-wire-two', 304)";
    let mut mutation_parse = Vec::new();
    push_c_string(&mut mutation_parse, "mutation_once");
    push_c_string(&mut mutation_parse, mutation_sql);
    mutation_parse.extend_from_slice(&0_u16.to_be_bytes());
    write_frontend_frame(&mut socket, b'P', &mutation_parse).await;
    let mut mutation_bind = Vec::new();
    push_c_string(&mut mutation_bind, "mutation_once_portal");
    push_c_string(&mut mutation_bind, "mutation_once");
    mutation_bind.extend_from_slice(&0_u16.to_be_bytes());
    mutation_bind.extend_from_slice(&0_u16.to_be_bytes());
    mutation_bind.extend_from_slice(&0_u16.to_be_bytes());
    write_frontend_frame(&mut socket, b'B', &mutation_bind).await;
    let mut mutation_execute = Vec::new();
    push_c_string(&mut mutation_execute, "mutation_once_portal");
    mutation_execute.extend_from_slice(&0_u32.to_be_bytes());
    write_frontend_frame(&mut socket, b'E', &mutation_execute).await;
    write_frontend_frame(&mut socket, b'E', &mutation_execute).await;
    write_frontend_frame(&mut socket, b'S', &[]).await;
    let mutation_messages = read_until_ready(&mut socket).await;
    assert_eq!(command_tags(&mutation_messages), ["INSERT 0 2"]);
    assert_eq!(
        first_error_sqlstate(&mutation_messages).as_deref(),
        Some("26000"),
        "a completed mutation portal must not execute its DML twice"
    );

    let begun = raw_simple_query(&mut socket, "BEGIN").await;
    assert_eq!(command_tags(&begun), ["BEGIN"]);
    assert_eq!(ready_status(&begun), Some(b'T'));
    let staged = raw_simple_query(
        &mut socket,
        "INSERT INTO documents (id, team_id, title, created_at) VALUES \
         ('00000000-0000-4000-8000-0000000000c1', 'team-wire-failed', 'one', 401), \
         ('00000000-0000-4000-8000-0000000000c2', 'team-wire-failed', 'two', 402)",
    )
    .await;
    assert_eq!(command_tags(&staged), ["INSERT 0 2"]);
    assert_eq!(ready_status(&staged), Some(b'T'));
    let failed = raw_simple_query(
        &mut socket,
        "INSERT INTO documents (id, team_id, title, created_at) VALUES \
         ('00000000-0000-4000-8000-0000000000c1', 'team-wire-failed', 'duplicate', 403)",
    )
    .await;
    assert_eq!(first_error_sqlstate(&failed).as_deref(), Some("23505"));
    assert_eq!(ready_status(&failed), Some(b'E'));
    let rolled_back = raw_simple_query(&mut socket, "COMMIT").await;
    assert_eq!(
        command_tags(&rolled_back),
        ["ROLLBACK"],
        "COMMIT in an aborted transaction reports PostgreSQL rollback"
    );
    assert_eq!(ready_status(&rolled_back), Some(b'I'));
    let discarded = raw_simple_query(
        &mut socket,
        "SELECT id FROM documents WHERE team_id = 'team-wire-failed' LIMIT 10",
    )
    .await;
    assert_eq!(
        discarded.iter().filter(|(tag, _)| *tag == b'D').count(),
        0,
        "a failed transaction does not partially publish staged rows"
    );

    let mut parse = Vec::new();
    push_c_string(&mut parse, "");
    push_c_string(&mut parse, "SELECT 1");
    parse.extend_from_slice(&0_u16.to_be_bytes());
    write_frontend_frame(&mut socket, b'P', &parse).await;
    let mut bind = Vec::new();
    push_c_string(&mut bind, "");
    push_c_string(&mut bind, "");
    bind.extend_from_slice(&0_u16.to_be_bytes());
    bind.extend_from_slice(&0_u16.to_be_bytes());
    bind.extend_from_slice(&0_u16.to_be_bytes());
    write_frontend_frame(&mut socket, b'B', &bind).await;
    let mut execute = Vec::new();
    push_c_string(&mut execute, "");
    execute.extend_from_slice(&0_u32.to_be_bytes());
    write_frontend_frame(&mut socket, b'E', &execute).await;
    write_frontend_frame(&mut socket, b'S', &[]).await;
    let executed = read_until_ready(&mut socket).await;
    assert!(executed.iter().any(|(tag, _)| *tag == b'1'));
    assert!(executed.iter().any(|(tag, _)| *tag == b'2'));
    assert_eq!(first_data_row_text(&executed), Some("1"));

    write_frontend_frame(&mut socket, b'E', &execute).await;
    write_frontend_frame(&mut socket, b'S', &[]).await;
    assert_eq!(
        first_error_sqlstate(&read_until_ready(&mut socket).await).as_deref(),
        Some("26000")
    );

    let mut named_parse = Vec::new();
    push_c_string(&mut named_parse, "s");
    push_c_string(&mut named_parse, "SELECT 2");
    named_parse.extend_from_slice(&0_u16.to_be_bytes());
    write_frontend_frame(&mut socket, b'P', &named_parse).await;
    write_frontend_frame(&mut socket, b'S', &[]).await;
    assert!(
        read_until_ready(&mut socket)
            .await
            .iter()
            .any(|(tag, _)| *tag == b'1')
    );

    write_frontend_frame(&mut socket, b'P', &named_parse).await;
    write_frontend_frame(&mut socket, b'S', &[]).await;
    assert_eq!(
        first_error_sqlstate(&read_until_ready(&mut socket).await).as_deref(),
        Some("42P05")
    );

    let mut named_bind = Vec::new();
    push_c_string(&mut named_bind, "p");
    push_c_string(&mut named_bind, "s");
    named_bind.extend_from_slice(&0_u16.to_be_bytes());
    named_bind.extend_from_slice(&0_u16.to_be_bytes());
    named_bind.extend_from_slice(&0_u16.to_be_bytes());
    write_frontend_frame(&mut socket, b'B', &named_bind).await;
    write_frontend_frame(&mut socket, b'B', &named_bind).await;
    write_frontend_frame(&mut socket, b'S', &[]).await;
    assert_eq!(
        first_error_sqlstate(&read_until_ready(&mut socket).await).as_deref(),
        Some("42P03")
    );

    let mut begin = Vec::new();
    push_c_string(&mut begin, "BEGIN");
    write_frontend_frame(&mut socket, b'Q', &begin).await;
    assert!(
        read_until_ready(&mut socket)
            .await
            .iter()
            .any(|(tag, _)| *tag == b'C')
    );

    write_frontend_frame(&mut socket, b'B', &named_bind).await;
    write_frontend_frame(&mut socket, b'S', &[]).await;
    assert!(
        read_until_ready(&mut socket)
            .await
            .iter()
            .any(|(tag, _)| *tag == b'2')
    );

    let mut named_execute = Vec::new();
    push_c_string(&mut named_execute, "p");
    named_execute.extend_from_slice(&0_u32.to_be_bytes());
    write_frontend_frame(&mut socket, b'E', &named_execute).await;
    write_frontend_frame(&mut socket, b'S', &[]).await;
    assert_eq!(
        first_data_row_text(&read_until_ready(&mut socket).await),
        Some("2")
    );

    let mut close_statement = vec![b'S'];
    push_c_string(&mut close_statement, "s");
    write_frontend_frame(&mut socket, b'C', &close_statement).await;
    write_frontend_frame(&mut socket, b'S', &[]).await;
    assert!(
        read_until_ready(&mut socket)
            .await
            .iter()
            .any(|(tag, _)| *tag == b'3')
    );

    write_frontend_frame(&mut socket, b'E', &named_execute).await;
    write_frontend_frame(&mut socket, b'S', &[]).await;
    assert_eq!(
        first_error_sqlstate(&read_until_ready(&mut socket).await).as_deref(),
        Some("26000")
    );

    let mut rollback = Vec::new();
    push_c_string(&mut rollback, "ROLLBACK");
    write_frontend_frame(&mut socket, b'Q', &rollback).await;
    assert!(
        read_until_ready(&mut socket)
            .await
            .iter()
            .any(|(tag, _)| *tag == b'C')
    );
}

async fn raw_simple_query(socket: &mut TcpStream, sql: &str) -> Vec<(u8, Vec<u8>)> {
    let mut query = Vec::new();
    push_c_string(&mut query, sql);
    write_frontend_frame(socket, b'Q', &query).await;
    read_until_ready(socket).await
}

fn push_c_string(target: &mut Vec<u8>, value: &str) {
    target.extend_from_slice(value.as_bytes());
    target.push(0);
}

async fn write_frontend_frame(socket: &mut TcpStream, tag: u8, body: &[u8]) {
    let mut frame = Vec::with_capacity(body.len() + 5);
    frame.push(tag);
    frame.extend_from_slice(&((body.len() + 4) as u32).to_be_bytes());
    frame.extend_from_slice(body);
    socket
        .write_all(&frame)
        .await
        .expect("write raw PostgreSQL frontend frame");
}

async fn read_backend_frame(socket: &mut TcpStream) -> (u8, Vec<u8>) {
    let tag = socket.read_u8().await.expect("read PostgreSQL backend tag");
    let length = socket
        .read_u32()
        .await
        .expect("read PostgreSQL backend length") as usize;
    assert!((4..=16 * 1024 * 1024 + 1024).contains(&length));
    let mut payload = vec![0_u8; length - 4];
    socket
        .read_exact(&mut payload)
        .await
        .expect("read PostgreSQL backend payload");
    (tag, payload)
}

async fn read_until_ready(socket: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut messages = Vec::new();
        loop {
            let message = read_backend_frame(socket).await;
            let ready = message.0 == b'Z';
            messages.push(message);
            if ready {
                return messages;
            }
        }
    })
    .await
    .expect("PostgreSQL backend reaches ReadyForQuery")
}

fn first_data_row_text(messages: &[(u8, Vec<u8>)]) -> Option<&str> {
    let payload = messages.iter().find(|(tag, _)| *tag == b'D')?.1.as_slice();
    if payload.get(..2)? != 1_u16.to_be_bytes().as_slice() {
        return None;
    }
    let length = i32::from_be_bytes(payload.get(2..6)?.try_into().ok()?);
    let length = usize::try_from(length).ok()?;
    std::str::from_utf8(payload.get(6..6 + length)?).ok()
}

fn command_tags(messages: &[(u8, Vec<u8>)]) -> Vec<&str> {
    messages
        .iter()
        .filter(|(tag, _)| *tag == b'C')
        .map(|(_, payload)| {
            let end = payload
                .iter()
                .position(|byte| *byte == 0)
                .expect("PostgreSQL command tag is NUL terminated");
            std::str::from_utf8(&payload[..end]).expect("PostgreSQL command tag is UTF-8")
        })
        .collect()
}

fn ready_status(messages: &[(u8, Vec<u8>)]) -> Option<u8> {
    messages
        .iter()
        .find(|(tag, _)| *tag == b'Z')
        .and_then(|(_, payload)| payload.first().copied())
}

fn first_error_sqlstate(messages: &[(u8, Vec<u8>)]) -> Option<String> {
    let payload = messages.iter().find(|(tag, _)| *tag == b'E')?.1.as_slice();
    let mut cursor = 0;
    while cursor < payload.len() && payload[cursor] != 0 {
        let field = payload[cursor];
        cursor += 1;
        let end = payload[cursor..].iter().position(|byte| *byte == 0)? + cursor;
        if field == b'C' {
            return std::str::from_utf8(&payload[cursor..end])
                .ok()
                .map(str::to_owned);
        }
        cursor = end + 1;
    }
    None
}
