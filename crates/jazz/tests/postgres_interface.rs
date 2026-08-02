#![cfg(feature = "test")]

mod support;

use std::time::Duration;
use std::{path::Path, process::Stdio};

use jazz::row_input;
use jazz::tools::server::JazzServer;
use jazz::tools::{ColumnType, DurabilityTier, Schema, SchemaBuilder, TableSchema, Value};
use support::TestingClient;
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

            let write_error = client
                .execute("DELETE FROM documents", &[])
                .await
                .expect_err("the PostgreSQL interface is read-only");
            assert_eq!(write_error.code().map(|code| code.code()), Some("0A000"));

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
                .expect("start read-only PostgreSQL transaction");
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
