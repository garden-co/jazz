//! Physical channel progress must not depend on a suspended semantic query.
use jazz::query::Query;
use jazz::tools::test_support::{AllowAll, ordinary_rows};
use jazz::tools::{ColumnType, ReadTier, SchemaBuilder, TableSchema, Value};
use jazz_testkit::{connect_ready_client, wait_for_edge_txs};
use std::time::Duration;

#[tokio::test(flavor = "current_thread")]
async fn fresh_native_client_reads_indirect_text_and_json() {
    tokio::task::LocalSet::new().run_until(async {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("documents")
                .column("label", ColumnType::Text)
                .column("text", ColumnType::Text)
                .column("json", ColumnType::Json { schema: None }))
            .table(TableSchema::builder("ready").column("label", ColumnType::Text))
            .allow_all().build();
        let server = jazz_server::JazzServer::start_with_schema(schema.clone()).await.unwrap();
        let writer = connect_ready_client(&server, &schema, "pump-writer", "ready", Duration::from_secs(30)).await;
        for size in [65_536, 65_537, 4 * 1024 * 1024 + 65_537] {
            // A deterministic nonuniform payload also exercises transport compression.
            let text: String = (0..size).map(|i| char::from(b'a' + ((i * 17 + i / 97) % 26) as u8)).collect();
            let json = format!("{{\"text\":\"{text}\"}}");
            let label = size.to_string();
            let (_, _, tx) = writer.insert("documents", jazz::row_input!("label" => label.clone(), "text" => text.clone(), "json" => json.clone())).unwrap();
            wait_for_edge_txs(&writer, &[tx.unwrap()]).await;
            // Ready against a separate empty table, so no document is prefetched.
            let reader = connect_ready_client(&server, &schema, &format!("pump-reader-{size}"), "ready", Duration::from_secs(30)).await;
            // Poll remote demand once, then cancel it before running another query.
            // This deterministically exercises cancellation without a timing race.
            {
                use std::future::Future;
                let mut cancelled = Box::pin(reader.query(Query::from("documents"), ReadTier::Remote));
                std::future::poll_fn(|cx| {
                    assert!(cancelled.as_mut().poll(cx).is_pending(), "fresh remote query must await admission");
                    std::task::Poll::Ready(())
                }).await;
            }
            let rows = tokio::time::timeout(Duration::from_secs(30), reader.query(Query::from("documents").select(["label"]), ReadTier::Remote)).await.expect("small projection progresses").unwrap();
            assert!(ordinary_rows(rows).iter().any(|(_, row)| row == &[Value::Text(label.clone())]));
            let rows = tokio::time::timeout(Duration::from_secs(30), reader.query(Query::from("documents").select(["label", "text", "json"]), ReadTier::Remote)).await.expect("remote indirect read progresses beyond channel credit window").unwrap();
            let rows = ordinary_rows(rows);
            let row = rows.iter().find(|(_, row)| row[0] == Value::Text(label.clone())).expect("inserted document");
            assert_eq!(row.1[1], Value::Text(text));
            assert_eq!(row.1[2], Value::Text(json));
            reader.shutdown().await.unwrap();
        }
        writer.shutdown().await.unwrap();
        server.shutdown().await;
    }).await;
}
