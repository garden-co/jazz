#![cfg(feature = "test-utils")]

use jazz::row_input;
use jazz::tools::{JazzClient, JsonDocumentSchema, JsonDocumentStore, QueryBuilder, Schema, Value};
use serde_json::json;
use std::time::Instant;

fn schema_and_documents() -> (Schema, JsonDocumentSchema) {
    let documents = JsonDocumentSchema::new("article_json")
        .project("/metadata/status")
        .expect("valid status pointer")
        .project("/metadata/priority")
        .expect("valid priority pointer");
    let mut schema = Schema::new();
    documents.install(&mut schema).expect("install JSON tables");
    (schema, documents)
}

#[test]
fn query_scalar_rejects_undeclared_pointer_before_building_query() {
    let (_, documents) = schema_and_documents();
    let error = documents
        .query_scalar("/body/unindexed", &json!("needle"))
        .expect_err("undeclared paths must not imply an indexed query");
    assert!(
        error
            .to_string()
            .contains("not a declared query projection"),
        "unexpected error: {error}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn ordinary_json_document_create_edit_query_and_retained_root_are_consistent() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (schema, documents) = schema_and_documents();
            let client = JazzClient::test_client(schema).await;
            let store = JsonDocumentStore::new(&client, &documents);
            let initial = json!({
                "metadata": {"status": "draft", "priority": 3},
                "body": {
                    "blocks": [
                        {"type": "paragraph", "text": "hello"},
                        {"type": "paragraph", "text": "world"}
                    ]
                },
                "escaped/key": {"~value": true}
            });

            let created = store.create(&initial).expect("create document");
            let first = store
                .load(created.document_id)
                .await
                .expect("load current root");
            assert_eq!(first.value, initial);
            assert_eq!(first.root_id, created.root_id);

            let edited = store
                .set_scalar(created.document_id, "/metadata/status", &json!("published"))
                .await
                .expect("edit one scalar in one transaction");
            assert_ne!(edited.root_id, created.root_id);
            let current = store
                .load(created.document_id)
                .await
                .expect("load edited root");
            assert_eq!(
                current.value.pointer("/metadata/status"),
                Some(&json!("published"))
            );
            assert_eq!(current.value.pointer("/metadata/priority"), Some(&json!(3)));

            // The old complete value is reconstructed from its retained root
            // and immutable parts, without consulting document row history.
            let historical = store
                .load_root(created.document_id, created.root_id)
                .await
                .expect("load retained root directly");
            assert_eq!(historical.value, initial);

            let matches = client
                .query(
                    documents
                        .query_scalar("/metadata/status", &json!("published"))
                        .expect("build declared-path query"),
                    None,
                )
                .await
                .expect("query declared path");
            assert_eq!(matches.len(), 1);
            assert_eq!(matches[0].1[0], Value::Uuid(created.document_id));

            let stale = client
                .query(
                    documents
                        .query_scalar("/metadata/status", &json!("draft"))
                        .expect("build stale-value query"),
                    None,
                )
                .await
                .expect("query stale path");
            assert!(stale.is_empty());

            // Root and projections changed in one ordinary commit, so an
            // ordinary query sees the same new root identity in both tables.
            let document_rows = client
                .query(
                    QueryBuilder::new(&documents.names.documents)
                        .filter_eq("id", Value::Uuid(created.document_id))
                        .select(&["root_id"])
                        .build(),
                    None,
                )
                .await
                .expect("query document root");
            assert_eq!(document_rows[0].1, vec![Value::Uuid(edited.root_id)]);
            assert_eq!(matches[0].1[1], Value::Uuid(edited.root_id));

            let underscored_id_rows = client
                .query(
                    QueryBuilder::new(&documents.names.documents)
                        .filter_eq("_id", Value::Uuid(created.document_id))
                        .select(&["root_id"])
                        .build(),
                    None,
                )
                .await
                .expect("query implicit _id alias");
            assert_eq!(underscored_id_rows, document_rows);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn ordinary_json_document_rejects_non_scalar_projection_and_edit_contracts() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let documents = JsonDocumentSchema::new("settings_json")
                .project("/metadata")
                .expect("valid pointer");
            let mut schema = Schema::new();
            documents.install(&mut schema).expect("install JSON tables");
            let client = JazzClient::test_client(schema).await;
            let store = JsonDocumentStore::new(&client, &documents);
            let error = store
                .create(&json!({"metadata": {"status": "draft"}}))
                .expect_err("declared projection must be scalar");
            assert!(error.to_string().contains("not scalar"));
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn duplicate_projection_fails_closed_without_advancing_root_or_leaving_new_stale_value() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let (schema, documents) = schema_and_documents();
            let client = JazzClient::test_client(schema).await;
            let store = JsonDocumentStore::new(&client, &documents);
            let created = store
                .create(&json!({
                    "metadata": {"status": "draft", "priority": 3},
                    "body": "unchanged"
                }))
                .expect("create document");

            client
                .insert(
                    &documents.names.projections,
                    row_input!(
                        "document_id" => created.document_id,
                        "root_id" => created.root_id,
                        "pointer" => "/metadata/status",
                        "scalar_json" => "\"draft\""
                    ),
                )
                .expect("plant duplicate projection row");

            let error = store
                .set_scalar(created.document_id, "/metadata/status", &json!("published"))
                .await
                .expect_err("duplicate projection must fail closed");
            assert!(
                error
                    .to_string()
                    .contains("expected exactly one declared projection row"),
                "unexpected error: {error}"
            );

            let current = store
                .load(created.document_id)
                .await
                .expect("load unchanged root");
            assert_eq!(current.root_id, created.root_id);
            assert_eq!(
                current.value.pointer("/metadata/status"),
                Some(&json!("draft"))
            );

            let published = client
                .query(
                    documents
                        .query_scalar("/metadata/status", &json!("published"))
                        .expect("published query"),
                    None,
                )
                .await
                .expect("query published projection");
            assert!(
                published.is_empty(),
                "rejected mutation leaked published projection"
            );

            let draft = client
                .query(
                    documents
                        .query_scalar("/metadata/status", &json!("draft"))
                        .expect("draft query"),
                    None,
                )
                .await
                .expect("query planted duplicate");
            assert_eq!(draft.len(), 2, "fixture retains both planted stale rows");
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "design performance receipt; invoke explicitly"]
async fn ordinary_json_document_representative_benchmark_receipt() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let documents_count = std::env::var("JAZZ_JSON_DOCUMENTS")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(100usize);
            let bytes_per_document = std::env::var("JAZZ_JSON_BYTES")
                .ok()
                .and_then(|value| value.parse().ok())
                .unwrap_or(10 * 1024usize);
            let (schema, documents) = schema_and_documents();
            let client = JazzClient::test_client(schema).await;
            let store = JsonDocumentStore::new(&client, &documents);
            let leaf_bytes = bytes_per_document / 32;
            let payload: Vec<_> = (0..32)
                .map(|index| {
                    (0..leaf_bytes)
                        .map(|offset| char::from(b'a' + ((index + offset) % 26) as u8))
                        .collect::<String>()
                })
                .collect();

            let create_started = Instant::now();
            let mut commits = Vec::with_capacity(documents_count);
            for index in 0..documents_count {
                commits.push(
                    store
                        .create(&json!({
                            "metadata": {"status": "open", "priority": index as i64 % 5},
                            "payload": payload,
                        }))
                        .expect("create benchmark document"),
                );
            }
            let create = create_started.elapsed();

            let point_read_started = Instant::now();
            for commit in &commits {
                let snapshot = store.load(commit.document_id).await.expect("load document");
                assert_eq!(snapshot.value.pointer("/metadata/status"), Some(&json!("open")));
            }
            let point_read = point_read_started.elapsed();

            let query_started = Instant::now();
            let matches = client
                .query(
                    documents
                        .query_scalar("/metadata/status", &json!("open"))
                        .expect("declared path query"),
                    None,
                )
                .await
                .expect("filter across documents");
            let query = query_started.elapsed();
            assert_eq!(matches.len(), documents_count);

            let edit_started = Instant::now();
            let first_old_root = commits[0].root_id;
            for commit in &commits {
                store
                    .set_scalar(commit.document_id, "/metadata/status", &json!("closed"))
                    .await
                    .expect("localized scalar edit");
            }
            let edit = edit_started.elapsed();

            let retained_started = Instant::now();
            let retained = store
                .load_root(commits[0].document_id, first_old_root)
                .await
                .expect("load retained root");
            let retained_read = retained_started.elapsed();
            assert_eq!(retained.value.pointer("/metadata/status"), Some(&json!("open")));

            eprintln!(
                "ORDINARY_JSON_DOCUMENT_RECEIPT docs={documents_count} logical_bytes_per_doc={bytes_per_document} parts_per_doc={} create_us_per_doc={} current_read_us_per_doc={} declared_filter_us={} edit_us_per_doc={} retained_root_read_us={}",
                retained.part_count,
                create.as_micros() / documents_count as u128,
                point_read.as_micros() / documents_count as u128,
                query.as_micros(),
                edit.as_micros() / documents_count as u128,
                retained_read.as_micros(),
            );
        })
        .await;
}
