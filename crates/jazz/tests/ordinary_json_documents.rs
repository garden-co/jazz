#![cfg(feature = "test-utils")]

use jazz::tools::{JazzClient, JsonDocumentSchema, JsonDocumentStore, QueryBuilder, Schema, Value};
use serde_json::json;

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
                        .select(&["root_id"])
                        .build(),
                    None,
                )
                .await
                .expect("query document root");
            assert_eq!(document_rows[0].1, vec![Value::Uuid(edited.root_id)]);
            assert_eq!(matches[0].1[1], Value::Uuid(edited.root_id));
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
