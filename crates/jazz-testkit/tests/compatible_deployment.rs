use jazz::{
    query::Query,
    tools::{
        ColumnDescriptor, ColumnMergeStrategy, ColumnType, PolicyExpr, RowDescriptor, Schema,
        SchemaHash, TableName, TablePolicies, TableSchema, Value,
    },
};
use jazz_server::JazzServer;
use jazz_testkit::{connect_ready_user, wait_for_global_txs, wait_for_visible_row};
use std::{collections::HashMap, time::Duration};

fn schema(counter: bool) -> Schema {
    let title = ColumnDescriptor::new("title", ColumnType::Text)
        .default(Value::Text(if counter { "new" } else { "old" }.into()));
    let count = ColumnDescriptor::new("count", ColumnType::Integer);
    let columns = if counter {
        vec![count.merge_strategy(ColumnMergeStrategy::Counter), title]
    } else {
        vec![title, count]
    };
    Schema::from([(
        TableName::from("notes"),
        TableSchema::new(RowDescriptor::new(columns)),
    )])
}

async fn deploy(server: &JazzServer, schema: &Schema) {
    let policies = TablePolicies::new()
        .with_select(PolicyExpr::True)
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True);
    let permissions = HashMap::from([(TableName::from("notes"), policies)]);
    let hash = SchemaHash::compute(schema).to_string();
    let response = reqwest::Client::new().post(format!("{}/apps/{}/admin/deploy", server.base_url(), server.app_id()))
        .header("X-Jazz-Admin-Secret", server.admin_secret())
        .json(&serde_json::json!({"targetSchemaHash":hash, "schemas":[{"hash":hash,"schema":schema}], "migrations":[], "permissions":permissions}))
        .send().await.unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert!(status.is_success(), "{status}: {body}");
}

/// Alice writes with the old definition, the administrator deploys compatible
/// defaults/order/merge semantics, and Bob reads the original values and writes.
/// Alice -> old row -> server -> compatible deployment -> Bob -> old + new rows.
#[tokio::test]
async fn automatic_lens_preserves_authored_values() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let server = JazzServer::builder()
                .with_admin_secret("compatible-deploy-test")
                .start()
                .await
                .unwrap();
            let old = schema(false);
            deploy(&server, &old).await;
            let alice = connect_ready_user(
                &server,
                &old,
                "9750dcc2-516e-5ea0-8a26-54fa6ff6986b",
                "notes",
                Duration::from_secs(30),
            )
            .await;
            let (row, _, tx) = alice
                .insert(
                    "notes",
                    jazz::row_input!("title" => "authored", "count" => 7),
                )
                .unwrap();
            wait_for_global_txs(&alice, &[tx.unwrap()]).await;
            alice.shutdown().await.unwrap();
            let new = schema(true);
            deploy(&server, &new).await;
            let bob = connect_ready_user(
                &server,
                &new,
                "5363f5ca-d268-52d3-af19-c4c0c5e93f63",
                "notes",
                Duration::from_secs(30),
            )
            .await;
            let query = Query::from("notes").select(["title", "count"]);
            wait_for_visible_row(
                &bob,
                query.clone(),
                "old row survives compatible deployment",
                row,
                vec![Value::Text("authored".into()), Value::Integer(7)],
            )
            .await;
            let (new_row, _, tx) = bob.insert("notes", jazz::row_input!("count" => 9)).unwrap();
            wait_for_global_txs(&bob, &[tx.unwrap()]).await;
            wait_for_visible_row(
                &bob,
                query,
                "new writes use the new default",
                new_row,
                vec![Value::Text("new".into()), Value::Integer(9)],
            )
            .await;
            bob.shutdown().await.unwrap();
            server.shutdown().await;
        })
        .await;
}
