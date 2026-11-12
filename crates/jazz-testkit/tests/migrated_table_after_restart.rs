//! A table a migration adds keeps taking writes after the server restarts
//! (#4769). The deployment publishes v1 and v2 with a `createTables` lens
//! and activates nothing, as createJazz does, so the restarted core opens
//! under its genesis v1 while clients write v2's added table and column.

use jazz::query::Query;
use jazz::row_input;
use jazz::tools::{
    ColumnType, JazzClient, ObjectId, PolicyExpr, ReadTier, Schema, SchemaBuilder, SchemaHash,
    TableName, TablePolicies, TableSchema, Value,
};
use jazz_server::JazzServer;
use jazz_testkit::{connect_ready_user, wait_for_global_txs};
use std::{collections::HashMap, path::Path, time::Duration};

const ADMIN_SECRET: &str = "migrated-table-after-restart";

fn v1() -> Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("parents").column("title", ColumnType::Text))
        .build()
}

/// v1 plus `parents.rank: bigint?` and the new table
/// `children(parentId: uuid, name: text, size: bigint)`.
fn v2() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("parents")
                .column("title", ColumnType::Text)
                .nullable_column("rank", ColumnType::BigInt),
        )
        .table(
            TableSchema::builder("children")
                .column("parentId", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .column("size", ColumnType::BigInt),
        )
        .build()
}

fn hash(schema: &Schema) -> String {
    SchemaHash::compute(schema).to_string()
}

/// Publish v1 and v2 with the migration a generated `createTables:
/// { children: true }` migration lowers to, and permissions allowing
/// everything on both tables.
async fn deploy(server: &JazzServer) {
    let policies = TablePolicies::new()
        .with_select(PolicyExpr::True)
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True);
    let permissions = HashMap::from([
        (TableName::from("parents"), policies.clone()),
        (TableName::from("children"), policies),
    ]);
    let (v1, v2) = (v1(), v2());
    let body = serde_json::json!({
        "targetSchemaHash": hash(&v2),
        "schemas": [
            {"hash": hash(&v1), "schema": v1},
            {"hash": hash(&v2), "schema": v2},
        ],
        "migrations": [{
            "fromHash": hash(&v1),
            "toHash": hash(&v2),
            "forward": [
                {"table": "parents", "operations": [{
                    "type": "introduce",
                    "column": "rank",
                    "column_type": ColumnType::BigInt,
                    "value": jazz::tools::public_schema::Value::Null,
                }]},
                {"table": "children", "added": true, "operations": []},
            ],
        }],
        "permissions": permissions,
    });
    let response = reqwest::Client::new()
        .post(format!(
            "{}/apps/{}/admin/bundles",
            server.base_url(),
            server.app_id()
        ))
        .header("X-Jazz-Admin-Secret", ADMIN_SECRET)
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let body = response.text().await.unwrap();
    assert!(status.is_success(), "{status}: {body}");
}

async fn start(data_dir: &Path) -> JazzServer {
    JazzServer::builder()
        .with_app_id(JazzServer::default_app_id())
        .with_admin_secret(ADMIN_SECRET)
        .with_data_dir(data_dir)
        .with_persistent_storage()
        .start()
        .await
        .expect("start test server")
}

/// Insert a parent with a `rank` and a child of it, and wait for both to
/// be globally durable.
async fn write(client: &JazzClient, name: &str, size: i64) {
    let (parent, _, parent_tx) = client
        .insert(
            "parents",
            row_input!("title" => name, "rank" => Value::BigInt(size)),
        )
        .unwrap();
    let (_, _, child_tx) = client
        .insert(
            "children",
            row_input!(
                "parentId" => Value::Uuid(parent),
                "name" => name,
                "size" => Value::BigInt(size)
            ),
        )
        .unwrap();
    wait_for_global_txs(client, &[parent_tx.unwrap(), child_tx.unwrap()]).await;
}

async fn children(client: &JazzClient) -> Vec<Vec<Value>> {
    let mut rows: Vec<Vec<Value>> = client
        .query(
            Query::from("children").select(["name", "size"]),
            ReadTier::Remote,
        )
        .await
        .map(jazz::tools::test_support::ordinary_rows)
        .expect("query children")
        .into_iter()
        .map(|(_, values): (ObjectId, Vec<Value>)| values)
        .collect();
    rows.sort_by_key(|values| format!("{values:?}"));
    rows
}

/// Alice writes the migrated table; the server restarts on the same data
/// dir; Bob writes it again and reads both rows back.
///
/// ```
/// deploy(v1, v2, createTables) ── alice(v2) ──insert──► ok
/// restart (core reopens under v1)
///                               ── bob(v2)   ──insert──► ok (was: "value does not match type U64")
///                               ── bob       ──read────► both rows
/// ```
#[tokio::test]
async fn a_migrated_table_takes_writes_after_the_server_restarts() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let data_dir = tempfile::tempdir().unwrap();
            let server = start(data_dir.path()).await;
            deploy(&server).await;
            let alice = connect_ready_user(
                &server,
                &v2(),
                "9750dcc2-516e-5ea0-8a26-54fa6ff6986b",
                "children",
                Duration::from_secs(30),
            )
            .await;
            write(&alice, "before restart", 1).await;
            alice.shutdown().await.unwrap();
            server.shutdown().await;

            let server = start(data_dir.path()).await;
            let bob = connect_ready_user(
                &server,
                &v2(),
                "5363f5ca-d268-52d3-af19-c4c0c5e93f63",
                "children",
                Duration::from_secs(30),
            )
            .await;
            write(&bob, "after restart", 2).await;
            assert_eq!(
                children(&bob).await,
                vec![
                    vec![Value::Text("after restart".into()), Value::BigInt(2)],
                    vec![Value::Text("before restart".into()), Value::BigInt(1)],
                ]
            );
            bob.shutdown().await.unwrap();
            server.shutdown().await;
        })
        .await;
}
