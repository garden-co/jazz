#![cfg(feature = "test")]

mod support;

use std::collections::HashMap;
use std::time::Duration;

use jazz_tools::public_schema::SchemaHash;
use jazz_tools::row_input;
use jazz_tools::schema_lens::{Lens, LensOp, LensTransform};
use jazz_tools::server::JazzServer;
use jazz_tools::{
    ColumnType, DurabilityTier, JazzClient, QueryBuilder, SchemaBuilder, TableSchema, Value,
};
use support::{
    publish_allow_all_permissions, push_catalogue_in_memory, wait_for_edge_query_ready,
    wait_for_query,
};
use uuid::Uuid;

fn user_id(label: &str) -> String {
    Uuid::new_v5(&Uuid::NAMESPACE_URL, label.as_bytes()).to_string()
}

fn users_v1() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text),
        )
        .build()
}

fn people_v2() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("people")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text),
        )
        .build()
}

fn users_to_people_lens() -> Lens {
    Lens::new(
        SchemaHash::compute(&users_v1()),
        SchemaHash::compute(&people_v2()),
        LensTransform::with_ops(vec![LensOp::RenameTable {
            old_name: "users".to_owned(),
            new_name: "people".to_owned(),
        }]),
    )
}

fn row(id: jazz_tools::ObjectId, email: &str) -> HashMap<String, Value> {
    row_input!("id" => id, "email" => email)
}

/// Ensures a v2 update and delete select their v1 `users` parent after the
/// table is renamed to `people`, whether the parent remains base storage or is
/// moved to a partition.
///
/// ```text
/// alice(v1) ──insert users──► server ──catalogue evolve──► users base|partition
/// bob(v2)   ──update/delete people──────────────────────► settled v2 winners
/// ```
#[tokio::test]
async fn renamed_table_currency_uses_v1_parent_for_v2_update_and_delete() {
    tokio::task::LocalSet::new()
        .run_until(async {
            for old_parent_is_partition in [false, true] {
                exercise_renamed_parent_currency(old_parent_is_partition).await;
            }
        })
        .await;
}

async fn exercise_renamed_parent_currency(old_parent_is_partition: bool) {
    let server = JazzServer::start().await;
    let v1 = users_v1();
    let v2 = people_v2();
    let v2_branch = format!("client-{}-main", SchemaHash::compute(&v2).short());

    if old_parent_is_partition {
        push_catalogue_in_memory(
            server.server_state(),
            server.app_id(),
            "dev",
            "main",
            std::slice::from_ref(&v2),
            &[],
        )
        .await
        .expect("start server with v2 base storage");
        push_catalogue_in_memory(
            server.server_state(),
            server.app_id(),
            "dev",
            "main",
            std::slice::from_ref(&v1),
            &[users_to_people_lens()],
        )
        .await
        .expect("publish v1 lineage partition");
    } else {
        push_catalogue_in_memory(
            server.server_state(),
            server.app_id(),
            "dev",
            "main",
            &[v1.clone(), v2.clone()],
            &[users_to_people_lens()],
        )
        .await
        .expect("publish v1 and v2 catalogue");
    }
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &v1,
    )
    .await;

    let alice = JazzClient::connect(
        server.make_client_context_for_user(v1.clone(), user_id("currency-lineage-alice")),
    )
    .await
    .expect("connect alice");
    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;
    let id = jazz_tools::ObjectId::new();
    let (row_id, _, insert_batch) = alice
        .insert("users", row(id, "before@example.com"))
        .expect("insert v1 user");
    alice
        .wait_for_batch(insert_batch, DurabilityTier::EdgeServer)
        .await
        .expect("v1 content winner settles");

    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &v2,
    )
    .await;

    let bob = JazzClient::connect(
        server.make_client_context_for_user(v2, user_id("currency-lineage-bob")),
    )
    .await
    .expect("connect bob");
    wait_for_edge_query_ready(&bob, "people", Duration::from_secs(30)).await;

    let update_batch = bob
        .update(
            row_id,
            vec![(
                "email".to_owned(),
                Value::Text("after@example.com".to_owned()),
            )],
        )
        .expect("update renamed row");
    bob.wait_for_batch(update_batch, DurabilityTier::EdgeServer)
        .await
        .expect("v2 content winner settles over v1 parent");
    let updated = wait_for_query(
        &bob,
        QueryBuilder::new("people").branch(v2_branch.clone()).build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "v2 content winner is observable after renamed-parent update",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == row_id
                && rows[0].1 == vec![Value::Uuid(id), Value::Text("after@example.com".into())])
                .then_some(rows)
        },
    )
    .await;
    assert_eq!(updated.len(), 1);

    let delete_batch = bob.delete(row_id).expect("delete renamed row");
    bob.wait_for_batch(delete_batch, DurabilityTier::EdgeServer)
        .await
        .expect("v2 deletion winner settles independently of content");
    let deleted = wait_for_query(
        &bob,
        QueryBuilder::new("people").branch(v2_branch).build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "v2 deletion winner hides renamed-parent content",
        |rows| rows.is_empty().then_some(rows),
    )
    .await;
    assert!(deleted.is_empty());

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}
