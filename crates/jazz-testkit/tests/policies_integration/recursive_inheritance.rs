use crate::JazzClient;
use jazz::tools::{DurabilityTier, TransactionId};
use jazz_server::JazzServer;
use jazz_testkit::{connect_ready_client, connect_ready_user, wait_for_edge_txs};

use super::*;

const READY_TIMEOUT: Duration = Duration::from_secs(30);

fn insert_folder(
    client: &JazzClient,
    owner_id: &str,
    name: &str,
    parent_id: Option<ObjectId>,
) -> (ObjectId, TransactionId) {
    let (id, _, transaction_id) = client
        .insert(
            "folders",
            crate::row_input!("owner_id" => owner_id, "name" => name, "parent_id" => parent_id),
        )
        .expect("insert folder");
    (
        id,
        transaction_id.expect("folder insert should commit immediately"),
    )
}

async fn query_folder_ids(client: &JazzClient) -> HashSet<ObjectId> {
    client
        .query(Query::from("folders"), Some(DurabilityTier::EdgeServer))
        .await
        .expect("query folders")
        .into_iter()
        .map(|(id, _)| id)
        .collect()
}

async fn query_folder_name_as(client: &JazzClient, folder_id: ObjectId) -> Option<String> {
    client
        .query(
            Query::from("folders")
                .filter(eq(col("id"), lit(*folder_id.uuid())))
                .select(["name"]),
            Some(DurabilityTier::EdgeServer),
        )
        .await
        .expect("query folders")
        .first()
        .map(|(_, values)| {
            let Some(Value::Text(name)) = values.first() else {
                panic!("folder name should be selected as text");
            };
            name.clone()
        })
}

/// Verifies unbounded recursive inheritance: ownership of an ancestor folder
/// grants visibility through the full descendant chain.
#[tokio::test]
async fn rebac_recursive_inherits_allows_ancestor_access() {
    tokio::task::LocalSet::new()
        .run_until(rebac_recursive_inherits_allows_ancestor_access_inner())
        .await;
}

async fn rebac_recursive_inherits_allows_ancestor_access_inner() {
    let schema = recursive_folders_schema(None);
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let admin = connect_ready_client(
        &server,
        &schema,
        "recursive-inheritance-admin",
        "folders",
        READY_TIMEOUT,
    )
    .await;
    let alice =
        connect_ready_user(&server, &schema, super::ALICE_ID, "folders", READY_TIMEOUT).await;

    let (root, root_tx) = insert_folder(&admin, super::ALICE_ID, "Root", None);
    let (child, child_tx) = insert_folder(&admin, super::BOB_ID, "Child", Some(root));
    let (grand, grand_tx) = insert_folder(&admin, super::CAROL_ID, "Grandchild", Some(child));
    wait_for_edge_txs(&admin, &[root_tx, child_tx, grand_tx]).await;

    let result_ids = query_folder_ids(&alice).await;

    assert!(result_ids.contains(&root), "Root should be visible");
    assert!(
        result_ids.contains(&child),
        "Child should be visible via recursive INHERITS"
    );
    assert!(
        result_ids.contains(&grand),
        "Grandchild should be visible via recursive INHERITS"
    );

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// Verifies that recursive inheritance depth limits are honored, granting
/// access only to descendants within the configured max depth.
#[tokio::test]
async fn rebac_recursive_inherits_respects_depth_override() {
    tokio::task::LocalSet::new()
        .run_until(rebac_recursive_inherits_respects_depth_override_inner())
        .await;
}

async fn rebac_recursive_inherits_respects_depth_override_inner() {
    let schema = recursive_folders_schema(Some(1));
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let admin = connect_ready_client(
        &server,
        &schema,
        "recursive-inheritance-admin",
        "folders",
        READY_TIMEOUT,
    )
    .await;
    let alice =
        connect_ready_user(&server, &schema, super::ALICE_ID, "folders", READY_TIMEOUT).await;

    let (root, root_tx) = insert_folder(&admin, super::ALICE_ID, "Root", None);
    let (child, child_tx) = insert_folder(&admin, super::BOB_ID, "Child", Some(root));
    let (grand, grand_tx) = insert_folder(&admin, super::CAROL_ID, "Grandchild", Some(child));
    wait_for_edge_txs(&admin, &[root_tx, child_tx, grand_tx]).await;

    let result_ids = query_folder_ids(&alice).await;

    assert!(result_ids.contains(&root), "Root should be visible");
    assert!(
        result_ids.contains(&child),
        "Child should be visible at depth=1"
    );
    assert!(
        !result_ids.contains(&grand),
        "Grandchild should be hidden when max_depth=1"
    );

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

async fn run_recursive_folder_update(max_depth: Option<usize>) -> (bool, bool) {
    let schema = recursive_folders_schema(max_depth);
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let admin = connect_ready_client(
        &server,
        &schema,
        "recursive-inheritance-admin",
        "folders",
        READY_TIMEOUT,
    )
    .await;
    let alice =
        connect_ready_user(&server, &schema, super::ALICE_ID, "folders", READY_TIMEOUT).await;
    let bob = connect_ready_user(&server, &schema, super::BOB_ID, "folders", READY_TIMEOUT).await;

    let (root, root_tx) = insert_folder(&admin, super::ALICE_ID, "Root", None);
    let (child, child_tx) = insert_folder(&admin, super::BOB_ID, "Child", Some(root));
    let (grand, grand_tx) = insert_folder(&admin, super::BOB_ID, "Grandchild", Some(child));
    wait_for_edge_txs(&admin, &[root_tx, child_tx, grand_tx]).await;
    let _alice_visible = query_folder_ids(&alice).await;
    let _bob_visible = query_folder_ids(&bob).await;

    let result = alice.update(
        grand,
        vec![("name".to_string(), Value::Text("Renamed by Alice".into()))],
    );
    let result_is_err = result.is_err();
    if let Ok(Some(transaction_id)) = result {
        wait_for_edge_txs(&alice, &[transaction_id]).await;
    }

    let name = query_folder_name_as(&bob, grand)
        .await
        .expect("bob should be able to see his folder");

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;

    (result_is_err, name == "Renamed by Alice")
}

/// Verifies recursive inherited UPDATE checks: too-shallow depth denies and
/// preserves the row, while sufficient depth allows the update.
#[tokio::test]
#[ignore = "#1762: the depth-2 inherited UPDATE remains pending and times out before EdgeServer durability"]
async fn rebac_recursive_inherits_write_checks_allow_and_deny() {
    tokio::task::LocalSet::new()
        .run_until(rebac_recursive_inherits_write_checks_allow_and_deny_inner())
        .await;
}

async fn rebac_recursive_inherits_write_checks_allow_and_deny_inner() {
    let (denied_shallow, applied_shallow) = run_recursive_folder_update(Some(1)).await;
    assert!(
        denied_shallow,
        "Update should be denied when recursive INHERITS max depth is too shallow"
    );
    assert!(
        !applied_shallow,
        "Denied update must not be applied to the row"
    );

    let (denied_deep, applied_deep) = run_recursive_folder_update(Some(2)).await;
    assert!(
        !denied_deep,
        "Update should be allowed when max depth reaches the ancestor owner"
    );
    assert!(applied_deep, "Allowed update should be applied");
}
