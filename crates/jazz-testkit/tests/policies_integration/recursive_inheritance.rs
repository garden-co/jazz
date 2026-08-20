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

/// Verifies unbounded recursive inheritance: ownership of an ancestor folder
/// grants visibility through the full descendant chain.
#[tokio::test]
#[ignore = "unbounded recursive INHERITS overflows the jazz-server-shell stack"]
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
