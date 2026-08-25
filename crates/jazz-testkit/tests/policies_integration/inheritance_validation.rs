use super::*;
use jazz::tools::DurabilityTier;
use jazz_server::JazzServer;
use jazz_testkit::{connect_ready_client, connect_ready_user, wait_for_edge_txs};

/// Verifies that recursive inherited access fails closed when row data forms a
/// cycle and no reachable ancestor grants the session access.
#[tokio::test]
#[ignore = "#1763: recursive INHERITS cycles still time out before EdgeServer durability"]
async fn rebac_recursive_inherits_cycle_does_not_overgrant() {
    tokio::task::LocalSet::new()
        .run_until(rebac_recursive_inherits_cycle_does_not_overgrant_inner())
        .await;
}

async fn rebac_recursive_inherits_cycle_does_not_overgrant_inner() {
    let schema = recursive_folders_schema(Some(10));
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let admin = connect_ready_client(
        &server,
        &schema,
        "inheritance-cycle-admin",
        "folders",
        Duration::from_secs(30),
    )
    .await;
    let alice = connect_ready_user(
        &server,
        &schema,
        super::ALICE_ID,
        "folders",
        Duration::from_secs(30),
    )
    .await;

    let (a, _, a_tx) = admin
        .insert(
            "folders",
            crate::row_input!("owner_id" => super::BOB_ID, "name" => "A", "parent_id" => Value::Null),
        )
        .expect("insert folder A");
    let (b, _, b_tx) = admin
        .insert(
            "folders",
            crate::row_input!("owner_id" => super::CAROL_ID, "name" => "B", "parent_id" => a),
        )
        .expect("insert folder B");
    wait_for_edge_txs(
        &admin,
        &[
            a_tx.expect("folder A insert should commit immediately"),
            b_tx.expect("folder B insert should commit immediately"),
        ],
    )
    .await;

    // Close the cycle: A.parent_id = B
    let cycle_tx = admin
        .update(a, vec![("parent_id".to_string(), Value::Uuid(b))])
        .expect("close folder cycle")
        .expect("cycle update should commit immediately");
    wait_for_edge_txs(&admin, &[cycle_tx]).await;

    let result_ids: HashSet<_> = alice
        .query(Query::from("folders"), Some(DurabilityTier::EdgeServer))
        .await
        .expect("query folders as alice")
        .into_iter()
        .map(|(id, _)| id)
        .collect();

    assert!(
        result_ids.is_empty(),
        "Cycle should not grant access when no ancestor is owned by session user"
    );

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}
