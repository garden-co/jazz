use super::*;

/// Verifies that recursive inherited access fails closed when row data forms a
/// cycle and no reachable ancestor grants the session access.
#[cfg(feature = "test-utils")]
#[tokio::test]
async fn rebac_recursive_inherits_cycle_does_not_overgrant() {
    let schema = recursive_folders_schema(Some(10));
    let client = crate::JazzClient::test_client(schema).await;

    let (a, _, _) = client
        .insert(
            "folders",
            crate::row_input!("owner_id" => "bob", "name" => "A", "parent_id" => Value::Null),
        )
        .expect("insert folder A");
    let (b, _, _) = client
        .insert(
            "folders",
            crate::row_input!("owner_id" => "carol", "name" => "B", "parent_id" => a),
        )
        .expect("insert folder B");

    // Close the cycle: A.parent_id = B
    client
        .for_session(Session::new("bob"))
        .update(a, vec![("parent_id".to_string(), Value::Uuid(b))])
        .expect("close folder cycle");

    let result_ids: HashSet<_> = client
        .for_session(Session::new("alice"))
        .query(QueryBuilder::new("folders").build(), None)
        .await
        .expect("query folders as alice")
        .into_iter()
        .map(|(id, _)| id)
        .collect();

    assert!(
        result_ids.is_empty(),
        "Cycle should not grant access when no ancestor is owned by session user"
    );
}
