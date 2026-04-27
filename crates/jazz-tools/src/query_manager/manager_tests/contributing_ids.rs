use super::*;

#[test]
fn contributing_ids_reflect_filter() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert 3 rows with different scores
    let handle1 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let handle2 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(30)],
        )
        .unwrap();
    let handle3 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(75)],
        )
        .unwrap();

    // Subscribe to query: score > 50
    let query = qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();
    let sub_id = qm.subscribe(query.clone()).unwrap();

    qm.process(&mut storage);

    // Get contributing ObjectIds
    let contributing = qm.get_subscription_contributing_ids(sub_id);

    // Should have 2 rows (Alice: 100, Charlie: 75), not Bob (30)
    assert_eq!(contributing.len(), 2, "Should have 2 contributing IDs");

    let branch_str = get_branch(&qm);
    let branch = crate::object::BranchName::new(&branch_str);
    assert!(
        contributing.contains(&(handle1.row_id, branch)),
        "Alice should be in contributing set"
    );
    assert!(
        !contributing.contains(&(handle2.row_id, branch)),
        "Bob should NOT be in contributing set (score < 50)"
    );
    assert!(
        contributing.contains(&(handle3.row_id, branch)),
        "Charlie should be in contributing set"
    );
}

#[test]
fn contributing_ids_update_reactively() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert 2 rows initially
    let _handle1 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let handle2 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(30)],
        )
        .unwrap();

    // Subscribe to query: score > 50
    let query = qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();
    let sub_id = qm.subscribe(query.clone()).unwrap();

    qm.process(&mut storage);

    // Initially 1 match (Alice: 100)
    let contributing = qm.get_subscription_contributing_ids(sub_id);
    assert_eq!(
        contributing.len(),
        1,
        "Should have 1 contributing ID initially"
    );

    // Insert new row with score > 50
    let handle3 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(75)],
        )
        .unwrap();

    qm.process(&mut storage);

    // Now 2 matches
    let contributing = qm.get_subscription_contributing_ids(sub_id);
    assert_eq!(
        contributing.len(),
        2,
        "Should have 2 contributing IDs after insert"
    );

    let branch_str = get_branch(&qm);
    let branch = crate::object::BranchName::new(&branch_str);
    assert!(
        contributing.contains(&(handle3.row_id, branch)),
        "Charlie should be in contributing set"
    );

    // Update Bob's score to > 50
    qm.update(
        &mut storage,
        handle2.row_id,
        &[Value::Text("Bob".into()), Value::Integer(60)],
    )
    .unwrap();
    qm.process(&mut storage);

    // Now 3 matches
    let contributing = qm.get_subscription_contributing_ids(sub_id);
    assert_eq!(
        contributing.len(),
        3,
        "Should have 3 contributing IDs after update"
    );
    assert!(
        contributing.contains(&(handle2.row_id, branch)),
        "Bob should now be in contributing set"
    );
}

#[test]
fn contributing_ids_for_limit_offset_include_ordered_prefix() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let handle_a = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("A".into()), Value::Integer(1)],
        )
        .unwrap();
    let handle_b = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("B".into()), Value::Integer(2)],
        )
        .unwrap();
    let handle_c = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("C".into()), Value::Integer(3)],
        )
        .unwrap();
    let _handle_d = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("D".into()), Value::Integer(4)],
        )
        .unwrap();

    let query = qm
        .query("users")
        .order_by("score")
        .offset(2)
        .limit(1)
        .build();
    let sub_id = qm.subscribe(query).unwrap();

    qm.process(&mut storage);

    let branch = crate::object::BranchName::new(get_branch(&qm));
    let contributing = qm.get_subscription_contributing_ids(sub_id);

    assert_eq!(
        contributing.len(),
        3,
        "Paginated queries need the ordered prefix through offset + limit"
    );
    assert!(contributing.contains(&(handle_a.row_id, branch)));
    assert!(contributing.contains(&(handle_b.row_id, branch)));
    assert!(contributing.contains(&(handle_c.row_id, branch)));
}

#[test]
fn contributing_ids_for_offset_only_include_full_input() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let handles = [
        qm.insert(
            &mut storage,
            "users",
            &[Value::Text("A".into()), Value::Integer(1)],
        )
        .unwrap(),
        qm.insert(
            &mut storage,
            "users",
            &[Value::Text("B".into()), Value::Integer(2)],
        )
        .unwrap(),
        qm.insert(
            &mut storage,
            "users",
            &[Value::Text("C".into()), Value::Integer(3)],
        )
        .unwrap(),
        qm.insert(
            &mut storage,
            "users",
            &[Value::Text("D".into()), Value::Integer(4)],
        )
        .unwrap(),
    ];

    let query = qm.query("users").order_by("score").offset(2).build();
    let sub_id = qm.subscribe(query).unwrap();

    qm.process(&mut storage);

    let branch = crate::object::BranchName::new(get_branch(&qm));
    let contributing = qm.get_subscription_contributing_ids(sub_id);

    assert_eq!(
        contributing.len(),
        handles.len(),
        "Offset without limit still needs the full ordered input to replay locally"
    );
    for handle in handles {
        assert!(contributing.contains(&(handle.row_id, branch)));
    }
}
