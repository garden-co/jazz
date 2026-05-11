use super::*;

#[test]
fn uuid_array_fk_forward_materialization_preserves_order_and_duplicates() {
    let sync_manager = SyncManager::new();
    let schema = file_storage_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let part_a = qm
        .insert(&mut storage, "file_parts", &[Value::Text("A".into())])
        .unwrap();
    let part_b = qm
        .insert(&mut storage, "file_parts", &[Value::Text("B".into())])
        .unwrap();

    qm.insert(
        &mut storage,
        "files",
        &[Value::Array(vec![
            Value::Uuid(part_b.row_id),
            Value::Uuid(part_a.row_id),
            Value::Uuid(part_b.row_id),
        ])],
    )
    .unwrap();

    let query = qm
        .query("files")
        .with_array("part_rows", |sub| {
            sub.from("file_parts").correlate("id", "files.parts")
        })
        .build();
    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let update = qm
        .take_updates()
        .into_iter()
        .find(|u| u.subscription_id == sub_id)
        .expect("files subscription should produce one update");
    let row_values =
        decode_row(&files_with_parts_descriptor(), &update.delta.added[0].data).unwrap();
    let part_rows = row_values[1]
        .as_array()
        .expect("part_rows should be an array");
    let labels: Vec<String> = part_rows
        .iter()
        .map(|row| {
            let values = row.as_row().expect("part row");
            assert!(row.row_id().is_some(), "row should have an id");
            match &values[0] {
                Value::Text(label) => label.clone(),
                other => panic!("expected text label, got {other:?}"),
            }
        })
        .collect();
    assert_eq!(labels, vec!["B", "A", "B"]);
}

#[test]
fn uuid_array_fk_reverse_membership_and_index_updates_on_edit() {
    let sync_manager = SyncManager::new();
    let schema = file_storage_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let branch = get_branch(&qm);

    let part_a = qm
        .insert(&mut storage, "file_parts", &[Value::Text("A".into())])
        .unwrap();
    let part_b = qm
        .insert(&mut storage, "file_parts", &[Value::Text("B".into())])
        .unwrap();
    let file = qm
        .insert(
            &mut storage,
            "files",
            &[Value::Array(vec![
                Value::Uuid(part_a.row_id),
                Value::Uuid(part_b.row_id),
                Value::Uuid(part_b.row_id),
            ])],
        )
        .unwrap();

    let query = qm
        .query("file_parts")
        .with_array("files", |sub| {
            sub.from("files").correlate("parts", "file_parts.id")
        })
        .build();
    let before = execute_query(&mut qm, &mut storage, query.clone()).unwrap();
    let before_counts: std::collections::HashMap<String, usize> = before
        .iter()
        .map(|(_, values)| {
            let label = match &values[0] {
                Value::Text(label) => label.clone(),
                other => panic!("expected label text, got {other:?}"),
            };
            let count = values[1]
                .as_array()
                .expect("files include should be array")
                .len();
            (label, count)
        })
        .collect();
    assert_eq!(before_counts.get("A"), Some(&1));
    assert_eq!(before_counts.get("B"), Some(&1));

    let ids_for_a_before =
        storage.index_lookup("files", "parts", &branch, &Value::Uuid(part_a.row_id));
    let ids_for_b_before =
        storage.index_lookup("files", "parts", &branch, &Value::Uuid(part_b.row_id));
    assert!(ids_for_a_before.contains(&file.row_id));
    assert!(ids_for_b_before.contains(&file.row_id));

    qm.update(
        &mut storage,
        file.row_id,
        &[Value::Array(vec![Value::Uuid(part_b.row_id)])],
    )
    .unwrap();

    let after = execute_query(&mut qm, &mut storage, query).unwrap();
    let after_counts: std::collections::HashMap<String, usize> = after
        .iter()
        .map(|(_, values)| {
            let label = match &values[0] {
                Value::Text(label) => label.clone(),
                other => panic!("expected label text, got {other:?}"),
            };
            let count = values[1]
                .as_array()
                .expect("files include should be array")
                .len();
            (label, count)
        })
        .collect();
    assert_eq!(after_counts.get("A"), Some(&0));
    assert_eq!(after_counts.get("B"), Some(&1));

    let ids_for_a_after =
        storage.index_lookup("files", "parts", &branch, &Value::Uuid(part_a.row_id));
    let ids_for_b_after =
        storage.index_lookup("files", "parts", &branch, &Value::Uuid(part_b.row_id));
    assert!(
        !ids_for_a_after.contains(&file.row_id),
        "removed array members should be removed from membership index"
    );
    assert!(ids_for_b_after.contains(&file.row_id));
}

#[test]
fn array_subquery_single_user_with_posts() {
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert one user: Alice with id=1
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();

    // Insert two posts for Alice
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Alice Post 1".into()),
            Value::Integer(1), // author_id = 1 (Alice)
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(101),
            Value::Text("Alice Post 2".into()),
            Value::Integer(1), // author_id = 1 (Alice)
        ],
    )
    .unwrap();

    // Query users with their posts as array
    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "users.id")
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have subscription update");

    // Should have exactly 1 user row
    assert_eq!(delta.added.len(), 1, "Expected 1 user row");

    // Decode the output row
    let output_descriptor = users_with_posts_descriptor();
    let row_data = &delta.added[0].data;
    let values = decode_row(&output_descriptor, row_data).expect("Should decode output row");

    // Verify user fields
    assert_eq!(values[0], Value::Integer(1), "User id should be 1");
    assert_eq!(
        values[1],
        Value::Text("Alice".into()),
        "User name should be Alice"
    );

    // Verify posts array
    let posts = values[2].as_array().expect("Third column should be array");
    assert_eq!(posts.len(), 2, "Alice should have 2 posts");

    // Each post is a Row of [id, title, author_id] with id in the Row struct
    for post in posts {
        let post_values = post.as_row().expect("Each post should be a Row");
        assert_eq!(
            post_values.len(),
            3,
            "Post should have 3 fields (schema cols)"
        );
        assert!(post.row_id().is_some(), "Post Row should have an id");
        // Verify author_id matches Alice (index 2)
        assert_eq!(
            post_values[2],
            Value::Integer(1),
            "Post author_id should be 1 (Alice)"
        );
    }
}

#[test]
fn array_subquery_update_descriptor_includes_array_column() {
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Hello world".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "users.id")
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let update = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .expect("should have subscription update");

    assert_eq!(
        update.descriptor.columns.len(),
        3,
        "Update descriptor should have 3 columns (base + array), got {}: {:?}",
        update.descriptor.columns.len(),
        update
            .descriptor
            .columns
            .iter()
            .map(|c| &c.name)
            .collect::<Vec<_>>()
    );

    let row_data = &update.delta.added[0].data;
    let values =
        decode_row(&update.descriptor, row_data).expect("should decode with update descriptor");

    assert_eq!(values[0], Value::Integer(1), "user id");
    assert_eq!(
        values[1],
        Value::Text("Alice".into()),
        "user name should be 'Alice', not corrupted"
    );

    // The included posts array should contain the post we inserted
    let posts = values[2]
        .as_array()
        .expect("third column should be the posts array");
    assert_eq!(posts.len(), 1, "Alice should have 1 post");
    let post_row = posts[0].as_row().expect("post element should be a Row");
    assert!(posts[0].row_id().is_some(), "post Row should have an id");
    assert_eq!(post_row[0], Value::Integer(100), "post id");
    assert_eq!(post_row[1], Value::Text("Hello world".into()), "post title");
    assert_eq!(post_row[2], Value::Integer(1), "post author_id");
}

#[test]
fn array_subquery_user_with_no_posts() {
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert user with no posts
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Lonely".into())],
    )
    .unwrap();

    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "users.id")
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have subscription update");

    assert_eq!(delta.added.len(), 1, "Should have 1 user");

    let output_descriptor = users_with_posts_descriptor();
    let values =
        decode_row(&output_descriptor, &delta.added[0].data).expect("Should decode output row");

    assert_eq!(values[0], Value::Integer(1));
    assert_eq!(values[1], Value::Text("Lonely".into()));

    // Posts array should be empty
    let posts = values[2].as_array().expect("Should have posts array");
    assert_eq!(posts.len(), 0, "User with no posts should have empty array");
}

#[test]
fn array_subquery_require_result_filters_and_readds_rows() {
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();

    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts")
                .correlate("author_id", "users.id")
                .require_result()
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let initial_updates = qm.take_updates();
    let initial_delta = initial_updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have subscription update");
    assert!(
        initial_delta.added.is_empty(),
        "user without posts should be filtered when include is required"
    );

    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Hello world".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.process(&mut storage);

    let follow_up_updates = qm.take_updates();
    let follow_up_delta = follow_up_updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have subscription update after post insert");

    assert_eq!(follow_up_delta.added.len(), 1);
    let row = decode_row(
        &users_with_posts_descriptor(),
        &follow_up_delta.added[0].data,
    )
    .unwrap();
    assert_eq!(row[0], Value::Integer(1));
    assert_eq!(row[1], Value::Text("Alice".into()));
    assert_eq!(row[2].as_array().expect("posts array").len(), 1);
}

#[test]
fn array_subquery_require_full_cardinality_filters_incomplete_array_refs() {
    let sync_manager = SyncManager::new();
    let schema = groups_users_array_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    qm.insert(
        &mut storage,
        "groups",
        &[
            Value::Integer(10),
            Value::Text("Maintainers".into()),
            Value::Array(vec![Value::Integer(1), Value::Integer(2)]),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();

    let query = qm
        .query("groups")
        .with_array("members", |sub| {
            sub.from("users")
                .correlate("user_id", "groups.member_ids")
                .require_match_correlation_cardinality()
        })
        .build();

    let initial_results = execute_query(&mut qm, &mut storage, query.clone()).unwrap();
    assert!(
        initial_results.is_empty(),
        "group should be filtered when one referenced member is missing"
    );

    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(2), Value::Text("Bob".into())],
    )
    .unwrap();
    let follow_up_results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(follow_up_results.len(), 1);
    let row = &follow_up_results[0].1;
    assert_eq!(row[0], Value::Integer(10));
    assert_eq!(row[1], Value::Text("Maintainers".into()));
    assert_eq!(
        row[2],
        Value::Array(vec![Value::Integer(1), Value::Integer(2)])
    );
    assert_eq!(row[3].as_array().expect("members array").len(), 2);
}

#[test]
fn array_subquery_multiple_users_correct_correlation() {
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert users
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(2), Value::Text("Bob".into())],
    )
    .unwrap();

    // Alice's posts (author_id = 1)
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Alice Post".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    // Bob's posts (author_id = 2)
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(200),
            Value::Text("Bob Post".into()),
            Value::Integer(2),
        ],
    )
    .unwrap();

    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "users.id")
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have updates");

    assert_eq!(delta.added.len(), 2, "Should have 2 users");

    let output_descriptor = users_with_posts_descriptor();

    // Build a map of user_id -> posts for verification
    let mut user_posts: std::collections::HashMap<i32, Vec<i32>> = std::collections::HashMap::new();
    for row in &delta.added {
        let values = decode_row(&output_descriptor, &row.data).expect("decode");
        let user_id = match &values[0] {
            Value::Integer(id) => id,
            _ => panic!("User id should be integer"),
        };
        let posts = values[2].as_array().expect("posts array");
        let post_ids: Vec<i32> = posts
            .iter()
            .filter_map(|p| {
                let row_vals = p.as_row()?;
                match &row_vals[0] {
                    Value::Integer(id) => Some(*id),
                    _ => None,
                }
            })
            .collect();
        user_posts.insert(*user_id, post_ids);
    }

    // Alice (id=1) should have post 100
    assert_eq!(
        user_posts.get(&1),
        Some(&vec![100]),
        "Alice should have post 100"
    );

    // Bob (id=2) should have post 200
    assert_eq!(
        user_posts.get(&2),
        Some(&vec![200]),
        "Bob should have post 200"
    );
}

#[test]
fn array_subquery_delta_on_inner_insert() {
    // Test: after subscription, inserting a new post should emit a delta
    // with the updated user row containing the new post in the array.
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert user Alice
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();

    // Insert initial post
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Post 1".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    // Subscribe to users with posts
    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "users.id")
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    // Consume initial update
    let initial_updates = qm.take_updates();
    let initial_delta = initial_updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have initial update");
    assert_eq!(initial_delta.added.len(), 1, "Initial: 1 user");

    // Verify initial state: Alice has 1 post
    let output_descriptor = users_with_posts_descriptor();
    let initial_values =
        decode_row(&output_descriptor, &initial_delta.added[0].data).expect("decode initial");
    let initial_posts = initial_values[2].as_array().expect("posts array");
    assert_eq!(initial_posts.len(), 1, "Initially Alice has 1 post");

    // NOW: Insert a new post for Alice
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(101),
            Value::Text("Post 2".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.process(&mut storage);

    // Check delta after inner insert
    let updates_after_insert = qm.take_updates();
    let delta_after = updates_after_insert
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have delta after post insert");

    // Should have an update (old row removed, new row with updated array added)
    // or just updated entries
    let total_changes = delta_after.added.len() + delta_after.updated.len();
    assert!(
        total_changes > 0,
        "Should have changes after inserting post"
    );

    // Find the new state - either in added or as new part of updated
    let new_row_data = if !delta_after.added.is_empty() {
        &delta_after.added[0].data
    } else if !delta_after.updated.is_empty() {
        &delta_after.updated[0].1.data
    } else {
        panic!("Expected added or updated row");
    };

    let new_values = decode_row(&output_descriptor, new_row_data).expect("decode new");
    let new_posts = new_values[2].as_array().expect("posts array");
    assert_eq!(
        new_posts.len(),
        2,
        "After insert, Alice should have 2 posts"
    );

    // Verify both post IDs are present
    let post_ids: Vec<i32> = new_posts
        .iter()
        .filter_map(|p| match &p.as_row()?[0] {
            Value::Integer(id) => Some(*id),
            _ => None,
        })
        .collect();
    assert!(post_ids.contains(&100), "Should contain post 100");
    assert!(post_ids.contains(&101), "Should contain post 101");
}

#[test]
fn array_subquery_reevaluate_all_does_not_corrupt_base_columns() {
    // When the inner table changes after subscription, reevaluate_all re-builds
    // the output tuple from the stored current_tuple.  Those stored tuples are
    // encoded with the *combined* descriptor (base cols + array col), but
    // build_output_tuple was decoding them with the *base* descriptor only,
    // causing the variable-length offset table to be misread and garbling
    // base column values on every subsequent fire.
    //
    // ASCII flow:
    //
    //   insert Alice (no posts)
    //   subscribe → [Alice, []] Added   ← outer tuple encoded with base desc (correct)
    //   insert Post → reevaluate_all    ← old_tuple now encoded with combined desc
    //                → [???, [Post]] Updated  ← name garbled before fix

    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert Alice with NO posts — ensures the stored current_tuple after the
    // first fire carries an empty-array combined encoding.
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();

    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "users.id")
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    // Consume and verify the initial Added delta.
    let initial_updates = qm.take_updates();
    let initial_delta = initial_updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have initial update");
    assert_eq!(initial_delta.added.len(), 1, "Initial: 1 user row Added");

    let output_descriptor = users_with_posts_descriptor();
    let initial_values =
        decode_row(&output_descriptor, &initial_delta.added[0].data).expect("decode initial row");
    assert_eq!(
        initial_values[0],
        Value::Integer(1),
        "Initial: id must be 1"
    );
    assert_eq!(
        initial_values[1],
        Value::Text("Alice".into()),
        "Initial: name must be Alice"
    );
    assert_eq!(
        initial_values[2].as_array().expect("posts array").len(),
        0,
        "Initial: posts must be empty"
    );

    // Now insert a post — this triggers reevaluate_all on the ArraySubqueryNode.
    // The old_tuple in current_tuples is the combined-descriptor-encoded row
    // from the initial fire.  Before the fix, build_output_tuple decodes it
    // with the base descriptor, corrupting the variable-length name field.
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("First Post".into()),
            Value::Integer(1), // author_id = Alice
        ],
    )
    .unwrap();
    qm.process(&mut storage);

    let second_updates = qm.take_updates();
    let second_delta = second_updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have delta after post insert");

    assert!(
        !second_delta.updated.is_empty() || !second_delta.added.is_empty(),
        "Expected an Updated or Added row after inner insert"
    );

    let new_row_data = if !second_delta.updated.is_empty() {
        &second_delta.updated[0].1.data
    } else {
        &second_delta.added[0].data
    };

    let new_values = decode_row(&output_descriptor, new_row_data).expect("decode updated row");

    // These are the assertions that expose the bug: before the fix, the
    // variable-length offset table mismatch causes id and name to be garbled.
    assert_eq!(
        new_values[0],
        Value::Integer(1),
        "After inner insert: id must not be corrupted"
    );
    assert_eq!(
        new_values[1],
        Value::Text("Alice".into()),
        "After inner insert: name must not be corrupted"
    );
    assert_eq!(
        new_values[2].as_array().expect("posts array").len(),
        1,
        "After inner insert: posts array must contain 1 post"
    );
}

#[test]
fn array_subquery_delta_on_outer_insert() {
    // Test: after subscription, inserting a new user should emit a delta
    // with the new user row (with their posts array).
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert user Alice with a post
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Alice Post".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    // Also insert a post for Bob (who doesn't exist yet)
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(200),
            Value::Text("Bob Post".into()),
            Value::Integer(2),
        ],
    )
    .unwrap();

    // Subscribe
    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "users.id")
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    // Consume initial update (just Alice)
    let initial_updates = qm.take_updates();
    let initial_delta = initial_updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have initial update");
    assert_eq!(initial_delta.added.len(), 1, "Initial: only Alice");

    // NOW: Insert Bob
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(2), Value::Text("Bob".into())],
    )
    .unwrap();
    qm.process(&mut storage);

    // Check delta after outer insert
    let updates_after = qm.take_updates();
    let delta_after = updates_after
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have delta after user insert");

    // Should have Bob added
    assert_eq!(delta_after.added.len(), 1, "Bob should be added");

    let output_descriptor = users_with_posts_descriptor();
    let bob_values =
        decode_row(&output_descriptor, &delta_after.added[0].data).expect("decode Bob");

    assert_eq!(bob_values[0], Value::Integer(2), "Should be Bob (id=2)");
    assert_eq!(
        bob_values[1],
        Value::Text("Bob".into()),
        "Name should be Bob"
    );

    // Bob should have his post (id=200)
    let bob_posts = bob_values[2].as_array().expect("posts array");
    assert_eq!(bob_posts.len(), 1, "Bob should have 1 post");

    let post_row = bob_posts[0].as_row().expect("post should be Row");
    assert!(
        bob_posts[0].row_id().is_some(),
        "post Row should have an id"
    );
    assert_eq!(
        post_row[0],
        Value::Integer(200),
        "Bob's post should be id=200"
    );
}

#[test]
fn array_subquery_with_order_by() {
    // Test: posts should be ordered by id descending
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert user
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();

    // Insert posts in random order
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(102),
            Value::Text("Middle".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("First".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(101),
            Value::Text("Last".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    // Query with order_by_desc on id
    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts")
                .correlate("author_id", "users.id")
                .order_by_desc("id")
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have update");

    let output_descriptor = users_with_posts_descriptor();
    let values = decode_row(&output_descriptor, &delta.added[0].data).expect("decode");
    let posts = values[2].as_array().expect("posts array");

    assert_eq!(posts.len(), 3, "Should have 3 posts");

    // Verify order: should be 102, 101, 100 (descending by id)
    let post_ids: Vec<i32> = posts
        .iter()
        .filter_map(|p| match &p.as_row()?[0] {
            Value::Integer(id) => Some(*id),
            _ => None,
        })
        .collect();
    assert_eq!(
        post_ids,
        vec![102, 101, 100],
        "Posts should be ordered by id desc"
    );
}

#[test]
fn array_subquery_with_limit() {
    // Test: limit should restrict number of posts returned
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert user
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();

    // Insert 5 posts
    for i in 100..105 {
        qm.insert(
            &mut storage,
            "posts",
            &[
                Value::Integer(i),
                Value::Text(format!("Post {}", i)),
                Value::Integer(1),
            ],
        )
        .unwrap();
    }

    // Query with limit 2
    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts")
                .correlate("author_id", "users.id")
                .order_by("id")
                .limit(2)
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have update");

    let output_descriptor = users_with_posts_descriptor();
    let values = decode_row(&output_descriptor, &delta.added[0].data).expect("decode");
    let posts = values[2].as_array().expect("posts array");

    assert_eq!(posts.len(), 2, "Limit should restrict to 2 posts");

    // Verify first 2 posts by id ascending
    let post_ids: Vec<i32> = posts
        .iter()
        .filter_map(|p| match &p.as_row()?[0] {
            Value::Integer(id) => Some(*id),
            _ => None,
        })
        .collect();
    assert_eq!(post_ids, vec![100, 101], "Should get first 2 posts by id");
}

#[test]
fn array_subquery_with_select_columns() {
    // Test: select specific columns from inner query
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert user and post
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Post Title".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    // Query selecting only id and title (not author_id)
    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts")
                .correlate("author_id", "users.id")
                .select(&["id", "title"])
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have update");

    // Build descriptor for selected columns only
    // Row id is in Value::Row { id, .. }, not as a column
    let posts_row_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("title", ColumnType::Text),
    ]);
    let output_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new(
            "posts",
            ColumnType::Array {
                element: Box::new(ColumnType::Row {
                    columns: Box::new(posts_row_desc),
                }),
            },
        ),
    ]);

    let values = decode_row(&output_descriptor, &delta.added[0].data).expect("decode");
    let posts = values[2].as_array().expect("posts array");

    assert_eq!(posts.len(), 1, "Should have 1 post");

    let post_row = posts[0].as_row().expect("post Row");
    assert_eq!(post_row.len(), 2, "Post should have 2 columns (id, title)");
    assert!(posts[0].row_id().is_some(), "post Row should have an id");
    assert_eq!(post_row[0], Value::Integer(100));
    assert_eq!(post_row[1], Value::Text("Post Title".into()));
}

#[test]
fn array_subquery_with_magic_timestamp_select_columns() {
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Post Title".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts")
                .correlate("author_id", "users.id")
                .select(&["id", "title", "$createdAt", "$updatedAt"])
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have update");

    let posts_row_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("$createdAt", ColumnType::Timestamp),
        ColumnDescriptor::new("$updatedAt", ColumnType::Timestamp),
    ]);
    let output_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new(
            "posts",
            ColumnType::Array {
                element: Box::new(ColumnType::Row {
                    columns: Box::new(posts_row_desc),
                }),
            },
        ),
    ]);

    let values = decode_row(&output_descriptor, &delta.added[0].data).expect("decode");
    let posts = values[2].as_array().expect("posts array");

    assert_eq!(posts.len(), 1, "Should have 1 post");
    let post_row = posts[0].as_row().expect("post Row");
    assert_eq!(
        post_row.len(),
        4,
        "Post should include selected magic columns"
    );
    assert_eq!(post_row[0], Value::Integer(100));
    assert_eq!(post_row[1], Value::Text("Post Title".into()));
    assert!(matches!(post_row[2], Value::Timestamp(_)));
    assert!(matches!(post_row[3], Value::Timestamp(_)));
}

#[test]
fn array_subquery_nested() {
    // Test: nested array subqueries
    // users with_array(posts with_array(comments))
    let sync_manager = SyncManager::new();
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("users"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
        ])
        .into(),
    );
    schema.insert(
        TableName::new("posts"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("author_id", ColumnType::Integer),
        ])
        .into(),
    );
    schema.insert(
        TableName::new("comments"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("text", ColumnType::Text),
            ColumnDescriptor::new("post_id", ColumnType::Integer),
        ])
        .into(),
    );

    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert user
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();

    // Insert posts
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Post A".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(101),
            Value::Text("Post B".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    // Insert comments - 2 on Post A, 1 on Post B
    qm.insert(
        &mut storage,
        "comments",
        &[
            Value::Integer(1000),
            Value::Text("Comment 1 on A".into()),
            Value::Integer(100),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "comments",
        &[
            Value::Integer(1001),
            Value::Text("Comment 2 on A".into()),
            Value::Integer(100),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "comments",
        &[
            Value::Integer(1002),
            Value::Text("Comment on B".into()),
            Value::Integer(101),
        ],
    )
    .unwrap();

    // Query: users with posts, where each post has its comments
    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts")
                .correlate("author_id", "users.id")
                .with_array("comments", |sub2| {
                    sub2.from("comments").correlate("post_id", "posts.id")
                })
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have update");

    // Build nested descriptor matching runtime output:
    // comments row: [id, text, post_id] (row id in Value::Row { id, .. })
    let comments_row_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("comment_id", ColumnType::Integer),
        ColumnDescriptor::new("text", ColumnType::Text),
        ColumnDescriptor::new("post_id", ColumnType::Integer),
    ]);
    // posts row with comments array: [id, title, author_id, comments[]]
    let posts_row_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("post_id", ColumnType::Integer),
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("author_id", ColumnType::Integer),
        ColumnDescriptor::new(
            "comments",
            ColumnType::Array {
                element: Box::new(ColumnType::Row {
                    columns: Box::new(comments_row_desc),
                }),
            },
        ),
    ]);
    // users row with posts array: [id, name, posts[]]
    let output_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new(
            "posts",
            ColumnType::Array {
                element: Box::new(ColumnType::Row {
                    columns: Box::new(posts_row_desc),
                }),
            },
        ),
    ]);

    assert_eq!(delta.added.len(), 1, "Should have 1 user");
    let values = decode_row(&output_descriptor, &delta.added[0].data).expect("decode");
    assert_eq!(values[0], Value::Integer(1)); // user id
    assert_eq!(values[1], Value::Text("Alice".into())); // user name

    let posts = values[2].as_array().expect("posts array");
    assert_eq!(posts.len(), 2, "Alice should have 2 posts");

    // Check each post has its comments
    for post in posts {
        let post_row = post.as_row().expect("post row");
        assert_eq!(
            post_row.len(),
            4,
            "Post should have 4 columns (id, title, author_id, comments)"
        );
        assert!(post.row_id().is_some(), "post Row should have an id");

        let post_id = match &post_row[0] {
            Value::Integer(id) => id,
            _ => panic!("Expected integer for post id"),
        };

        let comments = post_row[3].as_array().expect("comments array");

        if *post_id == 100 {
            // Post A has 2 comments
            assert_eq!(comments.len(), 2, "Post A should have 2 comments");
            for comment in comments {
                let comment_row = comment.as_row().expect("comment row");
                // comment_row: [id:Int, text:Text, post_id:Int]
                assert!(comment.row_id().is_some(), "comment should have an id");
                assert_eq!(comment_row[2], Value::Integer(100)); // post_id
            }
        } else if *post_id == 101 {
            // Post B has 1 comment
            assert_eq!(comments.len(), 1, "Post B should have 1 comment");
            let comment_row = comments[0].as_row().expect("comment row");
            assert!(comments[0].row_id().is_some(), "comment should have an id");
            assert_eq!(comment_row[2], Value::Integer(101)); // post_id
        } else {
            panic!("Unexpected post id: {}", post_id);
        }
    }
}

#[test]
fn array_subquery_multiple_columns() {
    // Test: two separate (non-nested) array subquery columns
    // users with posts[] and with comments[] (comments directly on user)
    let sync_manager = SyncManager::new();
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("users"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
        ])
        .into(),
    );
    schema.insert(
        TableName::new("posts"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("author_id", ColumnType::Integer),
        ])
        .into(),
    );
    schema.insert(
        TableName::new("comments"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("text", ColumnType::Text),
            ColumnDescriptor::new("user_id", ColumnType::Integer),
        ])
        .into(),
    );

    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert users
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(2), Value::Text("Bob".into())],
    )
    .unwrap();

    // Insert posts - Alice has 2, Bob has 1
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Alice Post 1".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(101),
            Value::Text("Alice Post 2".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(102),
            Value::Text("Bob Post".into()),
            Value::Integer(2),
        ],
    )
    .unwrap();

    // Insert comments (directly on users) - Alice has 1, Bob has 2
    qm.insert(
        &mut storage,
        "comments",
        &[
            Value::Integer(1000),
            Value::Text("Alice comment".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "comments",
        &[
            Value::Integer(1001),
            Value::Text("Bob comment 1".into()),
            Value::Integer(2),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "comments",
        &[
            Value::Integer(1002),
            Value::Text("Bob comment 2".into()),
            Value::Integer(2),
        ],
    )
    .unwrap();

    // Query: users with both posts[] and comments[]
    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "users.id")
        })
        .with_array("comments", |sub| {
            sub.from("comments").correlate("user_id", "users.id")
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have update");

    // Build descriptor: users + posts[] + comments[]
    // Row ids are in Value::Row { id, .. }, not as columns
    let posts_row_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("post_id", ColumnType::Integer),
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("author_id", ColumnType::Integer),
    ]);
    let comments_row_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("comment_id", ColumnType::Integer),
        ColumnDescriptor::new("text", ColumnType::Text),
        ColumnDescriptor::new("user_id", ColumnType::Integer),
    ]);
    let output_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new(
            "posts",
            ColumnType::Array {
                element: Box::new(ColumnType::Row {
                    columns: Box::new(posts_row_desc),
                }),
            },
        ),
        ColumnDescriptor::new(
            "comments",
            ColumnType::Array {
                element: Box::new(ColumnType::Row {
                    columns: Box::new(comments_row_desc),
                }),
            },
        ),
    ]);

    assert_eq!(delta.added.len(), 2, "Should have 2 users");

    // Decode and verify each user
    for row in &delta.added {
        let values = decode_row(&output_descriptor, &row.data).expect("decode");
        let user_id = match &values[0] {
            Value::Integer(id) => id,
            _ => panic!("Expected integer for user id"),
        };

        let posts = values[2].as_array().expect("posts array");
        let comments = values[3].as_array().expect("comments array");

        if *user_id == 1 {
            // Alice: 2 posts, 1 comment
            assert_eq!(values[1], Value::Text("Alice".into()));
            assert_eq!(posts.len(), 2, "Alice should have 2 posts");
            assert_eq!(comments.len(), 1, "Alice should have 1 comment");
        } else if *user_id == 2 {
            // Bob: 1 post, 2 comments
            assert_eq!(values[1], Value::Text("Bob".into()));
            assert_eq!(posts.len(), 1, "Bob should have 1 post");
            assert_eq!(comments.len(), 2, "Bob should have 2 comments");
        } else {
            panic!("Unexpected user id: {}", user_id);
        }
    }
}

#[test]
fn contributing_ids_for_array_subquery_include_inner_rows() {
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let user = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Integer(1), Value::Text("Alice".into())],
        )
        .unwrap();
    let post1 = qm
        .insert(
            &mut storage,
            "posts",
            &[
                Value::Integer(100),
                Value::Text("Post 1".into()),
                Value::Integer(1),
            ],
        )
        .unwrap();
    let post2 = qm
        .insert(
            &mut storage,
            "posts",
            &[
                Value::Integer(101),
                Value::Text("Post 2".into()),
                Value::Integer(1),
            ],
        )
        .unwrap();

    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "users.id")
        })
        .build();
    let sub_id = qm.subscribe(query).unwrap();

    qm.process(&mut storage);

    let branch = crate::object::BranchName::new(get_branch(&qm));
    let contributing = qm.get_subscription_contributing_ids(sub_id);

    assert_eq!(
        contributing.len(),
        3,
        "Array subquery outputs depend on both outer and inner rows"
    );
    assert!(contributing.contains(&(user.row_id, branch)));
    assert!(contributing.contains(&(post1.row_id, branch)));
    assert!(contributing.contains(&(post2.row_id, branch)));
}

#[test]
fn e2e_client_receives_array_subquery_server_data_via_subscription() {
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let schema = users_posts_schema();

    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());

    server
        .insert(
            &mut server_io,
            "users",
            &[Value::Integer(1), Value::Text("Alice".into())],
        )
        .unwrap();
    server
        .insert(
            &mut server_io,
            "posts",
            &[
                Value::Integer(100),
                Value::Text("Post 1".into()),
                Value::Integer(1),
            ],
        )
        .unwrap();
    server
        .insert(
            &mut server_io,
            "posts",
            &[
                Value::Integer(101),
                Value::Text("Post 2".into()),
                Value::Integer(1),
            ],
        )
        .unwrap();
    server.process(&mut server_io);

    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema);

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));

    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    let _ = client.sync_manager_mut().take_outbox();

    let query = client
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "users.id")
        })
        .build();

    let sub_id = client.subscribe_with_sync(query, None, None).unwrap();

    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    let results = client.get_subscription_results(sub_id);
    assert_eq!(results.len(), 1, "Client should receive the user row");
    assert_eq!(results[0].1[0], Value::Integer(1));
    assert_eq!(results[0].1[1], Value::Text("Alice".into()));

    let posts = results[0].1[2].as_array().expect("posts array");
    assert_eq!(
        posts.len(),
        2,
        "Client should receive inner rows needed for the array"
    );
}
