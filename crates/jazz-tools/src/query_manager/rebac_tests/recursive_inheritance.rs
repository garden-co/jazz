use super::*;

#[test]
fn rebac_recursive_inherits_allows_ancestor_access() {
    use crate::query_manager::query::QueryBuilder;

    let schema = recursive_folders_schema(None);
    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    let root = qm
        .insert(
            &mut storage,
            "folders",
            &[
                Value::Text("alice".into()),
                Value::Text("Root".into()),
                Value::Null,
            ],
        )
        .unwrap()
        .row_id;
    let child = qm
        .insert(
            &mut storage,
            "folders",
            &[
                Value::Text("bob".into()),
                Value::Text("Child".into()),
                Value::Uuid(root),
            ],
        )
        .unwrap()
        .row_id;
    let grand = qm
        .insert(
            &mut storage,
            "folders",
            &[
                Value::Text("carol".into()),
                Value::Text("Grandchild".into()),
                Value::Uuid(child),
            ],
        )
        .unwrap()
        .row_id;

    let sub_id = qm
        .subscribe_with_session(
            QueryBuilder::new("folders").build(),
            Some(Session::new("alice")),
            None,
        )
        .unwrap();

    for _ in 0..10 {
        qm.process(&mut storage);
    }

    let result_ids: HashSet<_> = qm
        .get_subscription_results(sub_id)
        .into_iter()
        .map(|(id, _)| id)
        .collect();

    assert!(result_ids.contains(&root), "Root should be visible");
    assert!(
        result_ids.contains(&child),
        "Child should be visible via recursive INHERITS"
    );
    assert!(
        result_ids.contains(&grand),
        "Grandchild should be visible via recursive INHERITS"
    );
}

#[test]
fn rebac_recursive_inherits_respects_depth_override() {
    use crate::query_manager::query::QueryBuilder;

    let schema = recursive_folders_schema(Some(1));
    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    let root = qm
        .insert(
            &mut storage,
            "folders",
            &[
                Value::Text("alice".into()),
                Value::Text("Root".into()),
                Value::Null,
            ],
        )
        .unwrap()
        .row_id;
    let child = qm
        .insert(
            &mut storage,
            "folders",
            &[
                Value::Text("bob".into()),
                Value::Text("Child".into()),
                Value::Uuid(root),
            ],
        )
        .unwrap()
        .row_id;
    let grand = qm
        .insert(
            &mut storage,
            "folders",
            &[
                Value::Text("carol".into()),
                Value::Text("Grandchild".into()),
                Value::Uuid(child),
            ],
        )
        .unwrap()
        .row_id;

    let sub_id = qm
        .subscribe_with_session(
            QueryBuilder::new("folders").build(),
            Some(Session::new("alice")),
            None,
        )
        .unwrap();

    for _ in 0..10 {
        qm.process(&mut storage);
    }

    let result_ids: HashSet<_> = qm
        .get_subscription_results(sub_id)
        .into_iter()
        .map(|(id, _)| id)
        .collect();

    assert!(result_ids.contains(&root), "Root should be visible");
    assert!(
        result_ids.contains(&child),
        "Child should be visible at depth=1"
    );
    assert!(
        !result_ids.contains(&grand),
        "Grandchild should be hidden when max_depth=1"
    );
}

#[test]
fn rebac_recursive_inherits_write_checks_allow_and_deny() {
    let (denied_shallow, applied_shallow) = run_recursive_folder_update(Some(1));
    assert!(
        denied_shallow,
        "Update should be denied when recursive INHERITS max depth is too shallow"
    );
    assert!(
        !applied_shallow,
        "Denied update must not be applied to the row"
    );

    let (denied_deep, applied_deep) = run_recursive_folder_update(Some(2));
    assert!(
        !denied_deep,
        "Update should be allowed when max depth reaches the ancestor owner"
    );
    assert!(applied_deep, "Allowed update should be applied");
}
