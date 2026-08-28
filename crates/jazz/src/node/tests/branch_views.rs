fn branch_view_schema() -> JazzSchema {
    build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .column("branch_id", PublicColumnType::Uuid)
                    .column("title", PublicColumnType::Text)
                    .column("owner", PublicColumnType::Uuid)
                    .branch_by("branch_id")
                    .policies(public_owner_policies("owner")),
            )
            .table(PublicTableSchemaBuilder::new("users").column("name", PublicColumnType::Text)),
    )
}

fn two_column_branch_view_schema() -> JazzSchema {
    build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("workspace_id", PublicColumnType::Uuid)
                .column("branch_id", PublicColumnType::Uuid)
                .column("title", PublicColumnType::Text)
                .column("owner", PublicColumnType::Uuid)
                .branch_by("workspace_id")
                .branch_by("branch_id")
                .policies(public_owner_policies("owner")),
        ),
    )
}

fn branch_selector(byte: u8) -> BranchSelector {
    BranchSelector::new([("branch_id", Value::Uuid(uuid::Uuid::from_bytes([byte; 16])))])
}

#[test]
fn known_history_parent_must_match_exact_branch_for_local_and_replicated_versions() {
    let schema = branch_view_schema();
    let (_dir, mut core) =
        open_history_complete_node_with_schema(NodeUuid::from_bytes([0x40; 16]), schema.clone());
    let owner = AuthorSubject::for_test_bytes([0x41; 16]);
    core.set_session_claims(
        owner,
        BTreeMap::from([("sub".to_owned(), Value::Uuid(owner.test_uuid()))]),
    );
    let row_uuid = row(0x42);
    let first_branch = branch_selector(0x43);
    let second_branch = branch_selector(0x44);
    let cells = |title| {
        BTreeMap::from([
            ("title".to_owned(), v(title)),
            ("owner".to_owned(), Value::Uuid(owner.test_uuid())),
        ])
    };
    let parent = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 10)
                .branch(first_branch.clone())
                .cells(cells("parent")),
        )
        .unwrap();

    assert!(matches!(
        core.commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 11)
                .branch(second_branch.clone())
                .parents(vec![parent])
                .cells(cells("wrong local branch")),
        ),
        Err(Error::InvalidMergeableCommit(
            "version parent does not resolve to the same physical row, branch, and layer"
        ))
    ));

    let table = &schema.tables[0];
    let (second_key, branch_cells) = schema
        .project_branch_selector(table, &second_branch)
        .expect("canonical second branch");
    let mut remote_cells = branch_cells;
    remote_cells.extend(cells("wrong replicated branch"));
    let remote = VersionRecord::from_cells(
        table,
        schema.version_id(),
        row_uuid,
        vec![parent],
        owner,
        12,
        owner,
        12,
        &remote_cells,
        None,
    )
    .unwrap()
    .with_branch_key(second_key);
    let error = core
        .ingest_known_transaction(
            Transaction {
                tx_id: TxId::new(TxTime::from(12), node(0x45)),
                kind: TxKind::Mergeable,
                n_total_writes: 1,
                made_by: owner,
                permission_subject: None,
                base_snapshot: None,
                row_read_set: None,
                absent_read_set: None,
                predicate_read_set: None,
                user_metadata_json: None,
                contribution_merge: None,
            },
            vec![remote],
            Fate::Accepted,
            None,
            DurabilityTier::Edge,
        )
        .unwrap_err();
    assert!(matches!(
        error,
        Error::InvalidMergeableCommit(
            "version parent does not resolve to the same physical row, branch, and layer"
        )
    ));
}

#[test]
fn branch_view_selects_head_then_base_and_keeps_unbranched_tables_shared() {
    let schema = branch_view_schema();
    let (_dir, mut node) =
        open_history_complete_node_with_schema(NodeUuid::from_bytes([0x42; 16]), schema.clone());
    let inherited = row(0x43);
    let overridden = row(0x44);
    let base = branch_selector(0x45);
    let head = branch_selector(0x46);
    let owner = AuthorSubject::for_test_bytes([0x48; 16]);
    node.set_test_provider_claims(
        owner,
        BTreeMap::from([("sub".to_owned(), Value::Uuid(owner.test_uuid()))]),
    );

    for (row_uuid, title) in [(inherited, "inherited"), (overridden, "base")] {
        node.commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 10)
                .branch(base.clone())
                .cells(BTreeMap::from([
                    ("title".to_owned(), v(title)),
                    ("owner".to_owned(), Value::Uuid(owner.test_uuid())),
                ])),
        )
        .unwrap();
    }
    node.commit_mergeable_settled(
        MergeableCommit::new("todos", overridden, 20)
            .branch(head.clone())
            .cells(BTreeMap::from([
                ("title".to_owned(), v("head")),
                ("owner".to_owned(), Value::Uuid(owner.test_uuid())),
            ])),
    )
    .unwrap();
    let shared = row(0x47);
    node.commit_mergeable_settled(
        MergeableCommit::new("users", shared, 30)
            .cells(BTreeMap::from([("name".to_owned(), v("shared"))])),
    )
    .unwrap();

    let read_view = crate::protocol::ReadViewSpec {
        source: crate::protocol::ReadViewSourceSpec::BranchView {
            head: head.clone(),
            base: Some(crate::protocol::BranchViewBase::Current(base)),
        },
    };
    let shape = Query::from("todos").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let snapshot = node
        .query_relation_snapshot_for_serving_in_read_view(
            &shape,
            &binding,
            DurabilityTier::Local,
            AuthorSubject::SYSTEM,
            &read_view,
        )
        .unwrap();
    let todos_table = schema.tables.iter().find(|table| table.name == "todos").unwrap();
    let titles = snapshot
        .rows
        .iter()
        .take(snapshot.root_count)
        .map(|row| {
            (
                row.row_uuid(),
                match row.cell(todos_table, "title").unwrap() {
                    Value::String(title) => title,
                    other => panic!("unexpected title value: {other:?}"),
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(titles[&inherited], "inherited");
    assert_eq!(titles[&overridden], "head");
    for row in snapshot.rows.iter().take(snapshot.root_count) {
        assert_eq!(row.cell(todos_table, "owner"), Some(Value::Uuid(owner.test_uuid())));
    }

    let authorized = node
        .query_relation_snapshot_for_serving_in_read_view(
            &shape,
            &binding,
            DurabilityTier::Local,
            owner,
            &read_view,
        )
        .unwrap();
    assert_eq!(authorized.root_count, 2);
    let denied = node
        .query_relation_snapshot_for_serving_in_read_view(
            &shape,
            &binding,
            DurabilityTier::Local,
            AuthorSubject::for_test_bytes([0x49; 16]),
            &read_view,
        )
        .unwrap();
    assert_eq!(denied.root_count, 0);

    let shared_default = node
        .query_relation_snapshot_for_serving(
            &shape,
            &binding,
            DurabilityTier::Local,
            AuthorSubject::SYSTEM,
        )
        .unwrap();
    assert_eq!(
        shared_default.root_count, 0,
        "ordinary reads address the empty shared branch key"
    );

    let users = Query::from("users").validate(&schema).unwrap();
    let users_binding = users.bind(BTreeMap::new()).unwrap();
    let shared_snapshot = node
        .query_relation_snapshot_for_serving_in_read_view(
            &users,
            &users_binding,
            DurabilityTier::Local,
            AuthorSubject::SYSTEM,
            &read_view,
        )
        .unwrap();
    assert_eq!(shared_snapshot.root_count, 1);
    assert_eq!(shared_snapshot.rows[0].row_uuid(), shared);

    node.commit_mergeable_settled(
        MergeableCommit::new("todos", overridden, 40)
            .branch(head)
            .deletion(DeletionEvent::Deleted),
    )
    .unwrap();
    let after_delete = node
        .query_relation_snapshot_for_serving_in_read_view(
            &shape,
            &binding,
            DurabilityTier::Local,
            AuthorSubject::SYSTEM,
            &read_view,
        )
        .unwrap();
    assert_eq!(after_delete.root_count, 1);
    assert_eq!(after_delete.rows[0].row_uuid(), inherited);
}

#[test]
/// Frozen-base lowering keeps the base content and deletion registers separate.
/// This internal test is needed because it verifies the maintained graph's
/// frozen input and its live head fate transition in one evaluation boundary.
///
/// alice writes and deletes base content at the snapshot, then the head's
/// `Restored` winner reveals that frozen content without a head content write.
fn frozen_base_deleted_row_reappears_after_head_deletion_is_restored() {
    let schema = branch_view_schema();
    let node_id = NodeUuid::from_bytes([0x4a; 16]);
    let (_dir, mut node) = open_history_complete_node_with_schema(node_id, schema.clone());
    let row_uuid = row(0x4b);
    let base = branch_selector(0x4c);
    let head = branch_selector(0x4d);
    let owner = AuthorSubject::for_test_bytes([0x4e; 16]);
    let base_tx = node
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 10)
                .branch(base.clone())
                .cells(BTreeMap::from([
                    ("title".to_owned(), v("frozen base")),
                    ("owner".to_owned(), Value::Uuid(owner.test_uuid())),
                ])),
        )
        .unwrap();
    let base_delete = node
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 15)
                .branch(base.clone())
                .parents(vec![base_tx])
                .deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("todos", row_uuid, 20)
            .branch(head.clone())
            .deletion(DeletionEvent::Deleted),
    )
    .unwrap();

    let read_view = crate::protocol::ReadViewSpec::branch_view(
        head.clone(),
        Some(crate::protocol::BranchViewBase::snapshot(
            base.clone(),
            crate::protocol::SnapshotRef {
                owner: node_id,
                global_base: GlobalTime(0),
                local_base: base_delete.time,
                dots: Vec::new(),
            },
        )),
    );
    let shape = Query::from("todos").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let (shape, binding, plan) = node
        .prepare_query_binding_for_link_in_authorization_mode(
            &shape,
            &binding,
            DurabilityTier::Local,
            AuthorSubject::SYSTEM,
            QueryAuthorizationMode::ClientLocal,
        )
        .unwrap();
    let (mut maintained, initial) = node
        .open_maintained_view_subscription_in_authorization_mode(
            &shape,
            &binding,
            AuthorSubject::SYSTEM,
            DurabilityTier::Local,
            &read_view,
            Some(plan),
            QueryAuthorizationMode::ClientLocal,
        )
        .unwrap();
    assert_eq!(initial.root_count, 0);

    node.commit_mergeable_settled(
        MergeableCommit::new("todos", row_uuid, 30)
            .branch(head)
            .deletion(DeletionEvent::Restored),
    )
    .unwrap();
    let fresh = node
        .query_relation_snapshot_for_serving_in_read_view(
            &shape,
            &binding,
            DurabilityTier::Local,
            AuthorSubject::SYSTEM,
            &read_view,
        )
        .unwrap();
    assert_eq!(fresh.root_count, 1, "fresh evaluation must see the restoration");
    let update = node
        .drain_local_maintained_view_subscription(&mut maintained, None)
        .unwrap()
        .expect("restoration must publish the frozen base row");

    let LocalMaintainedViewSubscriptionUpdate::Flat { added, removed, .. } = update else {
        panic!("flat branch query produced a structured maintained update");
    };
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].1.row_uuid(), row_uuid);
    assert!(removed.is_empty());
}

#[test]
fn frozen_base_subscription_does_not_capture_pending_head_content() {
    let schema = branch_view_schema();
    let node_id = NodeUuid::from_bytes([0x3a; 16]);
    let (_dir, mut node) = open_history_complete_node_with_schema(node_id, schema.clone());
    let row_uuid = row(0x3b);
    let base = branch_selector(0x3c);
    let head = branch_selector(0x3d);
    let base_tx = node
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 10)
                .branch(base.clone())
                .cells(BTreeMap::from([
                    ("title".to_owned(), v("frozen base")),
                    ("owner".to_owned(), Value::Uuid(uuid::Uuid::nil())),
                ])),
        )
        .unwrap();
    let (pending, _) = node
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row_uuid, 20)
                .branch(head.clone())
                .cells(BTreeMap::from([
                    ("title".to_owned(), v("pending head")),
                    ("owner".to_owned(), Value::Uuid(uuid::Uuid::nil())),
                ])),
        )
        .unwrap();
    let read_view = crate::protocol::ReadViewSpec::branch_view(
        head,
        Some(crate::protocol::BranchViewBase::snapshot(
            base.clone(),
            crate::protocol::SnapshotRef {
                owner: node_id,
                global_base: GlobalTime(0),
                local_base: base_tx.time,
                dots: Vec::new(),
            },
        )),
    );
    let shape = Query::from("todos").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let (shape, binding, plan) = node
        .prepare_query_binding_for_link_in_authorization_mode(
            &shape,
            &binding,
            DurabilityTier::Local,
            AuthorSubject::SYSTEM,
            QueryAuthorizationMode::ClientLocal,
        )
        .unwrap();
    let (mut maintained, initial) = node
        .open_maintained_view_subscription_in_authorization_mode(
            &shape,
            &binding,
            AuthorSubject::SYSTEM,
            DurabilityTier::Local,
            &read_view,
            Some(plan),
            QueryAuthorizationMode::ClientLocal,
        )
        .unwrap();
    let table = schema.tables.iter().find(|table| table.name == "todos").unwrap();
    assert_eq!(initial.rows[0].cell(table, "title"), Some(v("pending head")));

    node.apply_sync_message_settled(SyncMessage::FateUpdate {
        tx_id: pending,
        fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
        global_time: None,
        durability: None,
    })
    .unwrap();
    let fresh = node
        .query_relation_snapshot_for_serving_in_read_view(
            &shape,
            &binding,
            DurabilityTier::Local,
            AuthorSubject::SYSTEM,
            &read_view,
        )
        .unwrap();
    assert_eq!(
        fresh.root_count, 1,
        "fresh rejection evaluation must restore the frozen base"
    );
    let update = node
        .drain_local_maintained_view_subscription(&mut maintained, None)
        .unwrap()
        .expect("head rejection must restore the frozen base payload");
    let LocalMaintainedViewSubscriptionUpdate::Flat { added, removed, .. } = update else {
        panic!("flat branch query produced a structured maintained update");
    };
    assert_eq!(removed.len(), 1);
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].1.cell(table, "title"), Some(v("frozen base")));

    node.commit_mergeable_settled(
        MergeableCommit::new("todos", row_uuid, 30)
            .branch(base)
            .parents(vec![base_tx])
            .cells(BTreeMap::from([
                ("title".to_owned(), v("later base")),
                ("owner".to_owned(), Value::Uuid(uuid::Uuid::nil())),
            ])),
    )
    .unwrap();
    assert!(
        node.drain_local_maintained_view_subscription(&mut maintained, None)
            .unwrap()
            .is_none(),
        "later base changes must remain outside the frozen relation"
    );

    node.commit_mergeable_settled(
        MergeableCommit::new("todos", row_uuid, 40)
            .branch(branch_selector(0x3d))
            .cells(BTreeMap::from([
                ("title".to_owned(), v("replacement head")),
                ("owner".to_owned(), Value::Uuid(uuid::Uuid::nil())),
            ])),
    )
    .unwrap();
    let replacement = node
        .drain_local_maintained_view_subscription(&mut maintained, None)
        .unwrap()
        .expect("replacement head content must remain live");
    let LocalMaintainedViewSubscriptionUpdate::Flat { added, removed, .. } = replacement else {
        panic!("flat branch query produced a structured maintained update");
    };
    assert_eq!(removed.len(), 1);
    assert_eq!(added.len(), 1);
    assert_eq!(
        added[0].1.cell(table, "title"),
        Some(v("replacement head"))
    );
}

#[test]
fn version_parents_cannot_cross_branch_keys() {
    let schema = branch_view_schema();
    let (_dir, mut node) =
        open_history_complete_node_with_schema(NodeUuid::from_bytes([0x51; 16]), schema);
    let row_uuid = row(0x52);
    let owner = AuthorSubject::for_test_bytes([0x53; 16]);
    let parent = node
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 10)
                .branch(branch_selector(0x54))
                .cells(BTreeMap::from([
                    ("title".to_owned(), v("base")),
                    ("owner".to_owned(), Value::Uuid(owner.test_uuid())),
                ])),
        )
        .unwrap();
    let error = node
        .commit_mergeable(
            MergeableCommit::new("todos", row_uuid, 20)
                .branch(branch_selector(0x55))
                .parents(vec![parent])
                .cells(BTreeMap::from([
                    ("title".to_owned(), v("invalid")),
                    ("owner".to_owned(), Value::Uuid(owner.test_uuid())),
                ])),
        )
        .resolve()
        .err()
        .expect("cross-branch causal parent is rejected");
    assert!(matches!(error, Error::InvalidMergeableCommit(_)));
}

#[test]
fn parent_validation_scopes_same_table_transactions_to_the_physical_row() {
    let schema = branch_view_schema();
    let (_dir, mut node) =
        open_history_complete_node_with_schema(NodeUuid::from_bytes([0x56; 16]), schema);
    let target = row(0x57);
    let sibling = row(0x58);
    let branch_a = branch_selector(0x59);
    let branch_b = branch_selector(0x5a);
    let owner = AuthorSubject::for_test_bytes([0x5b; 16]);
    let cells = |title: &str| {
        BTreeMap::from([
            ("title".to_owned(), v(title)),
            ("owner".to_owned(), Value::Uuid(owner.test_uuid())),
        ])
    };

    // A same-table multi-row transaction can legitimately contain a parent
    // for the target and an unrelated sibling under another branch.
    let valid_parent = node
        .commit_mergeable_many_settled(vec![
            MergeableCommit::new("todos", target, 10)
                .branch(branch_a.clone())
                .cells(cells("target base")),
            MergeableCommit::new("todos", sibling, 11)
                .branch(branch_b.clone())
                .cells(cells("sibling other branch")),
        ])
        .unwrap();
    let _valid_child = node
        .commit_mergeable_settled(
            MergeableCommit::new("todos", target, 20)
                .branch(branch_a.clone())
                .parents(vec![valid_parent])
                .cells(cells("target child")),
        )
        .unwrap();

    // Content and deletion history are independent. The first deletion starts
    // its own chain; the restore then continues that deletion-register chain.
    let deletion_parent = node
        .commit_mergeable_settled(
            MergeableCommit::new("todos", target, 30)
                .branch(branch_a.clone())
                .deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("todos", target, 40)
            .branch(branch_a.clone())
            .parents(vec![deletion_parent])
            .deletion(DeletionEvent::Restored),
    )
    .unwrap();

    // This transaction contains the target only under branch B, plus a
    // sibling deletion under branch A. A table-only lookup would see branch A
    // and wrongly bless the foreign target parent.
    let mut foreign_parent_commits = vec![
        MergeableCommit::new("todos", target, 50)
            .branch(branch_b)
            .cells(cells("foreign target parent")),
        MergeableCommit::new("todos", sibling, 51)
            .branch(branch_a.clone())
            .deletion(DeletionEvent::Deleted),
    ];
    // The wide same-table batch is the cache-hit and storage-fallback
    // boundary: neither path may materialize these unrelated physical rows.
    foreign_parent_commits.extend((0..128).map(|index| {
        MergeableCommit::new("todos", row(0x80 + index), 52 + u64::from(index))
            .branch(branch_a.clone())
            .cells(cells("unrelated same-table sibling"))
    }));
    let foreign_parent = node
        .commit_mergeable_many_settled(foreign_parent_commits)
        .unwrap();
    reset_parent_version_lookup_materialized_row_count();
    let error = node
        .commit_mergeable(
            MergeableCommit::new("todos", target, 60)
                .branch(branch_a.clone())
                .parents(vec![foreign_parent])
                .cells(cells("must reject foreign target parent")),
        )
        .resolve()
        .err()
        .expect("a sibling under the requested branch cannot validate a foreign target parent");
    assert!(matches!(error, Error::InvalidMergeableCommit(_)));
    assert_eq!(
        parent_version_lookup_materialized_row_count(),
        1,
        "a cache hit must materialize only the foreign target row, not same-table siblings"
    );

    // Force the storage scan path after the same wide transaction. Content
    // history and shared deletion history must discard sibling rows before
    // decoding/materializing them, while still rejecting the foreign target.
    node.invalidate_tx_version_tables_cache(foreign_parent);
    reset_parent_version_lookup_materialized_row_count();
    let error = node
        .commit_mergeable(
            MergeableCommit::new("todos", target, 61)
                .branch(branch_a)
                .parents(vec![foreign_parent])
                .cells(cells("must reject foreign target parent after cache eviction")),
        )
        .resolve()
        .err()
        .expect("a storage scan must reject the foreign target parent");
    assert!(matches!(error, Error::InvalidMergeableCommit(_)));
    assert_eq!(
        parent_version_lookup_materialized_row_count(),
        1,
        "a storage fallback must materialize only the foreign target row"
    );
}

#[test]
fn replicated_parent_validation_scopes_wide_transactions_to_the_physical_row() {
    let schema = branch_view_schema();
    let (_writer_dir, mut writer) =
        open_history_complete_node_with_schema(NodeUuid::from_bytes([0x62; 16]), schema.clone());
    let (_child_writer_dir, mut child_writer) =
        open_history_complete_node_with_schema(NodeUuid::from_bytes([0x63; 16]), schema.clone());
    let (_receiver_dir, mut receiver) =
        open_history_complete_node_with_schema(NodeUuid::from_bytes([0x64; 16]), schema);
    let target = row(0x65);
    let sibling = row(0x66);
    let branch_a = branch_selector(0x67);
    let branch_b = branch_selector(0x68);
    let owner = AuthorSubject::for_test_bytes([0x69; 16]);
    let cells = |title: &str| {
        BTreeMap::from([
            ("title".to_owned(), v(title)),
            ("owner".to_owned(), Value::Uuid(owner.test_uuid())),
        ])
    };

    let mut parent_commits = vec![
        MergeableCommit::new("todos", target, 10)
            .branch(branch_b)
            .cells(cells("foreign target parent")),
        MergeableCommit::new("todos", sibling, 11)
            .branch(branch_a.clone())
            .deletion(DeletionEvent::Deleted),
    ];
    parent_commits.extend((0..128).map(|index| {
        MergeableCommit::new("todos", row(0x80 + index), 12 + u64::from(index))
            .branch(branch_a.clone())
            .cells(cells("unrelated replicated sibling"))
    }));
    let parent = writer.commit_mergeable_many_settled(parent_commits).unwrap();
    receiver
        .apply_sync_message_settled(writer.commit_unit_for(parent).unwrap())
        .unwrap();

    let (_first_child, first_unit) = child_writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", target, 200)
                .branch(branch_a.clone())
                .parents(vec![parent])
                .cells(cells("replicated child cache hit")),
        )
        .unwrap();
    let SyncMessage::CommitUnit {
        tx: first_tx,
        versions: first_versions,
    } = first_unit
    else {
        panic!("commit unit expected");
    };
    reset_parent_version_lookup_materialized_row_count();
    let first_error = receiver
        .ingest_commit_unit_settled(first_tx, first_versions, u64::MAX - SKEW_TOLERANCE_MS)
        .err()
        .expect("a remote target parent under another branch must be rejected");
    assert!(matches!(first_error, Error::InvalidMergeableCommit(_)));
    assert_eq!(
        parent_version_lookup_materialized_row_count(),
        1,
        "replicated cache-hit validation must materialize only the target parent row"
    );

    receiver.invalidate_tx_version_tables_cache(parent);
    let (_second_child, second_unit) = child_writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", target, 201)
                .branch(branch_a)
                .parents(vec![parent])
                .cells(cells("replicated child storage fallback")),
        )
        .unwrap();
    let SyncMessage::CommitUnit {
        tx: second_tx,
        versions: second_versions,
    } = second_unit
    else {
        panic!("commit unit expected");
    };
    reset_parent_version_lookup_materialized_row_count();
    let second_error = receiver
        .ingest_commit_unit_settled(second_tx, second_versions, u64::MAX - SKEW_TOLERANCE_MS)
        .err()
        .expect("a storage fallback must reject the remote foreign target parent");
    assert!(matches!(second_error, Error::InvalidMergeableCommit(_)));
    assert_eq!(
        parent_version_lookup_materialized_row_count(),
        1,
        "replicated storage validation must materialize only the target parent row"
    );
}

#[test]
fn malformed_branch_key_rejects_multi_key_commit_without_residue() {
    let schema = branch_view_schema();
    let (_dir, mut node) =
        open_history_complete_node_with_schema(NodeUuid::from_bytes([0x4a; 16]), schema);
    let valid_row = row(0x4b);
    let invalid_row = row(0x4c);
    let cells = |title: &str| {
        BTreeMap::from([
            ("title".to_owned(), v(title)),
            ("owner".to_owned(), Value::Uuid(uuid::Uuid::nil())),
        ])
    };
    let error = node
        .commit_mergeable_many(vec![
            MergeableCommit::new("todos", valid_row, 10)
                .branch(branch_selector(0x4d))
                .cells(cells("valid")),
            MergeableCommit::new("todos", invalid_row, 10)
                .branch(BranchSelector::default())
                .cells(cells("invalid")),
        ])
        .resolve()
        .err()
        .expect("malformed branch key is rejected");
    assert!(matches!(error, Error::InvalidBranchKey(_)));
    assert!(
        node.visible_current_cells_in_branch("todos", &branch_selector(0x4d), valid_row)
            .unwrap()
            .is_none(),
        "preflight failure must leave no valid sibling residue"
    );
    assert!(node.query_table_versions("todos").unwrap().is_empty());
}

#[test]
fn branch_coordinates_use_one_canonical_prefix_in_memory_and_after_rocks_reopen() {
    let schema = branch_view_schema();
    let branch = branch_selector(0x70);
    let row_uuid = row(0x71);
    let cells = || {
        BTreeMap::from([
            ("title".to_owned(), v("branch receipt")),
            ("owner".to_owned(), Value::Uuid(uuid::Uuid::nil())),
        ])
    };

    // Memory uses the same physical row projections as durable backends. This
    // first receipt catches a writer that only updates one implementation.
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = MemoryStorage::new(&refs).unwrap();
    let mut memory = NodeState::new_history_complete(node(0x70), schema.clone(), storage).unwrap();
    memory
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 10)
                .branch(branch.clone())
                .cells(cells()),
        )
        .unwrap();
    memory
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 20)
                .branch(branch.clone())
                .deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    assert_eq!(
        memory
            .query_row_versions_in_branch("todos", &schema.project_branch_view_selector(&schema.tables[0], &branch).unwrap().0, row_uuid)
            .unwrap()
            .len(),
        2,
        "history and deletion projections share the same exact branch coordinate"
    );

    let (dir, mut rocks) =
        open_history_complete_node_with_schema(NodeUuid::from_bytes([0x72; 16]), schema.clone());
    let content_tx = rocks
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 10)
                .branch(branch.clone())
                .cells(cells()),
        )
        .unwrap();
    let deletion_tx = rocks
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 20)
                .branch(branch.clone())
                .deletion(DeletionEvent::Deleted),
        )
        .unwrap();

    let key = schema
        .project_branch_view_selector(&schema.tables[0], &branch)
        .unwrap()
        .0;
    let table_id = rocks.catalogue.physical_mappings[&schema.version_id()].tables["todos"].table_id;
    let prefix = vec![Value::Bytes(key.canonical_bytes())];
    assert_eq!(
        rocks
            .database
            .primary_key_scan_raw(&physical_history_table_name(table_id), &prefix)
            .unwrap()
            .len(),
        1,
        "content history is addressed by the canonical branch prefix"
    );
    assert_eq!(
        rocks
            .database
            .primary_key_scan_raw(
                SHARED_DELETION_HISTORY_TABLE,
                &[Value::Bytes(key.canonical_bytes()), Value::U64(table_id.0)],
            )
            .unwrap()
            .len(),
        1,
        "deletion history is addressed by the same canonical branch prefix"
    );
    assert_eq!(
        rocks
            .database
            .primary_key_scan_raw(&physical_ahead_current_table_name(table_id), &prefix)
            .unwrap()
            .len(),
        1,
        "locally settled content uses the canonical branch prefix in ahead-current"
    );
    assert_eq!(
        rocks
            .database
            .primary_key_scan_raw(
                &physical_register_ahead_current_table_name(table_id),
                &prefix,
            )
            .unwrap()
            .len(),
        1,
        "locally settled deletion uses the same prefix in register ahead-current"
    );

    rocks
        .apply_fate_update(
            content_tx,
            Fate::Accepted,
            Some(GlobalTime(1)),
            Some(DurabilityTier::Global),
        )
        .unwrap();
    rocks
        .apply_fate_update(
            deletion_tx,
            Fate::Accepted,
            Some(GlobalTime(2)),
            Some(DurabilityTier::Global),
        )
        .unwrap();

    assert_eq!(
        rocks
            .database
            .primary_key_scan_raw(&physical_global_current_table_name(table_id), &prefix)
            .unwrap()
            .len(),
        1,
        "globally accepted content retains the canonical branch prefix in global-current"
    );
    assert_eq!(
        rocks
            .database
            .primary_key_scan_raw(
                &physical_register_global_current_table_name(table_id),
                &prefix,
            )
            .unwrap()
            .len(),
        1,
        "globally accepted deletion retains the same prefix in register global-current"
    );
    drop(rocks);

    let mut reopened = reopen_history_complete_node_at(&dir, NodeUuid::from_bytes([0x72; 16]), schema.clone());
    assert_eq!(
        reopened
            .query_row_versions_in_branch("todos", &key, row_uuid)
            .unwrap()
            .len(),
        2,
        "reopen decodes the exact branch coordinate for both layers"
    );
    assert!(
        reopened
            .visible_current_cells_in_branch("todos", &branch, row_uuid)
            .unwrap()
            .is_none(),
        "the reopened deletion current projection still masks the content row"
    );
}

// This is necessarily an internal protocol-boundary regression test: public
// mutation APIs canonicalize branch selectors and therefore cannot construct
// the adversarial VersionRecord values a remote peer may send.
#[test]
fn remote_authored_branch_keys_are_validated_atomically_before_storage() {
    let schema = two_column_branch_view_schema();
    let table = &schema.tables[0];
    let selector = BranchSelector::new([
        ("workspace_id", Value::Uuid(uuid::Uuid::from_bytes([0x61; 16]))),
        ("branch_id", Value::Uuid(uuid::Uuid::from_bytes([0x62; 16]))),
    ]);
    let (valid_key, branch_cells) = schema.project_branch_selector(table, &selector).unwrap();
    let mut content_cells = branch_cells;
    content_cells.insert("title".to_owned(), v("content"));
    content_cells.insert("owner".to_owned(), Value::Uuid(uuid::Uuid::nil()));
    let content = VersionRecord::from_cells(
        table,
        schema.version_id(),
        row(0x63),
        Vec::new(),
        AuthorSubject::SYSTEM,
        10,
        AuthorSubject::SYSTEM,
        10,
        &content_cells,
        None,
    )
    .unwrap()
    .with_branch_key(valid_key.clone());
    let deletion = VersionRecord::from_cells(
        table,
        schema.version_id(),
        row(0x64),
        Vec::new(),
        AuthorSubject::SYSTEM,
        10,
        AuthorSubject::SYSTEM,
        10,
        &BTreeMap::<String, Value>::new(),
        Some(DeletionEvent::Deleted),
    )
    .unwrap()
    .with_branch_key(valid_key.clone());

    let first = valid_key.values[0].clone();
    let second = valid_key.values[1].clone();
    let wrong_value_key = BranchKey {
        values: vec![
            first.clone(),
            (
                second.0.clone(),
                crate::protocol::BranchColumnValue::from(Value::Uuid(
                    uuid::Uuid::from_bytes([0x65; 16]),
                )),
            ),
        ],
    };
    let mut noncanonical = second.1.clone();
    noncanonical.0.push(0);
    let cases = vec![
        ("missing", 0, BranchKey::default()),
        (
            "duplicate",
            0,
            BranchKey {
                values: vec![first.clone(), first.clone()],
            },
        ),
        (
            "extra",
            0,
            BranchKey {
                values: vec![
                    first.clone(),
                    second.clone(),
                    (
                        "unknown".to_owned(),
                        second.1.clone(),
                    ),
                ],
            },
        ),
        (
            "out-of-order",
            0,
            BranchKey {
                values: vec![second.clone(), first.clone()],
            },
        ),
        (
            "wrong-type",
            0,
            BranchKey {
                values: vec![
                    first.clone(),
                    (
                        second.0.clone(),
                        crate::protocol::BranchColumnValue::from(Value::String(
                            "not-a-uuid".to_owned(),
                        )),
                    ),
                ],
            },
        ),
        (
            "noncanonical-encoding",
            0,
            BranchKey {
                values: vec![first.clone(), (second.0.clone(), noncanonical)],
            },
        ),
        ("content-disagrees", 0, wrong_value_key),
        ("deletion-missing", 1, BranchKey::default()),
    ];

    for (case, malformed_index, malformed_key) in cases {
        let (_dir, mut receiver) = open_history_complete_node_with_schema(
            NodeUuid::from_bytes([case.len() as u8; 16]),
            schema.clone(),
        );
        let tx_id = TxId::new(TxTime(10), NodeUuid::from_bytes([0x66; 16]));
        let tx = Transaction {
            tx_id,
            kind: TxKind::Mergeable,
            n_total_writes: 2,
            made_by: AuthorSubject::SYSTEM,
            permission_subject: None,
            base_snapshot: None,
            row_read_set: None,
            absent_read_set: None,
            predicate_read_set: None,
            user_metadata_json: None,
            contribution_merge: None,
        };
        let mut versions = vec![content.clone(), deletion.clone()];
        versions[malformed_index] = versions[malformed_index]
            .clone()
            .with_branch_key(malformed_key);
        let updates = receiver
            .apply_sync_message(SyncMessage::CommitUnit { tx, versions })
            .unwrap();
        assert!(matches!(
            updates.value.as_slice(),
            [SyncMessage::FateUpdate {
                fate: Fate::Rejected(RejectionReason::MalformedCommit(_)),
                global_time: None,
                ..
            }]
        ), "case {case}");
        assert!(receiver.query_table_versions("todos").unwrap().is_empty(), "case {case}");
        assert_eq!(receiver.committed_global_time(), GlobalTime(0), "case {case}");
    }
}

#[test]
fn remote_branch_write_does_not_invalidate_live_branch_view_plans() {
    let schema = branch_view_schema();
    let (_writer_dir, mut writer) =
        open_node_with_schema(NodeUuid::from_bytes([0x56; 16]), schema.clone());
    let (_reader_dir, mut reader) =
        open_node_with_schema(NodeUuid::from_bytes([0x57; 16]), schema);
    let (_, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0x58), 10)
                .branch(branch_selector(0x59))
                .cells(BTreeMap::from([
                    ("title".to_owned(), v("remote")),
                    ("owner".to_owned(), Value::Uuid(uuid::Uuid::nil())),
                ])),
        )
        .unwrap();
    let before = reader.groove_runtime_token();
    reader.apply_sync_message_settled(unit).unwrap();
    assert_eq!(reader.groove_runtime_token(), before);
}

#[test]
fn calculated_merge_commit_persists_only_emitted_target_coordinates() {
    let schema = branch_view_schema();
    let (_dir, mut node) =
        open_history_complete_node_with_schema(NodeUuid::from_bytes([0x64; 16]), schema.clone());
    let row_uuid = row(0x65);
    let source = branch_selector(0x66);
    let target = branch_selector(0x67);
    let table = schema.tables.iter().find(|table| table.name == "todos").unwrap();
    let (source_key, _) = schema.project_branch_selector(table, &source).unwrap();
    let (target_key, _) = schema.project_branch_selector(table, &target).unwrap();
    let source_coordinate = ContributionCoordinate {
        branch_key: source_key.clone(),
        table: "todos".to_owned(),
        row_uuid,
        layer: MergeAspect::Content,
        component: ContributionComponent::Column("title".to_owned()),
    };
    let target_coordinate = ContributionCoordinate {
        branch_key: target_key.clone(),
        table: "todos".to_owned(),
        row_uuid,
        layer: MergeAspect::Content,
        component: ContributionComponent::Column("title".to_owned()),
    };
    let provenance = ContributionMergeProvenance::canonical(
        source_key,
        target_key,
        vec![ContributionSubstitution {
            target: target_coordinate,
            sources: vec![ContributionDot {
                tx_id: TxId::new(TxTime::from(5), NodeUuid::from_bytes([0x68; 16])),
                coordinate: source_coordinate,
            }],
        }],
    )
    .unwrap();
    let published = node
        .commit_calculated_merge_many(
            vec![MergeableCommit::new("todos", row_uuid, 10)
                .branch(target)
                .cells(BTreeMap::from([
                    ("title".to_owned(), v("merged")),
                    ("owner".to_owned(), Value::Uuid(uuid::Uuid::nil())),
                ]))],
            provenance.clone(),
        )
        .unwrap();
    let tx_id = node.persist_and_settle_transaction(published).unwrap();
    assert_eq!(
        node.transaction_record(tx_id).unwrap().contribution_merge,
        Some(provenance)
    );
}

#[test]
fn scalar_contribution_merge_is_retry_safe_and_does_not_echo_home() {
    let schema = branch_view_schema();
    let (_dir, mut node) =
        open_history_complete_node_with_schema(NodeUuid::from_bytes([0x69; 16]), schema);
    let row_uuid = row(0x6a);
    let a = branch_selector(0x6b);
    let b = branch_selector(0x6c);
    let c = branch_selector(0x6d);
    node.commit_mergeable_settled(
        MergeableCommit::new("todos", row_uuid, 10)
            .branch(a.clone())
            .cells(BTreeMap::from([
                ("title".to_owned(), v("from a")),
                ("owner".to_owned(), Value::Uuid(uuid::Uuid::nil())),
            ])),
    )
    .unwrap();
    let request = |source: BranchSelector, target: BranchSelector, now_ms| {
        ContributionMergeRequest {
            source,
            target,
            rows: vec![ContributionMergeRow {
                table: "todos".to_owned(),
                row_uuid,
            }],
            made_by: AuthorSubject::SYSTEM,
            permission_subject: None,
            now_ms,
        }
    };

    assert!(
        node.merge_branch_contributions_settled(request(a.clone(), b.clone(), 20))
            .unwrap()
            .is_some()
    );
    assert_eq!(
        node.visible_current_cells_in_branch("todos", &b, row_uuid)
            .unwrap()
            .unwrap()["title"],
        v("from a")
    );
    assert!(
        node.merge_branch_contributions_settled(request(a.clone(), b.clone(), 30))
            .unwrap()
            .is_none(),
        "observed provenance suppresses retry"
    );
    assert!(
        node.merge_branch_contributions_settled(request(b, c.clone(), 40))
            .unwrap()
            .is_some()
    );
    assert!(
        node.merge_branch_contributions_settled(request(c, a, 50))
            .unwrap()
            .is_none(),
        "A -> B -> C -> A must not echo A's native dots home"
    );
}

#[test]
fn contribution_merge_carries_delete_and_restore_register_events() {
    let schema = branch_view_schema();
    let (_dir, mut node) =
        open_history_complete_node_with_schema(NodeUuid::from_bytes([0x6e; 16]), schema);
    let row_uuid = row(0x6f);
    let source = branch_selector(0x70);
    let target = branch_selector(0x71);
    node.commit_mergeable_settled(
        MergeableCommit::new("todos", row_uuid, 10)
            .branch(source.clone())
            .cells(BTreeMap::from([
                ("title".to_owned(), v("row")),
                ("owner".to_owned(), Value::Uuid(uuid::Uuid::nil())),
            ])),
    )
    .unwrap();
    let request = |now_ms| ContributionMergeRequest {
        source: source.clone(),
        target: target.clone(),
        rows: vec![ContributionMergeRow {
            table: "todos".to_owned(),
            row_uuid,
        }],
        made_by: AuthorSubject::SYSTEM,
        permission_subject: None,
        now_ms,
    };
    node.merge_branch_contributions_settled(request(20)).unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("todos", row_uuid, 30)
            .branch(source.clone())
            .deletion(DeletionEvent::Deleted),
    )
    .unwrap();
    node.merge_branch_contributions_settled(request(40)).unwrap();
    assert!(
        node.visible_current_cells_in_branch("todos", &target, row_uuid)
            .unwrap()
            .is_none()
    );

    node.commit_mergeable_settled(
        MergeableCommit::new("todos", row_uuid, 50)
            .branch(source.clone())
            .deletion(DeletionEvent::Restored),
    )
    .unwrap();
    node.merge_branch_contributions_settled(request(60)).unwrap();
    assert_eq!(
        node.visible_current_cells_in_branch("todos", &target, row_uuid)
            .unwrap()
            .unwrap()["title"],
        v("row")
    );
}

#[test]
fn contribution_merge_receiver_needs_no_source_history() {
    let schema = branch_view_schema();
    let (_writer_dir, mut writer) = open_history_complete_node_with_schema(
        NodeUuid::from_bytes([0x72; 16]),
        schema.clone(),
    );
    let (_receiver_dir, mut receiver) = open_history_complete_node_with_schema(
        NodeUuid::from_bytes([0x73; 16]),
        schema,
    );
    let row_uuid = row(0x74);
    let source = branch_selector(0x75);
    let target = branch_selector(0x76);
    writer
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 10)
                .branch(source.clone())
                .cells(BTreeMap::from([
                    ("title".to_owned(), v("portable")),
                    ("owner".to_owned(), Value::Uuid(uuid::Uuid::nil())),
                ])),
        )
        .unwrap();
    let published = writer
        .merge_branch_contributions(ContributionMergeRequest {
            source,
            target: target.clone(),
            rows: vec![ContributionMergeRow {
                table: "todos".to_owned(),
                row_uuid,
            }],
            made_by: AuthorSubject::SYSTEM,
            permission_subject: None,
            now_ms: 20,
        })
        .unwrap()
        .unwrap();
    let merge = writer.persist_and_settle_transaction(published).unwrap();
    let unit = writer.commit_unit_for(merge).unwrap();
    receiver.apply_sync_message_settled(unit).unwrap();
    assert_eq!(
        receiver
            .visible_current_cells_in_branch("todos", &target, row_uuid)
            .unwrap()
            .unwrap()["title"],
        v("portable")
    );
}

#[test]
fn contribution_merge_denies_unreadable_source_before_minting() {
    let schema = branch_view_schema();
    let (_dir, mut node) =
        open_history_complete_node_with_schema(NodeUuid::from_bytes([0x77; 16]), schema);
    let row_uuid = row(0x78);
    let source = branch_selector(0x79);
    let target = branch_selector(0x7a);
    node.commit_mergeable_settled(
        MergeableCommit::new("todos", row_uuid, 10)
            .branch(source.clone())
            .cells(BTreeMap::from([
                ("title".to_owned(), v("private")),
                ("owner".to_owned(), Value::Uuid(uuid::Uuid::nil())),
            ])),
    )
    .unwrap();
    let unauthorized = AuthorSubject::for_test_bytes([0x7b; 16]);
    let error = node
        .merge_branch_contributions(ContributionMergeRequest {
            source,
            target: target.clone(),
            rows: vec![ContributionMergeRow {
                table: "todos".to_owned(),
                row_uuid,
            }],
            made_by: unauthorized,
            permission_subject: Some(unauthorized),
            now_ms: 20,
        })
        .resolve()
        .err()
        .expect("unreadable contribution source is rejected");
    assert!(
        matches!(error, Error::InvalidMergeableCommit(_)),
        "unexpected contribution authorization error: {error:?}"
    );
    assert!(
        node.visible_current_cells_in_branch("todos", &target, row_uuid)
            .unwrap()
            .is_none()
    );
    let next = node
        .commit_mergeable_settled(
            MergeableCommit::new("users", row(0x7c), 20)
                .cell("name", v("clock receipt")),
        )
        .unwrap();
    assert_eq!(next.time, TxTime::from(20));
}

#[test]
fn counter_contribution_merge_imports_only_novel_native_deltas() {
    let schema = JazzSchema::new_with_branch_columns([TableSchema::new(
            "counts",
            [
                ColumnSchema::new("branch_id", ColumnType::Uuid),
                ColumnSchema::new("count", ColumnType::U64),
            ],
        )
        .with_branch_column("branch_id")
        .with_column_merge_strategy("count", MergeStrategy::Counter)],
    );
    let (_dir, mut node) =
        open_history_complete_node_with_schema(NodeUuid::from_bytes([0x7e; 16]), schema);
    let row_uuid = row(0x7f);
    let a = branch_selector(0x80);
    let b = branch_selector(0x81);
    let c = branch_selector(0x82);
    let first = node
        .commit_mergeable_settled(
            MergeableCommit::new("counts", row_uuid, 10)
                .branch(a.clone())
                .cell("count", Value::U64(5)),
        )
        .unwrap();
    let request = |source: BranchSelector, target: BranchSelector, now_ms| {
        ContributionMergeRequest {
            source,
            target,
            rows: vec![ContributionMergeRow {
                table: "counts".to_owned(),
                row_uuid,
            }],
            made_by: AuthorSubject::SYSTEM,
            permission_subject: None,
            now_ms,
        }
    };
    node.merge_branch_contributions_settled(request(a.clone(), b.clone(), 20))
        .unwrap();
    node.commit_mergeable_settled(
        MergeableCommit::new("counts", row_uuid, 30)
            .branch(a.clone())
            .parents(vec![first])
            .cell("count", Value::U64(8)),
    )
    .unwrap();
    node.merge_branch_contributions_settled(request(a.clone(), b.clone(), 40))
        .unwrap();
    assert_eq!(
        node.visible_current_cells_in_branch("counts", &b, row_uuid)
            .unwrap()
            .unwrap()["count"],
        Value::U64(8)
    );
    node.merge_branch_contributions_settled(request(b, c.clone(), 50))
        .unwrap();
    assert!(
        node.merge_branch_contributions_settled(request(c, a, 60))
            .unwrap()
            .is_none()
    );
}

#[test]
fn gset_contribution_merge_tracks_elements_as_native_operations() {
    let schema = JazzSchema::new_with_branch_columns([TableSchema::new(
            "sets",
            [
                ColumnSchema::new("branch_id", ColumnType::Uuid),
                ColumnSchema::new("members", ColumnType::Array(Box::new(ColumnType::String))),
            ],
        )
        .with_branch_column("branch_id")
        .with_column_merge_strategy("members", MergeStrategy::GSet)],
    );
    let (_dir, mut node) =
        open_history_complete_node_with_schema(NodeUuid::from_bytes([0x84; 16]), schema);
    let row_uuid = row(0x85);
    let a = branch_selector(0x86);
    let b = branch_selector(0x87);
    let c = branch_selector(0x88);
    let first = node
        .commit_mergeable_settled(
            MergeableCommit::new("sets", row_uuid, 10)
                .branch(a.clone())
                .cell("members", Value::Array(vec![v("one")])),
        )
        .unwrap();
    let request = |source: BranchSelector, target: BranchSelector, now_ms| {
        ContributionMergeRequest {
            source,
            target,
            rows: vec![ContributionMergeRow {
                table: "sets".to_owned(),
                row_uuid,
            }],
            made_by: AuthorSubject::SYSTEM,
            permission_subject: None,
            now_ms,
        }
    };
    let first_merge = node
        .merge_branch_contributions_settled(request(a.clone(), b.clone(), 20))
        .unwrap()
        .unwrap();
    let provenance = node
        .transaction_record(first_merge)
        .unwrap()
        .contribution_merge
        .unwrap();
    let ContributionComponent::Operation { column, identity } =
        &provenance.substitutions[0].target.component
    else {
        panic!("g-set substitution target must carry an operation identity");
    };
    assert_eq!(column, "members");
    let descriptor = records::RecordDescriptor::new([("element", records::ValueType::String)]);
    assert_eq!(identity, &descriptor.create(&[v("one")]).unwrap());
    node.commit_mergeable_settled(
        MergeableCommit::new("sets", row_uuid, 30)
            .branch(a.clone())
            .parents(vec![first])
            .cell("members", Value::Array(vec![v("two")])),
    )
    .unwrap();
    node.merge_branch_contributions_settled(request(a.clone(), b.clone(), 40))
        .unwrap();
    assert_eq!(
        node.visible_current_cells_in_branch("sets", &b, row_uuid)
            .unwrap()
            .unwrap()["members"],
        Value::Array(vec![v("one"), v("two")])
    );
    node.merge_branch_contributions_settled(request(b, c.clone(), 50))
        .unwrap();
    assert!(
        node.merge_branch_contributions_settled(request(c, a, 60))
            .unwrap()
            .is_none()
    );
}

#[test]
fn maintained_live_base_emits_a_delta_before_facade_refresh() {
    let schema = branch_view_schema();
    let (_dir, mut node) =
        open_history_complete_node_with_schema(NodeUuid::from_bytes([0x5a; 16]), schema.clone());
    let row_uuid = row(0x5b);
    let base = branch_selector(0x5c);
    let head = branch_selector(0x5d);
    node.commit_mergeable_settled(
        MergeableCommit::new("todos", row_uuid, 10)
            .branch(base.clone())
            .cells(BTreeMap::from([
                ("title".to_owned(), v("base")),
                ("owner".to_owned(), Value::Uuid(uuid::Uuid::nil())),
            ])),
    )
    .unwrap();
    let read_view = crate::protocol::ReadViewSpec::branch_view(
        head,
        Some(crate::protocol::BranchViewBase::Current(base.clone())),
    );
    let shape = Query::from("todos").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let (shape, binding, plan) = node
        .prepare_query_binding_for_link_in_authorization_mode(
            &shape,
            &binding,
            DurabilityTier::Local,
            AuthorSubject::SYSTEM,
            QueryAuthorizationMode::ClientLocal,
        )
        .unwrap();
    let (mut maintained, initial) = node
        .open_maintained_view_subscription_in_authorization_mode(
            &shape,
            &binding,
            AuthorSubject::SYSTEM,
            DurabilityTier::Local,
            &read_view,
            Some(plan),
            QueryAuthorizationMode::ClientLocal,
        )
        .unwrap();
    assert_eq!(initial.root_count, 1);

    node.commit_mergeable_settled(
        MergeableCommit::new("todos", row_uuid, 20)
            .branch(base)
            .cells(BTreeMap::from([
                ("title".to_owned(), v("base edited")),
                ("owner".to_owned(), Value::Uuid(uuid::Uuid::nil())),
            ])),
    )
    .unwrap();
    let update = node
        .drain_local_maintained_view_subscription(&mut maintained, None)
        .unwrap()
        .expect("live-base write must emit a maintained delta");
    let LocalMaintainedViewSubscriptionUpdate::Flat { added, removed, .. } = update else {
        panic!("flat branch query produced a structured maintained update");
    };
    assert_eq!(added.len(), 1);
    assert_eq!(removed.len(), 1);
}

#[test]
fn added_branch_column_defaults_old_history_and_survives_column_rename() {
    // Schema-lineage physical identities are not exposed by the public facade,
    // so this internal test exercises publication, normalization, and reopen as
    // one mechanism boundary.
    let base = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("todos").column("title", PublicColumnType::Text)),
    );
    let (dir, mut core) = open_history_complete_node_with_schema(node(0x91), base.clone());
    let inherited = row(0x92);
    core.commit_mergeable_settled(
        MergeableCommit::new("todos", inherited, 10).cells(title_cells("old-default")),
    )
    .unwrap();

    let default_workspace = uuid::Uuid::from_bytes([0x94; 16]);
    let other_workspace = uuid::Uuid::from_bytes([0x95; 16]);
    let evolved = build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column_with_default(
                    "workspace_id",
                    PublicColumnType::Uuid,
                    PublicValue::Uuid(crate::tools::ObjectId::from_uuid(default_workspace)),
                )
                .branch_by("workspace_id"),
        ),
    );
    let evolved_version = SchemaVersion::new(evolved.clone());
    publish_schema_lineage(
        &mut core,
        evolved_version.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved_version.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "workspace_id".to_owned(),
                    default: Value::Uuid(default_workspace),
                }],
            }],
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: evolved_version.id,
        },
    })
    .unwrap();

    let other = row(0x96);
    core.commit_mergeable_settled(
        MergeableCommit::new("todos", other, 20)
            .branch(BranchSelector::new([(
                "workspace_id",
                Value::Uuid(other_workspace),
            )]))
            .cells(BTreeMap::from([
                ("title".to_owned(), v("other")),
                ("workspace_id".to_owned(), Value::Uuid(other_workspace)),
            ])),
    )
    .unwrap();

    let rows_for = |node: &mut NodeState<_>, schema: &JazzSchema, workspace| {
        let shape = Query::from("todos").validate(schema).unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let branch_column = schema.tables[0].branch_by[0].clone();
        let view = crate::protocol::ReadViewSpec {
            source: crate::protocol::ReadViewSourceSpec::BranchView {
                head: BranchSelector::new([(branch_column, Value::Uuid(workspace))]),
                base: None,
            },
        };
        node.query_relation_snapshot_for_serving_in_read_view(
            &shape,
            &binding,
            DurabilityTier::Local,
            AuthorSubject::SYSTEM,
            &view,
        )
        .unwrap()
        .rows
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>()
    };
    assert_eq!(
        rows_for(&mut core, &evolved, default_workspace),
        BTreeSet::from([inherited])
    );
    assert_eq!(
        rows_for(&mut core, &evolved, other_workspace),
        BTreeSet::from([other])
    );

    let renamed = build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column_with_default(
                    "space_id",
                    PublicColumnType::Uuid,
                    PublicValue::Uuid(crate::tools::ObjectId::from_uuid(default_workspace)),
                )
                .branch_by("space_id"),
        ),
    );
    let renamed_version = SchemaVersion::new(renamed.clone());
    publish_schema_lineage(
        &mut core,
        renamed_version.clone(),
        MigrationLens::new(
            evolved_version.id,
            renamed_version.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::RenameColumn {
                    from: "workspace_id".to_owned(),
                    to: "space_id".to_owned(),
                }],
            }],
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 2,
            schema: renamed_version.id,
        },
    })
    .unwrap();
    drop(core);

    let mut reopened = reopen_node_at(&dir, node(0x91), base);
    assert_eq!(
        rows_for(&mut reopened, &renamed, other_workspace),
        BTreeSet::from([other])
    );
}

#[test]
fn branched_table_writes_require_an_explicit_exact_selector() {
    let schema = branch_view_schema();
    let (_dir, mut core) = open_history_complete_node_with_schema(node(0x97), schema);

    let error = core
        .commit_mergeable(
            MergeableCommit::new("todos", row(0x98), 10).cells(BTreeMap::from([
                ("title".to_owned(), v("missing branch")),
                ("owner".to_owned(), Value::Uuid(uuid::Uuid::nil())),
            ])),
        )
        .resolve()
        .err()
        .expect("branched write without selector is rejected");

    assert!(matches!(
        error,
        crate::node::Error::InvalidBranchKey(message)
            if message == "branch selector for todos must provide exactly 1 values"
    ));
}

#[test]
fn branch_column_evolution_rejects_non_monotone_changes() {
    // These catalogue identities are deliberately exercised below the facade:
    // publication must reject invalid lineage before it becomes writable.
    let source = branch_view_schema();
    let mut changed_default = source.clone();
    changed_default.runtime_mut_for_testing().tables[0].columns[0].default =
        Some(Value::Uuid(uuid::Uuid::from_bytes([0x99; 16])));
    let mut changed_type = source.clone();
    changed_type.runtime_mut_for_testing().tables[0].columns[0].column_type = ColumnType::String;
    let mut removed_from_table = source.clone();
    removed_from_table.runtime_mut_for_testing().tables[0].branch_by.clear();

    for (target, expected) in [
        (
            changed_default,
            "branch column type and migration default are immutable",
        ),
        (
            changed_type,
            "branch column type and migration default are immutable",
        ),
        (
            removed_from_table,
            "table branch columns cannot be removed",
        ),
    ] {
        let (_dir, mut core) =
            open_history_complete_node_with_schema(node(0x9a), source.clone());
        let target = SchemaVersion::new(target);
        let error = publish_schema_lineage(
            &mut core,
            target.clone(),
            MigrationLens::new(
                source.version_id(),
                target.id,
                vec![TableLens {
                    source_table: "todos".to_owned(),
                    target_table: "todos".to_owned(),
                    ops: Vec::new(),
                }],
            ),
            Vec::<String>::new(),
            Vec::<String>::new(),
        )
        .unwrap_err();
        assert!(matches!(
            error,
            crate::node::Error::InvalidCatalogueUpdate(message) if message == expected
        ));
    }
}

#[test]
fn branch_column_evolution_accepts_monotone_addition_with_default() {
    let source = branch_view_schema();
    let mut target = source.clone();
    target.runtime_mut_for_testing().tables[0].columns.push(
        crate::schema::ColumnSchema::new("alpha", ColumnType::Uuid)
            .with_default(Value::Uuid(uuid::Uuid::nil())),
    );
    target.runtime_mut_for_testing().tables[0].branch_by.insert(0, "alpha".to_owned());
    let source = SchemaVersion::new(source);
    let target = SchemaVersion::new(target);
    let lens = MigrationLens::new(
        source.id,
        target.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![LensOp::AddColumn {
                column: "alpha".to_owned(),
                default: Value::Uuid(uuid::Uuid::nil()),
            }],
        }],
    );

    NodeState::<RocksDbStorage>::validate_migration_lens_between(&lens, &source, &target)
        .expect("a branch column can be added monotonically with an immutable default");
}
