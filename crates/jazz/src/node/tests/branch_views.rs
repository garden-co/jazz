fn branch_view_schema() -> JazzSchema {
    let dimension = crate::ids::BranchDimensionId(uuid::Uuid::from_bytes([0x41; 16]));
    JazzSchema::new_with_branch_dimensions(
        [crate::schema::BranchDimensionSchema::new(
            dimension,
            "branch",
            ColumnType::Uuid,
            Value::Uuid(uuid::Uuid::nil()),
        )],
        [TableSchema::new(
            "todos",
            [
                ColumnSchema::new("branch_id", ColumnType::Uuid),
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("owner", ColumnType::Uuid),
            ],
        )
        .with_branch_dimension("branch_id", dimension)
        .with_read_policy(Policy::owner_only("todos", "owner")),
        TableSchema::new("users", [ColumnSchema::new("name", ColumnType::String)])],
    )
}

fn branch_selector(byte: u8) -> BranchSelector {
    BranchSelector::new([("branch", Value::Uuid(uuid::Uuid::from_bytes([byte; 16])))])
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
    let owner = AuthorId::from_bytes([0x48; 16]);

    for (row_uuid, title) in [(inherited, "inherited"), (overridden, "base")] {
        node.commit_mergeable(
            MergeableCommit::new("todos", row_uuid, 10)
                .branch(base.clone())
                .cells(BTreeMap::from([
                    ("title".to_owned(), v(title)),
                    ("owner".to_owned(), Value::Uuid(owner.0)),
                ])),
        )
        .unwrap();
    }
    node.commit_mergeable(
        MergeableCommit::new("todos", overridden, 20)
            .branch(head.clone())
            .cells(BTreeMap::from([
                ("title".to_owned(), v("head")),
                ("owner".to_owned(), Value::Uuid(owner.0)),
            ])),
    )
    .unwrap();
    let shared = row(0x47);
    node.commit_mergeable(
        MergeableCommit::new("users", shared, 30)
            .cells(BTreeMap::from([("name".to_owned(), v("shared"))])),
    )
    .unwrap();

    let read_view = crate::protocol::ReadViewSpec {
        source: crate::protocol::ReadViewSourceSpec::BranchView {
            head: head.clone(),
            base: Some(crate::protocol::BranchViewBase::Current(base)),
        },
        ..crate::protocol::ReadViewSpec::default()
    };
    let shape = Query::from("todos").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let snapshot = node
        .query_relation_snapshot_for_serving_in_read_view(
            &shape,
            &binding,
            DurabilityTier::Local,
            AuthorId::SYSTEM,
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
        assert_eq!(row.cell(todos_table, "owner"), Some(Value::Uuid(owner.0)));
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
            AuthorId::from_bytes([0x49; 16]),
            &read_view,
        )
        .unwrap();
    assert_eq!(denied.root_count, 0);

    let shared_default = node
        .query_relation_snapshot_for_serving(
            &shape,
            &binding,
            DurabilityTier::Local,
            AuthorId::SYSTEM,
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
            AuthorId::SYSTEM,
            &read_view,
        )
        .unwrap();
    assert_eq!(shared_snapshot.root_count, 1);
    assert_eq!(shared_snapshot.rows[0].row_uuid(), shared);

    node.commit_mergeable(
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
            AuthorId::SYSTEM,
            &read_view,
        )
        .unwrap();
    assert_eq!(after_delete.root_count, 1);
    assert_eq!(after_delete.rows[0].row_uuid(), inherited);
}

#[test]
fn version_parents_cannot_cross_branch_keys() {
    let schema = branch_view_schema();
    let (_dir, mut node) =
        open_history_complete_node_with_schema(NodeUuid::from_bytes([0x51; 16]), schema);
    let row_uuid = row(0x52);
    let owner = AuthorId::from_bytes([0x53; 16]);
    let parent = node
        .commit_mergeable(
            MergeableCommit::new("todos", row_uuid, 10)
                .branch(branch_selector(0x54))
                .cells(BTreeMap::from([
                    ("title".to_owned(), v("base")),
                    ("owner".to_owned(), Value::Uuid(owner.0)),
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
                    ("owner".to_owned(), Value::Uuid(owner.0)),
                ])),
        )
        .unwrap_err();
    assert!(matches!(error, Error::InvalidMergeableCommit(_)));
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
                    ("owner".to_owned(), Value::Uuid(AuthorId::SYSTEM.0)),
                ])),
        )
        .unwrap();
    let before = reader.groove_runtime_token();
    reader.apply_sync_message(unit).unwrap();
    assert_eq!(reader.groove_runtime_token(), before);
}

#[test]
fn maintained_live_base_emits_a_delta_before_facade_refresh() {
    let schema = branch_view_schema();
    let (_dir, mut node) =
        open_history_complete_node_with_schema(NodeUuid::from_bytes([0x5a; 16]), schema.clone());
    let row_uuid = row(0x5b);
    let base = branch_selector(0x5c);
    let head = branch_selector(0x5d);
    node.commit_mergeable(
        MergeableCommit::new("todos", row_uuid, 10)
            .branch(base.clone())
            .cells(BTreeMap::from([
                ("title".to_owned(), v("base")),
                ("owner".to_owned(), Value::Uuid(AuthorId::SYSTEM.0)),
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
            AuthorId::SYSTEM,
            QueryAuthorizationMode::ClientLocal,
        )
        .unwrap();
    let (mut maintained, initial) = node
        .open_maintained_view_subscription_in_authorization_mode(
            &shape,
            &binding,
            AuthorId::SYSTEM,
            DurabilityTier::Local,
            &read_view,
            Some(plan),
            QueryAuthorizationMode::ClientLocal,
        )
        .unwrap();
    assert_eq!(initial.root_count, 1);

    node.commit_mergeable(
        MergeableCommit::new("todos", row_uuid, 20)
            .branch(base)
            .cells(BTreeMap::from([
                ("title".to_owned(), v("base edited")),
                ("owner".to_owned(), Value::Uuid(AuthorId::SYSTEM.0)),
            ])),
    )
    .unwrap();
    let update = node
        .drain_local_maintained_view_subscription(&mut maintained, None)
        .unwrap()
        .expect("live-base write must emit a maintained delta");
    assert_eq!(update.added.len(), 1);
    assert_eq!(update.removed.len(), 1);
}
