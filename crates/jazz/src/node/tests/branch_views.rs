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
            ],
        )
        .with_branch_dimension("branch_id", dimension),
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

    for (row_uuid, title) in [(inherited, "inherited"), (overridden, "base")] {
        node.commit_mergeable(
            MergeableCommit::new("todos", row_uuid, 10)
                .branch(base.clone())
                .cells(BTreeMap::from([("title".to_owned(), v(title))])),
        )
        .unwrap();
    }
    node.commit_mergeable(
        MergeableCommit::new("todos", overridden, 20)
            .branch(head.clone())
            .cells(BTreeMap::from([("title".to_owned(), v("head"))])),
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
