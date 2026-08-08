#[test]
fn branch_read_is_base_snapshot_plus_overlay_writes() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_history_complete_node_with_schema(node(2), schema());
    let mut oracle = Oracle::new();
    let shape = Query::from("todos").validate(&core.catalogue.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    commit_global_and_oracle(
        &mut writer,
        &mut core,
        &mut oracle,
        MergeableCommit::new("todos", row(1), 10).cells(title_cells("base-one")),
    );
    let branch_id = branch(1);
    let branch_record = core.create_branch(branch_id).unwrap();
    assert_eq!(branch_record.base.unwrap().global_base, GlobalSeq(1));

    commit_global_and_oracle(
        &mut writer,
        &mut core,
        &mut oracle,
        MergeableCommit::new("todos", row(2), 20).cells(title_cells("after-branch")),
    );
    core.commit_mergeable_on_branch(
        branch_id,
        MergeableCommit::new("todos", row(1), 30).cells(title_cells("overlay-one")),
    )
    .unwrap();
    core.commit_mergeable_on_branch(
        branch_id,
        MergeableCommit::new("todos", row(3), 31).cells(title_cells("overlay-three")),
    )
    .unwrap();

    core.reset_query_engine_read_metrics();
    let actual = core
        .query_rows_on_branch(branch_id, &shape, &binding)
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        actual,
        BTreeMap::from([
            (row(1), title_cells("overlay-one")),
            (row(3), title_cells("overlay-three")),
        ])
    );
    assert_eq!(
        core.query_engine_read_metrics()
            .source_global_seq_range_scans,
        1,
        "branch base hydration should use the bounded historical range path"
    );

    let main = core
        .current_rows("todos", DurabilityTier::Global)
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        main,
        BTreeMap::from([
            (row(1), title_cells("base-one")),
            (row(2), title_cells("after-branch")),
        ])
    );
}

#[test]
fn branch_read_filter_shape_uses_shared_branch_source_lowering() {
    let (_dir, mut core) = open_history_complete_node_with_schema(node(2), schema());
    let branch_id = branch(0x41);
    core.create_branch(branch_id).unwrap();
    core.commit_mergeable_on_branch(
        branch_id,
        MergeableCommit::new("todos", row(1), 10).cells(title_cells("branch write")),
    )
    .unwrap();

    let shape = Query::from("todos")
        .filter(eq(col("title"), lit("branch write")))
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let rows = core
        .query_rows_on_branch(branch_id, &shape, &binding)
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        rows,
        BTreeMap::from([(row(1), title_cells("branch write"))])
    );
}

#[test]
fn branch_read_join_uses_shared_branch_sources() {
    let schema = JazzSchema::new([
        TableSchema::new("todos", [ColumnSchema::new("title", ColumnType::String)]),
        TableSchema::new(
            "todo_members",
            [
                ColumnSchema::new("todo", ColumnType::Uuid),
                ColumnSchema::new("member", ColumnType::Uuid),
            ],
        )
        .with_reference("todo", "todos"),
    ]);
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_history_complete_node_with_schema(node(2), schema.clone());
    let mut oracle = Oracle::new();
    let member = row(0x77);

    commit_global_and_oracle(
        &mut writer,
        &mut core,
        &mut oracle,
        MergeableCommit::new("todos", row(1), 10).cells(title_cells("base")),
    );
    let branch_id = branch(0x42);
    core.create_branch(branch_id).unwrap();
    commit_global_and_oracle(
        &mut writer,
        &mut core,
        &mut oracle,
        MergeableCommit::new("todo_members", row(2), 20).cells(BTreeMap::from([
            ("todo".to_owned(), Value::Uuid(row(1).0)),
            ("member".to_owned(), Value::Uuid(member.0)),
        ])),
    );

    let shape = Query::from("todos")
        .join_via(
            "todo_members",
            "todo",
            [eq(col("member"), param("member"))],
        )
        .validate(&schema)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([(
            "member".to_owned(),
            Value::Uuid(member.0),
        )]))
        .unwrap();

    assert!(
        core.query_rows_on_branch(branch_id, &shape, &binding)
            .unwrap()
            .is_empty(),
        "post-branch global join rows must not be visible in the branch view"
    );

    core.commit_mergeable_on_branch(
        branch_id,
        MergeableCommit::new("todo_members", row(3), 30).cells(BTreeMap::from([
            ("todo".to_owned(), Value::Uuid(row(1).0)),
            ("member".to_owned(), Value::Uuid(member.0)),
        ])),
    )
    .unwrap();
    let rows = core
        .query_rows_on_branch(branch_id, &shape, &binding)
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    assert_eq!(rows, BTreeMap::from([(row(1), title_cells("base"))]));
}

#[test]
fn branch_read_reachable_uses_shared_branch_sources() {
    let schema = JazzSchema::new([
        TableSchema::new("teams", [ColumnSchema::new("name", ColumnType::String)]),
        TableSchema::new("resources", [ColumnSchema::new("name", ColumnType::String)]),
        TableSchema::new(
            "resourceAccess",
            [
                ColumnSchema::new("resource", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
            ],
        )
        .with_reference("resource", "resources")
        .with_reference("team", "teams"),
        TableSchema::new(
            "teamTeamMemberships",
            [
                ColumnSchema::new("member", ColumnType::Uuid),
                ColumnSchema::new("parent", ColumnType::Uuid),
                ColumnSchema::new("onlyAdmins", ColumnType::Bool),
            ],
        )
        .with_reference("member", "teams")
        .with_reference("parent", "teams"),
    ]);
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_history_complete_node_with_schema(node(2), schema.clone());
    let mut oracle = Oracle::new();
    let team1 = row(1);
    let team2 = row(2);
    let team3 = row(3);
    let resource1 = row(101);
    commit_global_and_oracle(
        &mut writer,
        &mut core,
        &mut oracle,
        MergeableCommit::new("resources", resource1, 10)
            .cells(BTreeMap::from([("name".to_owned(), v("r1"))])),
    );
    commit_global_and_oracle(
        &mut writer,
        &mut core,
        &mut oracle,
        MergeableCommit::new("resourceAccess", row(201), 11).cells(BTreeMap::from([
            ("resource".to_owned(), Value::Uuid(resource1.0)),
            ("team".to_owned(), Value::Uuid(team3.0)),
        ])),
    );
    commit_global_and_oracle(
        &mut writer,
        &mut core,
        &mut oracle,
        MergeableCommit::new("teamTeamMemberships", row(0x31), 12).cells(BTreeMap::from([
            ("member".to_owned(), Value::Uuid(team1.0)),
            ("parent".to_owned(), Value::Uuid(team2.0)),
            ("onlyAdmins".to_owned(), Value::Bool(false)),
        ])),
    );
    let branch_id = branch(0x43);
    core.create_branch(branch_id).unwrap();
    commit_global_and_oracle(
        &mut writer,
        &mut core,
        &mut oracle,
        MergeableCommit::new("teamTeamMemberships", row(0x32), 13).cells(BTreeMap::from([
            ("member".to_owned(), Value::Uuid(team2.0)),
            ("parent".to_owned(), Value::Uuid(team3.0)),
            ("onlyAdmins".to_owned(), Value::Bool(false)),
        ])),
    );

    let shape = Query::from("resources")
        .reachable_via(
            "resourceAccess",
            "resource",
            "team",
            param("team"),
            "teamTeamMemberships",
            "member",
            "parent",
            [eq(col("onlyAdmins"), lit(false))],
        )
        .validate(&schema)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([("team".to_owned(), Value::Uuid(team1.0))]))
        .unwrap();
    assert!(
        core.query_rows_on_branch(branch_id, &shape, &binding)
            .unwrap()
            .is_empty(),
        "post-branch global reachable edges must not be visible in the branch view"
    );

    core.commit_mergeable_on_branch(
        branch_id,
        MergeableCommit::new("teamTeamMemberships", row(0x33), 14).cells(BTreeMap::from([
            ("member".to_owned(), Value::Uuid(team2.0)),
            ("parent".to_owned(), Value::Uuid(team3.0)),
            ("onlyAdmins".to_owned(), Value::Bool(false)),
        ])),
    )
    .unwrap();
    let rows = core
        .query_rows_on_branch(branch_id, &shape, &binding)
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        rows,
        BTreeMap::from([(
            resource1,
            BTreeMap::from([("name".to_owned(), v("r1"))])
        )])
    );
}

#[test]
fn branches_do_not_observe_sibling_overlays_and_recover_metadata() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (core_dir, mut core) = open_history_complete_node_with_schema(node(2), schema());
    let mut oracle = Oracle::new();
    let shape = Query::from("todos").validate(&core.catalogue.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    commit_global_and_oracle(
        &mut writer,
        &mut core,
        &mut oracle,
        MergeableCommit::new("todos", row(1), 10).cells(title_cells("base")),
    );
    let left = branch(2);
    let right = branch(3);
    core.create_branch(left).unwrap();
    core.create_branch(right).unwrap();
    core.commit_mergeable_on_branch(
        left,
        MergeableCommit::new("todos", row(1), 20).cells(title_cells("left")),
    )
    .unwrap();
    core.commit_mergeable_on_branch(
        right,
        MergeableCommit::new("todos", row(1), 21).cells(title_cells("right")),
    )
    .unwrap();

    let left_rows = core
        .query_rows_on_branch(left, &shape, &binding)
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    let right_rows = core
        .query_rows_on_branch(right, &shape, &binding)
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    assert_eq!(left_rows, BTreeMap::from([(row(1), title_cells("left"))]));
    assert_eq!(right_rows, BTreeMap::from([(row(1), title_cells("right"))]));

    drop(core);
    let reopened = reopen_node_at(&core_dir, node(2), schema());
    assert_eq!(
        reopened.branch_record(left).unwrap().base.as_ref().unwrap().global_base,
        GlobalSeq(1)
    );
    assert_eq!(
        reopened.branch_record(right).unwrap().base.as_ref().unwrap().global_base,
        GlobalSeq(1)
    );
    assert_eq!(
        reopened.branch_record(left).unwrap().created_by,
        AuthorId(uuid::Uuid::nil()),
        "branch creator attribution must survive durable recovery"
    );
}
#[test]
fn branch_exclusive_returns_v1_error() {
    let (_dir, mut node) = open_history_complete_node_with_schema(node(2), schema());
    let branch_id = branch(4);
    node.create_branch(branch_id).unwrap();
    assert!(matches!(
        node.open_exclusive_on_branch(branch_id),
        Err(Error::UnsupportedBranchExclusive)
    ));
}
#[test]
fn branch_creation_does_not_scale_with_base_row_count() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_small_dir, mut small) = open_history_complete_node_with_schema(node(2), schema());
    let (_large_dir, mut large) = open_history_complete_node_with_schema(node(3), schema());
    let mut oracle = Oracle::new();

    for idx in 0..4_u8 {
        commit_global_and_oracle(
            &mut writer,
            &mut small,
            &mut oracle,
            MergeableCommit::new("todos", row(idx), 10 + idx as u64)
                .cells(title_cells(format!("small-{idx}"))),
        );
    }
    for idx in 0..80_u8 {
        commit_global_and_oracle(
            &mut writer,
            &mut large,
            &mut oracle,
            MergeableCommit::new("todos", row(idx), 100 + idx as u64)
                .cells(title_cells(format!("large-{idx}"))),
        );
    }

    let small_before = std::time::Instant::now();
    small.create_branch(branch(5)).unwrap();
    let small_us = small_before.elapsed().as_micros();
    let large_before = std::time::Instant::now();
    large.create_branch(branch(6)).unwrap();
    let large_us = large_before.elapsed().as_micros();

    assert_eq!(
        small.branch_record(branch(5)).unwrap().base.as_ref().unwrap().global_base,
        GlobalSeq(4)
    );
    assert_eq!(
        large.branch_record(branch(6)).unwrap().base.as_ref().unwrap().global_base,
        GlobalSeq(80)
    );
    assert!(
        large_us < small_us.saturating_mul(50).saturating_add(50_000),
        "branch creation should be O(1)-style metadata write, small={small_us}us large={large_us}us"
    );
}

#[test]
fn branch_read_requires_branch_row_read_then_branch_local_row_policy() {
    let branch_id = branch(7);
    let allowed = user(0xa1);
    let denied = user(0xb2);
    let schema = branch_rls_schema();
    let (_dir, mut core) = open_history_complete_node_with_schema(node(2), schema);
    let shape = Query::from("todos").validate(&core.catalogue.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    seed_branch_acl(&mut core, branch_id, allowed, row(70), 10);
    core.create_branch(branch_id).unwrap();
    core.commit_mergeable_on_branch(
        branch_id,
        MergeableCommit::new("todos", row(71), 20).cells(owner_cells(denied, "hidden row")),
    )
    .unwrap();
    core.commit_mergeable_on_branch(
        branch_id,
        MergeableCommit::new("todos", row(72), 21).cells(owner_cells(allowed, "visible row")),
    )
    .unwrap();

    assert!(
        core.query_rows_on_branch_for_link(branch_id, &shape, &binding, denied)
            .unwrap()
            .is_empty(),
        "branch-row read denial must hide the whole branch view"
    );

    let allowed_rows = core
        .query_rows_on_branch_for_link(branch_id, &shape, &binding, allowed)
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        allowed_rows,
        BTreeMap::from([(row(72), owner_cells(allowed, "visible row"))]),
        "branch-row read access must still be narrowed by table read policy"
    );
}

#[test]
fn branch_write_requires_branch_row_write_then_branch_local_write_policy() {
    let branch_id = branch(8);
    let allowed = user(0xa1);
    let denied = user(0xb2);
    let schema = branch_rls_schema();
    let (_dir, mut core) = open_history_complete_node_with_schema(node(2), schema);

    seed_branch_acl(&mut core, branch_id, allowed, row(80), 10);
    core.create_branch(branch_id).unwrap();

    assert!(matches!(
        core.commit_mergeable_on_branch(
            branch_id,
            MergeableCommit::new("todos", row(81), 20)
                .made_by(denied)
                .cells(owner_cells(denied, "blocked at branch row")),
        ),
        Err(Error::AuthorizationDenied)
    ));

    assert!(matches!(
        core.commit_mergeable_on_branch(
            branch_id,
            MergeableCommit::new("todos", row(82), 21)
                .made_by(allowed)
                .cells(owner_cells(denied, "blocked at table row")),
        ),
        Err(Error::AuthorizationDenied)
    ));

    let create = core
        .commit_mergeable_on_branch(
            branch_id,
            MergeableCommit::new("todos", row(83), 22)
                .made_by(allowed)
                .cells(owner_cells(allowed, "branch-local owner")),
        )
        .unwrap();
    core.commit_mergeable_on_branch(
        branch_id,
        MergeableCommit::new("todos", row(83), 23)
            .made_by(allowed)
            .parents(vec![create])
            .deletion(DeletionEvent::Deleted),
    )
    .unwrap();
}

fn branch_rls_schema() -> JazzSchema {
    let branch_policy = Policy::shape(Query::from("jazz_branches").join_via(
        "branchAccess",
        "branch_id",
        [eq(col("userID"), claim("sub"))],
    ));
    JazzSchema::new([
        TableSchema::new(
            "todos",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("owner", ColumnType::Uuid),
            ],
        )
        .with_read_policy(Policy::owner_only("todos", "owner"))
        .with_write_policy(Policy::owner_only("todos", "owner")),
        TableSchema::new(
            "branchAccess",
            [
                ColumnSchema::new("branch_id", ColumnType::Uuid),
                ColumnSchema::new("userID", ColumnType::Uuid),
            ],
        )
        .with_reference("branch_id", "jazz_branches"),
    ])
    .with_branch_read_policy(branch_policy.clone())
    .with_branch_write_policy(branch_policy)
}

fn seed_branch_acl(
    core: &mut NodeState<RocksDbStorage>,
    branch_id: BranchId,
    author: AuthorId,
    row_uuid: RowUuid,
    now_ms: u64,
) {
    let tx_id = core
        .commit_mergeable(MergeableCommit::new("branchAccess", row_uuid, now_ms).cells(
            BTreeMap::from([
                ("branch_id".to_owned(), Value::Uuid(branch_id.0)),
                ("userID".to_owned(), Value::Uuid(author.0)),
            ]),
        ))
        .unwrap();
    core.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(core.clock.next_global_seq),
        Some(DurabilityTier::Global),
    )
    .unwrap();
}

#[test]
fn branch_creation_persists_no_overlay_partition_until_first_write() {
    let (_dir, mut core) = open_history_complete_node_with_schema(node(2), schema());
    let branch_id = branch(0x12);

    core.create_branch(branch_id).unwrap();
    assert!(
        !core.branches.branch_partitions
            .iter()
            .any(|(_, _, existing)| *existing == branch_id),
        "INV-BRANCH-12: branch creation must not eagerly create overlay partitions"
    );

    core.commit_mergeable_on_branch(
        branch_id,
        MergeableCommit::new("todos", row(1), 10).cells(title_cells("branch write")),
    )
    .unwrap();
    assert!(
        core.branches.branch_partitions.iter().any(
            |(table, schema_version, existing)| table == "todos"
                && *schema_version == core.current_write_schema().schema
                && *existing == branch_id
        ),
        "INV-BRANCH-12: first branch write must create the overlay partition"
    );
}

#[test]
fn branch_overlay_partition_creation_rebuilds_live_database_without_storage_reopen() {
    let mut core = open_history_complete_reopen_refusing_node_with_schema(node(0x22), schema());
    let branch_id = branch(0x22);
    let shape = Query::from("todos").validate(&core.catalogue.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    core.create_branch(branch_id).unwrap();
    core.commit_mergeable_on_branch(
        branch_id,
        MergeableCommit::new("todos", row(0x22), 10).cells(title_cells("branch partition write")),
    )
    .unwrap();

    let rows = core
        .query_rows_on_branch(branch_id, &shape, &binding)
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        rows,
        BTreeMap::from([(row(0x22), title_cells("branch partition write"))])
    );
    assert!(
        core.branches.branch_partitions.iter().any(
            |(table, schema_version, existing)| table == "todos"
                && *schema_version == core.current_write_schema().schema
                && *existing == branch_id
        ),
        "first branch write must create a live overlay partition without reopening storage"
    );
}

#[test]
fn branch_writes_reject_unknown_and_closed_branches() {
    let (_dir, mut core) = open_history_complete_node_with_schema(node(2), schema());
    let unknown = branch(0x14);
    assert!(matches!(
        core.commit_mergeable_on_branch(
            unknown,
            MergeableCommit::new("todos", row(1), 10).cells(title_cells("implicit branch")),
        ),
        Err(Error::BranchNotFound(id)) if id == unknown
    ));

    let closed = branch(0x15);
    core.branches.branches.insert(
        closed,
        crate::node::branches::BranchRecord {
            branch_id: closed,
            created_by: AuthorId::SYSTEM,
            parent: None,
            base: None,
            state: codec::BranchState::Merged,
        },
    );
    assert!(matches!(
        core.commit_mergeable_on_branch(
            closed,
            MergeableCommit::new("todos", row(2), 11).cells(title_cells("closed branch")),
        ),
        Err(Error::BranchClosed(id)) if id == closed
    ));
    assert!(
        !core.branches.branch_partitions
            .iter()
            .any(|(_, _, existing)| *existing == unknown || *existing == closed),
        "INV-BRANCH-14: rejected branch writes must not create implicit partitions"
    );
}

#[test]
fn discard_branch_closes_branch_for_writes_and_merge_back() {
    let (_dir, mut core) = open_history_complete_node_with_schema(node(2), schema());
    let branch_id = branch(0x16);
    core.create_branch(branch_id).unwrap();
    core.commit_mergeable_on_branch(
        branch_id,
        MergeableCommit::new("todos", row(1), 10).cells(title_cells("branch write")),
    )
    .unwrap();

    core.discard_branch(branch_id).unwrap();
    assert_eq!(
        core.branch_record(branch_id).unwrap().state,
        codec::BranchState::Discarded
    );
    assert!(matches!(
        core.commit_mergeable_on_branch(
            branch_id,
            MergeableCommit::new("todos", row(2), 11).cells(title_cells("late write")),
        ),
        Err(Error::BranchClosed(id)) if id == branch_id
    ));
    assert!(matches!(
        core.merge_back_branch(branch_id),
        Err(Error::BranchClosed(id)) if id == branch_id
    ));
}

#[test]
fn merge_back_branch_emits_ordinary_target_transaction_and_leaves_branch_open() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (core_dir, mut core) = open_history_complete_node_with_schema(node(2), schema());
    let mut oracle = Oracle::new();
    commit_global_and_oracle(
        &mut writer,
        &mut core,
        &mut oracle,
        MergeableCommit::new("todos", row(1), 10).cells(title_cells("base update target")),
    );
    commit_global_and_oracle(
        &mut writer,
        &mut core,
        &mut oracle,
        MergeableCommit::new("todos", row(2), 11).cells(title_cells("base delete target")),
    );
    let branch_id = branch(0x17);
    core.create_branch(branch_id).unwrap();

    let branch_update = core
        .commit_mergeable_on_branch(
            branch_id,
            MergeableCommit::new("todos", row(1), 20).cells(title_cells("branch update")),
        )
        .unwrap();
    let branch_insert = core
        .commit_mergeable_on_branch(
            branch_id,
            MergeableCommit::new("todos", row(3), 21).cells(title_cells("branch insert")),
        )
        .unwrap();
    let branch_delete = core
        .commit_mergeable_on_branch(
            branch_id,
            MergeableCommit::new("todos", row(2), 22).deletion(DeletionEvent::Deleted),
        )
        .unwrap();

    let merger = user(0x77);
    let squash = core.merge_back_branch_as(branch_id, merger).unwrap();
    assert_eq!(
        core.branch_record(branch_id).unwrap().state,
        codec::BranchState::Open
    );
    let parent_rows = core
        .current_rows("todos", DurabilityTier::Local)
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        parent_rows,
        BTreeMap::from([
            (row(1), title_cells("branch update")),
            (row(3), title_cells("branch insert")),
        ])
    );

    let tx = core.transaction_record(squash).unwrap();
    assert_eq!(tx.made_by, merger);
    assert_eq!(tx.fate, Fate::Pending);
    assert_eq!(
        tx.branch_merge.as_ref().map(|merge| merge.source_lineage),
        Some(crate::tx::BranchLineage::Branch(branch_id))
    );
    let provenance = tx.branch_merge.as_ref().unwrap();
    assert_eq!(
        provenance.through_frontier,
        vec![branch_update, branch_insert, branch_delete]
    );
    assert_eq!(provenance.substitutions.len(), 3);
    assert!(provenance
        .substitutions
        .iter()
        .all(|substitution| substitution.sources.len() == 1));
    assert_eq!(tx.user_metadata_json, None);

    let squash_versions = core.query_versions_for_tx(squash).unwrap();
    assert_eq!(squash_versions.len(), 3);
    for substitution in &provenance.substitutions {
        assert!(
            core.validate_lww_branch_substitution(
                branch_id,
                provenance,
                substitution,
                &squash_versions,
            )
            .unwrap()
        );
    }
    let mut forged = provenance.substitutions[0].clone();
    forged.sources[0].tx_id = squash;
    assert!(
        !core
            .validate_lww_branch_substitution(
                branch_id,
                provenance,
                &forged,
                &squash_versions,
            )
            .unwrap()
    );
    assert!(
        squash_versions
            .iter()
            .all(|version| version.updated_by() == merger)
    );
    for branch_tip in [branch_update, branch_insert, branch_delete] {
        assert!(
            squash_versions
                .iter()
                .all(|version| !version.parents().contains(&branch_tip)),
            "ordinary target transaction must not expose a source branch parent"
        );
    }
    let (_receiver_dir, mut receiver) = open_node_with_schema(node(0x78), schema());
    let parent_ids = squash_versions
        .iter()
        .flat_map(|version| version.parents())
        .collect::<BTreeSet<_>>();
    for parent in parent_ids {
        receiver
            .apply_sync_message(core.commit_unit_for(parent).unwrap())
            .unwrap();
    }
    receiver
        .apply_sync_message(core.commit_unit_for(squash).unwrap())
        .unwrap();
    let received = receiver
        .transaction_record(squash)
        .unwrap()
        .branch_merge
        .unwrap();
    assert_eq!(received, provenance.clone());
    assert!(!received.substitutions.is_empty());
    let late_write = core
        .commit_mergeable_on_branch(
            branch_id,
            MergeableCommit::new("todos", row(4), 23).cells(title_cells("late write")),
        )
        .unwrap();
    let second_merge = core.merge_back_branch(branch_id).unwrap();
    let second_versions = core.query_versions_for_tx(second_merge).unwrap();
    assert_eq!(second_versions.len(), 1);
    assert_eq!(second_versions[0].row_uuid(), row(4));
    let second_provenance = core
        .transaction_record(second_merge)
        .unwrap()
        .branch_merge
        .unwrap();
    assert_eq!(second_provenance.substitutions.len(), 1);
    assert_eq!(second_provenance.substitutions[0].sources[0].tx_id, late_write);
    drop(core);
    let mut reopened = reopen_node_at(&core_dir, node(2), schema());
    assert_eq!(
        reopened
            .transaction_record(squash)
            .unwrap()
            .branch_merge
            .map(|merge| merge.source_lineage),
        Some(crate::tx::BranchLineage::Branch(branch_id))
    );
    assert_eq!(
        reopened.branch_record(branch_id).unwrap().state,
        codec::BranchState::Open
    );
}

#[test]
fn merge_back_parents_every_concurrent_target_head() {
    let (_core_dir, mut core) = open_node_with_schema(node(2), schema());
    let row_uuid = row(0x41);
    let branch_id = branch(0x41);
    core.create_root_branch(branch_id).unwrap();

    let (left, _) = core
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("left")),
        )
        .unwrap();
    let (right, _) = core
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row_uuid, 11).cells(title_cells("right")),
        )
        .unwrap();
    core.commit_mergeable_on_branch(
        branch_id,
        MergeableCommit::new("todos", row_uuid, 12).cells(title_cells("branch")),
    )
    .unwrap();

    let merge = core.merge_back_branch(branch_id).unwrap();
    let version = core
        .query_versions_for_tx(merge)
        .unwrap()
        .into_iter()
        .find(|version| version.row_uuid() == row_uuid)
        .unwrap();
    let mut parents = version.parents();
    parents.sort();
    assert_eq!(parents, vec![left, right]);
}

/// Successive partial branch writes accumulate authored presence across their
/// private ancestry. A later patch must not make an earlier explicit branch
/// edit look inherited merely because the tip's own authored set names only
/// the later patch.
///
/// Planted positive: calculate the merge from only the final branch winner's
/// authored set. The merged transaction would then lose `title=branch`.
#[test]
fn merge_back_accumulates_authored_columns_across_successive_branch_patches() {
    let schema = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("completed", ColumnType::Bool),
        ],
    )]);
    let (_writer_dir, mut writer) = open_node_with_schema(node(0x83), schema.clone());
    let (_core_dir, mut core) = open_history_complete_node_with_schema(node(0x84), schema.clone());
    let mut oracle = Oracle::new();
    let row_uuid = row(0x83);
    let (base, _) = commit_global_and_oracle(
        &mut writer,
        &mut core,
        &mut oracle,
        MergeableCommit::new("todos", row_uuid, 10).cells(BTreeMap::from([
            ("title".to_owned(), Value::String("base".to_owned())),
            ("completed".to_owned(), Value::Bool(false)),
        ])),
    );

    let branch_id = branch(0x83);
    core.create_branch(branch_id).unwrap();
    let title_patch = core
        .commit_mergeable_on_branch(
            branch_id,
            MergeableCommit::new("todos", row_uuid, 20)
                .parents(vec![base])
                .cells(BTreeMap::from([
                    ("title".to_owned(), Value::String("branch".to_owned())),
                    ("completed".to_owned(), Value::Bool(false)),
                ]))
                .authored_columns(BTreeSet::from(["title".to_owned()])),
        )
        .unwrap();
    core.commit_mergeable_on_branch(
        branch_id,
        MergeableCommit::new("todos", row_uuid, 30)
            .parents(vec![title_patch])
            .cells(BTreeMap::from([
                ("title".to_owned(), Value::String("branch".to_owned())),
                ("completed".to_owned(), Value::Bool(true)),
            ]))
            .authored_columns(BTreeSet::from(["completed".to_owned()])),
    )
    .unwrap();

    core.merge_back_branch(branch_id).unwrap();
    let rows = core.current_rows("todos", DurabilityTier::Local).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].test_cells_by_descriptor(),
        BTreeMap::from([
            ("title".to_owned(), Value::String("branch".to_owned())),
            ("completed".to_owned(), Value::Bool(true)),
        ])
    );
}

#[test]
fn branch_target_is_canonical_atomic_transaction_state_across_reopen() {
    let (core_dir, mut core) = open_history_complete_node_with_schema(node(2), schema());
    let branch_id = branch(0x61);
    core.create_root_branch(branch_id).unwrap();
    let tx_id = core
        .commit_mergeable_many_on_branch(
            branch_id,
            vec![
                MergeableCommit::new("todos", row(0x61), 10)
                    .cells(title_cells("one")),
                MergeableCommit::new("todos", row(0x62), 10)
                    .cells(title_cells("two")),
            ],
        )
        .unwrap();

    let record = core.transaction_record(tx_id).unwrap();
    assert_eq!(
        record.target_lineage,
        crate::tx::BranchLineage::Branch(branch_id)
    );
    assert_eq!(record.n_total_writes, 2);
    let SyncMessage::CommitUnit { tx, versions } = core.commit_unit_for(tx_id).unwrap() else {
        panic!("commit unit expected");
    };
    assert_eq!(tx.target_lineage, crate::tx::BranchLineage::Branch(branch_id));
    assert_eq!(versions.len(), 2);
    core.finalize_local_mergeable_commit(tx_id).unwrap();
    assert_eq!(core.transaction_record(tx_id).unwrap().fate, Fate::Accepted);
    assert!(
        core.current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .all(|current| ![row(0x61), row(0x62)].contains(&current.row_uuid()))
    );

    drop(core);
    let mut reopened = reopen_node_at(&core_dir, node(2), schema());
    assert_eq!(
        reopened.transaction_record(tx_id).unwrap().target_lineage,
        crate::tx::BranchLineage::Branch(branch_id)
    );
    assert_eq!(reopened.query_versions_for_tx(tx_id).unwrap().len(), 2);
}

#[test]
fn root_branch_transitive_merge_expands_provenance_without_echo() {
    let (_core_dir, mut core) = open_history_complete_node_with_schema(node(2), schema());
    let branch_b = branch(0x62);
    let branch_c = branch(0x63);
    core.create_root_branch(branch_b).unwrap();
    core.create_root_branch(branch_c).unwrap();

    let root_native = core
        .commit_mergeable(
            MergeableCommit::new("todos", row(0x70), 10).cells(title_cells("root")),
        )
        .unwrap();
    let root_to_b = core
        .merge_lineage_into(
            crate::tx::BranchLineage::Root,
            crate::tx::BranchLineage::Branch(branch_b),
        )
        .unwrap();
    assert_eq!(
        core.transaction_record(root_to_b).unwrap().target_lineage,
        crate::tx::BranchLineage::Branch(branch_b)
    );
    let b_to_c = core
        .merge_lineage_into(
            crate::tx::BranchLineage::Branch(branch_b),
            crate::tx::BranchLineage::Branch(branch_c),
        )
        .unwrap();
    let b_to_c_provenance = core
        .transaction_record(b_to_c)
        .unwrap()
        .branch_merge
        .unwrap();
    assert_eq!(b_to_c_provenance.substitutions.len(), 1);
    assert_eq!(b_to_c_provenance.substitutions[0].sources[0].tx_id, root_native);
    assert_eq!(
        b_to_c_provenance.substitutions[0].sources[0].lineage,
        crate::tx::BranchLineage::Root
    );

    core.commit_mergeable_on_branch(
        branch_c,
        MergeableCommit::new("todos", row(0x71), 20).cells(title_cells("from-c")),
    )
    .unwrap();
    let c_to_root = core
        .merge_lineage_into(
            crate::tx::BranchLineage::Branch(branch_c),
            crate::tx::BranchLineage::Root,
        )
        .unwrap();
    let versions = core.query_versions_for_tx(c_to_root).unwrap();
    assert_eq!(versions.len(), 1, "root-originated row must not echo home");
    assert_eq!(versions[0].row_uuid(), row(0x71));
    assert!(versions[0].parents().iter().all(|parent| {
        *parent != root_to_b && *parent != b_to_c
    }));
}

#[test]
fn ordinary_commit_unit_routes_to_branch_target_without_touching_root() {
    let (_writer_dir, mut writer) = open_history_complete_node_with_schema(node(1), schema());
    let (_receiver_dir, mut receiver) =
        open_history_complete_node_with_schema(node(2), schema());
    let branch_id = branch(0x64);
    writer.create_root_branch(branch_id).unwrap();
    receiver.create_root_branch(branch_id).unwrap();
    let tx_id = writer
        .commit_mergeable_on_branch(
            branch_id,
            MergeableCommit::new("todos", row(0x81), 10).cells(title_cells("synced")),
        )
        .unwrap();

    let unit = writer.commit_unit_for(tx_id).unwrap();
    let updates = receiver.apply_sync_message(unit.clone()).unwrap();
    assert!(updates.iter().any(|message| matches!(
        message,
        SyncMessage::FateUpdate {
            tx_id: candidate,
            fate: Fate::Accepted,
            ..
        } if *candidate == tx_id
    )));
    assert_eq!(
        receiver.transaction_record(tx_id).unwrap().target_lineage,
        crate::tx::BranchLineage::Branch(branch_id)
    );
    let shape = Query::from("todos")
        .validate(&receiver.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let branch_rows = receiver
        .query_rows_on_branch(branch_id, &shape, &binding)
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    assert_eq!(branch_rows.get(&row(0x81)), Some(&title_cells("synced")));
    assert!(
        receiver
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .all(|current| current.row_uuid() != row(0x81))
    );
    let root_tx = receiver
        .commit_mergeable(
            MergeableCommit::new("todos", row(0x81), 30).cells(title_cells("root-after-branch")),
        )
        .unwrap();
    receiver.finalize_local_mergeable_commit(root_tx).unwrap();
    receiver
        .assert_merge_heads_match_history_for_test("todos", row(0x81))
        .unwrap();
    assert_eq!(
        receiver
            .visible_current_cells("todos", row(0x81))
            .unwrap()
            .unwrap(),
        title_cells("root-after-branch")
    );
    receiver.apply_sync_message(unit).unwrap();
    let SyncMessage::CommitUnit {
        mut tx,
        versions,
    } = writer.commit_unit_for(tx_id).unwrap()
    else {
        panic!("commit unit expected");
    };
    tx.target_lineage = crate::tx::BranchLineage::Root;
    assert!(matches!(
        receiver.apply_sync_message(SyncMessage::CommitUnit { tx, versions }),
        Err(Error::ConflictingCommitUnit(candidate)) if candidate == tx_id
    ));
}

#[test]
fn invalid_branch_targets_do_not_persist_poison_partitions() {
    let (_writer_dir, mut writer) = open_history_complete_node_with_schema(node(1), schema());
    let branch_id = branch(0x65);
    writer.create_root_branch(branch_id).unwrap();
    let tx_id = writer
        .commit_mergeable_on_branch(
            branch_id,
            MergeableCommit::new("todos", row(0x82), 10).cells(title_cells("candidate")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = writer.commit_unit_for(tx_id).unwrap() else {
        panic!("expected commit unit");
    };
    let version = versions[0].clone();

    let (unknown_schema_dir, mut unknown_schema) =
        open_history_complete_node_with_schema(node(2), schema());
    unknown_schema.create_root_branch(branch_id).unwrap();
    let unknown_schema_version = SchemaVersionId(uuid::Uuid::from_bytes([0xee; 16]));
    let unknown_schema_record = VersionRecord::new(
        version.table(),
        unknown_schema_version,
        version.record().clone(),
    );
    unknown_schema
        .apply_sync_message(SyncMessage::CommitUnit {
            tx: tx.clone(),
            versions: vec![unknown_schema_record],
        })
        .unwrap();
    assert!(
        !unknown_schema
            .branches
            .branch_partitions
            .iter()
            .any(|(_, _, existing)| *existing == branch_id)
    );
    drop(unknown_schema);
    let reopened = reopen_node_at(&unknown_schema_dir, node(2), schema());
    assert!(
        !reopened
            .branches
            .branch_partitions
            .iter()
            .any(|(_, _, existing)| *existing == branch_id)
    );

    let (unknown_table_dir, mut unknown_table) =
        open_history_complete_node_with_schema(node(3), schema());
    unknown_table.create_root_branch(branch_id).unwrap();
    let unknown_table_record = VersionRecord::new(
        "unknown_table",
        version.schema_version(),
        version.record().clone(),
    );
    assert!(
        unknown_table
            .apply_sync_message(SyncMessage::CommitUnit {
                tx: tx.clone(),
                versions: vec![unknown_table_record],
            })
            .is_err()
    );
    assert!(
        !unknown_table
            .branches
            .branch_partitions
            .iter()
            .any(|(_, _, existing)| *existing == branch_id)
    );
    drop(unknown_table);
    let reopened = reopen_node_at(&unknown_table_dir, node(3), schema());
    assert!(
        !reopened
            .branches
            .branch_partitions
            .iter()
            .any(|(_, _, existing)| *existing == branch_id)
    );

    let (oversized_dir, mut oversized) = open_history_complete_node_with_schema(node(4), schema());
    oversized.create_root_branch(branch_id).unwrap();
    let oversized_versions =
        vec![version; crate::protocol_limits::MAX_COMMIT_UNIT_VERSIONS + 1];
    let mut oversized_tx = tx;
    oversized_tx.n_total_writes = oversized_versions.len() as u32;
    let updates = oversized
        .apply_sync_message(SyncMessage::CommitUnit {
            tx: oversized_tx,
            versions: oversized_versions,
        })
        .unwrap();
    assert!(updates.iter().any(|update| matches!(
        update,
        SyncMessage::FateUpdate {
            fate: Fate::Rejected(RejectionReason::MalformedCommit(_)),
            ..
        }
    )));
    assert!(
        !oversized
            .branches
            .branch_partitions
            .iter()
            .any(|(_, _, existing)| *existing == branch_id)
    );
    drop(oversized);
    let reopened = reopen_node_at(&oversized_dir, node(4), schema());
    assert!(
        !reopened
            .branches
            .branch_partitions
            .iter()
            .any(|(_, _, existing)| *existing == branch_id)
    );
}

#[test]
fn ordinary_branch_target_ingest_applies_target_authorization() {
    let schema = branch_rls_schema();
    let (_writer_dir, mut writer) =
        open_history_complete_node_with_schema(node(1), schema.clone());
    let (_receiver_dir, mut receiver) = open_history_complete_node_with_schema(node(2), schema);
    let branch_id = branch(0x65);
    let allowed = user(0xa1);
    let denied = user(0xb2);
    seed_branch_acl(&mut writer, branch_id, allowed, row(0x90), 1);
    seed_branch_acl(&mut receiver, branch_id, allowed, row(0x90), 1);
    writer.create_branch(branch_id).unwrap();
    receiver.create_branch(branch_id).unwrap();
    let tx_id = writer
        .commit_mergeable_on_branch(
            branch_id,
            MergeableCommit::new("todos", row(0x91), 10)
                .made_by(allowed)
                .cells(owner_cells(allowed, "candidate")),
        )
        .unwrap();
    let SyncMessage::CommitUnit {
        mut tx,
        versions,
    } = writer.commit_unit_for(tx_id).unwrap()
    else {
        panic!("commit unit expected");
    };
    tx.made_by = denied;
    let updates = receiver
        .apply_sync_message(SyncMessage::CommitUnit { tx, versions })
        .unwrap();
    assert!(updates.iter().any(|message| matches!(
        message,
        SyncMessage::FateUpdate {
            tx_id: candidate,
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            ..
        } if *candidate == tx_id
    )));
    let shape = Query::from("todos")
        .validate(&receiver.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    assert!(
        receiver
            .query_rows_on_branch(branch_id, &shape, &binding)
            .unwrap()
            .into_iter()
            .all(|current| current.row_uuid() != row(0x91))
    );
}

#[test]
fn merge_back_fails_whole_calculation_when_source_row_is_not_readable() {
    let (_core_dir, mut core) =
        open_history_complete_node_with_schema(node(2), owner_policy_schema());
    let owner = user(0x71);
    let outsider = user(0x72);
    let branch_id = branch(0x42);
    core.create_root_branch(branch_id).unwrap();
    core.commit_mergeable_on_branch(
        branch_id,
        MergeableCommit::new("todos", row(0x42), 10)
            .made_by(owner)
            .cells(owner_cells(owner, "private")),
    )
    .unwrap();

    assert!(matches!(
        core.merge_back_branch_as(branch_id, outsider),
        Err(Error::AuthorizationDenied)
    ));
    assert!(core.merge_back_branch_as(branch_id, owner).is_ok());
}

#[test]
fn merge_back_fails_closed_for_strategy_without_contribution_capabilities() {
    let (_core_dir, mut core) =
        open_history_complete_node_with_schema(node(2), counter_schema());
    let branch_id = branch(0x43);
    core.create_root_branch(branch_id).unwrap();
    core.commit_mergeable_on_branch(
        branch_id,
        MergeableCommit::new("counters", row(0x43), 10)
            .cell("count", Value::U64(1))
            .cell("title", v("branch")),
    )
    .unwrap();
    assert!(matches!(
        core.merge_back_branch(branch_id),
        Err(Error::BranchMergeCalculation(
            "column strategy lacks branch contribution capabilities"
        ))
    ));
}

#[derive(Clone, Copy)]
enum MergeBackOracleAction {
    Update(RowUuid, &'static str),
    Delete(RowUuid),
    Restore(RowUuid),
}

struct MergeBackOracleRun {
    merged: NodeState<RocksDbStorage>,
    direct: NodeState<RocksDbStorage>,
    merge_back_tx: TxId,
    direct_txs: Vec<TxId>,
}

type MergeBackVersionSummary = (
    RowUuid,
    VersionLayer,
    Option<DeletionEvent>,
    BTreeMap<String, Value>,
    Vec<TxId>,
);

#[test]
fn merge_back_seeded_net_effect_oracle_matches_direct_parent_rows_and_deletions() {
    for seed in 0..32_u64 {
        let run = run_merge_back_oracle_seed(seed, false);
        assert_merge_back_visible_rows_match(seed, run.merged, run.direct);
    }
}

#[test]
fn merge_back_seeded_strict_oracle_matches_direct_parent_versions() {
    for seed in 0..32_u64 {
        let run = run_merge_back_oracle_seed(seed, false);
        assert_merge_back_versions_match(seed, run);
    }
}

#[test]
fn merge_back_seeded_restore_oracle_matches_direct_parent_rows_and_deletions() {
    for seed in 0..32_u64 {
        let run = run_merge_back_oracle_seed(seed, true);
        assert_merge_back_visible_rows_match(seed, run.merged, run.direct);
    }
}

#[test]
fn merge_back_seeded_restore_oracle_matches_direct_parent_versions() {
    for seed in 0..32_u64 {
        let run = run_merge_back_oracle_seed(seed, true);
        assert_merge_back_versions_match(seed, run);
    }
}

fn run_merge_back_oracle_seed(seed: u64, include_restore: bool) -> MergeBackOracleRun {
    let branch_id = branch(0x30 + seed as u8);
    let (_merge_writer_dir, mut merge_writer) = open_node_with_uuid(node(1));
    let (_direct_writer_dir, mut direct_writer) = open_node_with_uuid(node(1));
    let (_merged_dir, mut merged) = open_history_complete_node_with_schema(node(2), schema());
    let (_direct_dir, mut direct) = open_history_complete_node_with_schema(node(2), schema());
    let mut merge_oracle = Oracle::new();
    let mut direct_oracle = Oracle::new();
    let mut branch_parents = BTreeMap::<(RowUuid, VersionLayer), TxId>::new();
    {
        let mut parent_pair = ParentPair {
            merge_writer: &mut merge_writer,
            direct_writer: &mut direct_writer,
            merged: &mut merged,
            direct: &mut direct,
            merge_oracle: &mut merge_oracle,
            direct_oracle: &mut direct_oracle,
        };

        parent_pair.seed_parent(row(0x31), 10, "base-one");
        parent_pair.seed_parent(row(0x32), 11, "base-two");
        parent_pair.seed_parent(row(0x33), 12, "base-delete-target");
        parent_pair.seed_parent(row(0x37), 13, "base-restore-target");
        parent_pair.seed_parent(row(0x38), 14, "base-update-delete-target");
        parent_pair.seed_parent(row(0x39), 15, "base-overlap");

        if seed & 8 == 8 {
            parent_pair.commit_parent_local(row(0x31), 16, format!("parent-pre-{seed}"));
        }
        if seed & 16 == 16 {
            parent_pair.commit_parent_local(
                row(0x39),
                17,
                format!("parent-overlap-pre-{seed}"),
            );
        }

        parent_pair.merged.create_branch(branch_id).unwrap();

        for (idx, action) in merge_back_seed_actions(seed, include_restore)
            .into_iter()
            .enumerate()
        {
            let now_ms = 20 + idx as u64;
            let previous = branch_parents.get(&(action.row(), action.layer())).copied();
            let branch_tx = parent_pair
                .merged
                .commit_mergeable_on_branch(
                    branch_id,
                    action.commit(now_ms, previous.into_iter().collect()),
                )
                .unwrap();
            branch_parents.insert((action.row(), action.layer()), branch_tx);
        }

        if seed & 1 == 1 {
            parent_pair.commit_parent_local(row(0x34), 3_000 + seed, "parent-only");
        }
        if seed & 2 == 2 {
            let title = format!("parent-concurrent-{seed}");
            parent_pair.commit_parent_local(row(0x32), 4_000 + seed, title);
        }
        if seed & 4 == 4 {
            parent_pair.commit_parent_local(row(0x31), 5_000 + seed, format!("parent-post-{seed}"));
        }
        if seed & 16 == 16 {
            parent_pair.commit_parent_local(
                row(0x39),
                6_000 + seed,
                format!("parent-overlap-post-{seed}"),
            );
        }
    }

    let merge_back_tx = merged.merge_back_branch(branch_id).unwrap();
    let direct_txs = apply_direct_net_effects(&mut direct, seed, include_restore, branch_parents);
    MergeBackOracleRun {
        merged,
        direct,
        merge_back_tx,
        direct_txs,
    }
}

struct ParentPair<'a> {
    merge_writer: &'a mut NodeState<RocksDbStorage>,
    direct_writer: &'a mut NodeState<RocksDbStorage>,
    merged: &'a mut NodeState<RocksDbStorage>,
    direct: &'a mut NodeState<RocksDbStorage>,
    merge_oracle: &'a mut Oracle,
    direct_oracle: &'a mut Oracle,
}

impl ParentPair<'_> {
    fn seed_parent(&mut self, row_uuid: RowUuid, now_ms: u64, title: impl Into<String>) {
        let title = title.into();
        commit_global_and_oracle(
            self.merge_writer,
            self.merged,
            self.merge_oracle,
            MergeableCommit::new("todos", row_uuid, now_ms).cells(title_cells(title.clone())),
        );
        commit_global_and_oracle(
            self.direct_writer,
            self.direct,
            self.direct_oracle,
            MergeableCommit::new("todos", row_uuid, now_ms).cells(title_cells(title)),
        );
    }

    fn commit_parent_local(&mut self, row_uuid: RowUuid, now_ms: u64, title: impl Into<String>) {
        let title = title.into();
        self.merged
            .commit_mergeable(
                MergeableCommit::new("todos", row_uuid, now_ms)
                    .cells(title_cells(title.clone())),
            )
            .unwrap();
        self.direct
            .commit_mergeable(
                MergeableCommit::new("todos", row_uuid, now_ms).cells(title_cells(title)),
            )
            .unwrap();
    }
}

fn merge_back_seed_actions(seed: u64, include_restore: bool) -> Vec<MergeBackOracleAction> {
    let multi_first = if seed & 4 == 4 {
        "branch-one-b"
    } else {
        "branch-one-a"
    };
    let mut actions = vec![
        MergeBackOracleAction::Update(row(0x31), multi_first),
        MergeBackOracleAction::Update(row(0x35), "branch-insert"),
        MergeBackOracleAction::Delete(row(0x33)),
        MergeBackOracleAction::Update(row(0x31), "branch-one-final"),
        MergeBackOracleAction::Update(row(0x36), "branch-second-insert"),
        MergeBackOracleAction::Update(row(0x38), "branch-update-before-delete"),
        MergeBackOracleAction::Delete(row(0x38)),
        MergeBackOracleAction::Update(row(0x39), "branch-overlap-first"),
        MergeBackOracleAction::Update(row(0x39), "branch-overlap-final"),
    ];
    if include_restore {
        actions.extend([
            MergeBackOracleAction::Delete(row(0x37)),
            MergeBackOracleAction::Restore(row(0x37)),
            MergeBackOracleAction::Update(row(0x37), "branch-restored"),
        ]);
    }
    if seed & 1 == 1 {
        actions.swap(1, 2);
    }
    if seed & 2 == 2 {
        actions.push(MergeBackOracleAction::Delete(row(0x35)));
    }
    actions
}

impl MergeBackOracleAction {
    fn row(self) -> RowUuid {
        match self {
            MergeBackOracleAction::Update(row_uuid, _)
            | MergeBackOracleAction::Delete(row_uuid)
            | MergeBackOracleAction::Restore(row_uuid) => row_uuid,
        }
    }

    fn commit(self, now_ms: u64, parents: Vec<TxId>) -> MergeableCommit {
        let commit = MergeableCommit::new("todos", self.row(), now_ms).parents(parents);
        match self {
            MergeBackOracleAction::Update(_, title) => commit.cells(title_cells(title)),
            MergeBackOracleAction::Delete(_) => commit.deletion(DeletionEvent::Deleted),
            MergeBackOracleAction::Restore(_) => commit.deletion(DeletionEvent::Restored),
        }
    }

    fn layer(self) -> VersionLayer {
        match self {
            MergeBackOracleAction::Update(_, _) => VersionLayer::Content,
            MergeBackOracleAction::Delete(_) | MergeBackOracleAction::Restore(_) => {
                VersionLayer::Deletion
            }
        }
    }
}

fn apply_direct_net_effects(
    direct: &mut NodeState<RocksDbStorage>,
    seed: u64,
    include_restore: bool,
    branch_parents: BTreeMap<(RowUuid, VersionLayer), TxId>,
) -> Vec<TxId> {
    let mut effects = Vec::from([
        MergeBackOracleAction::Update(row(0x31), "branch-one-final"),
        MergeBackOracleAction::Delete(row(0x33)),
        MergeBackOracleAction::Update(row(0x35), "branch-insert"),
        MergeBackOracleAction::Update(row(0x36), "branch-second-insert"),
        MergeBackOracleAction::Update(row(0x38), "branch-update-before-delete"),
        MergeBackOracleAction::Delete(row(0x38)),
        MergeBackOracleAction::Update(row(0x39), "branch-overlap-final"),
    ]);
    if include_restore {
        effects.extend([
            MergeBackOracleAction::Restore(row(0x37)),
            MergeBackOracleAction::Update(row(0x37), "branch-restored"),
        ]);
    }
    if seed & 2 == 2 {
        effects.push(MergeBackOracleAction::Delete(row(0x35)));
    }

    let mut direct_txs = Vec::new();
    for (idx, effect) in effects.into_iter().enumerate() {
        let layer = effect.layer();
        let parent = direct
            .query_local_layer_winner("todos", effect.row(), layer)
            .unwrap()
            .map(|winner| direct.version_tx_id(&winner).unwrap());
        let parents = parent
            .into_iter()
            .chain(branch_parents.get(&(effect.row(), layer)).copied())
            .collect();
        direct_txs.push(
            direct
                .commit_mergeable(effect.commit(80 + seed * 10 + idx as u64, parents))
                .unwrap(),
        );
    }
    direct_txs
}

fn assert_merge_back_visible_rows_match(
    seed: u64,
    mut merged: NodeState<RocksDbStorage>,
    mut direct: NodeState<RocksDbStorage>,
) {
    let merged_rows = current_todo_rows(&mut merged);
    let direct_rows = current_todo_rows(&mut direct);
    assert_eq!(
        merged_rows, direct_rows,
        "seed {seed}: merge-back current rows must equal direct net effects"
    );
    for row_uuid in [
        row(0x31),
        row(0x32),
        row(0x33),
        row(0x35),
        row(0x36),
        row(0x37),
        row(0x38),
        row(0x39),
    ] {
        assert_eq!(
            layer_summary(&mut merged, row_uuid, VersionLayer::Deletion),
            layer_summary(&mut direct, row_uuid, VersionLayer::Deletion),
            "seed {seed}: deletion-register behavior differs for {row_uuid:?}"
        );
    }
}

fn assert_merge_back_versions_match(seed: u64, mut run: MergeBackOracleRun) {
    let merged = tx_version_summary(&mut run.merged, run.merge_back_tx);
    let direct = txs_version_summary(&mut run.direct, &run.direct_txs);
    let merged_effects = merged
        .iter()
        .map(|(row, layer, deletion, cells, _)| (*row, *layer, *deletion, cells.clone()))
        .collect::<Vec<_>>();
    let direct_effects = direct
        .iter()
        .map(|(row, layer, deletion, cells, _)| (*row, *layer, *deletion, cells.clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        merged_effects, direct_effects,
        "seed {seed}: merge-back payload must equal direct target-local effects"
    );
    let provenance = run
        .merged
        .transaction_record(run.merge_back_tx)
        .unwrap()
        .branch_merge
        .unwrap();
    let source_txs = provenance
        .through_frontier
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    assert!(
        merged
            .iter()
            .flat_map(|(_, _, _, _, parents)| parents)
            .all(|parent| !source_txs.contains(parent)),
        "seed {seed}: source lineage must not appear in target causal parents"
    );
    assert_eq!(
        provenance.substitutions.len(),
        merged.len(),
        "seed {seed}: every emitted LWW/register effect needs field provenance"
    );
}

fn current_todo_rows(node: &mut NodeState<RocksDbStorage>) -> BTreeMap<RowUuid, BTreeMap<String, Value>> {
    node.current_rows("todos", DurabilityTier::Local)
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect()
}

fn layer_summary(
    node: &mut NodeState<RocksDbStorage>,
    row_uuid: RowUuid,
    layer: VersionLayer,
) -> Option<(Option<DeletionEvent>, BTreeMap<String, Value>)> {
    let table = node.table("todos").unwrap().clone();
    node.query_local_layer_winner("todos", row_uuid, layer)
        .unwrap()
        .map(|version| (version.deletion(), version.cells(&table).unwrap()))
}

fn tx_version_summary(node: &mut NodeState<RocksDbStorage>, tx_id: TxId) -> Vec<MergeBackVersionSummary> {
    let table = node.table("todos").unwrap().clone();
    node.query_versions_for_tx(tx_id)
        .unwrap()
        .into_iter()
        .map(|version| {
            (
                version.row_uuid(),
                version.layer(),
                version.deletion(),
                version.cells(&table).unwrap(),
                {
                    let mut parents = version.parents();
                    parents.sort();
                    parents
                },
            )
        })
        .collect()
}

fn txs_version_summary(
    node: &mut NodeState<RocksDbStorage>,
    tx_ids: &[TxId],
) -> Vec<MergeBackVersionSummary> {
    let mut summaries = tx_ids
        .iter()
        .flat_map(|tx_id| tx_version_summary(node, *tx_id))
        .collect::<Vec<_>>();
    summaries.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    summaries
}
