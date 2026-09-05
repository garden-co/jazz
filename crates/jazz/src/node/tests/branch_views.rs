fn branch_view_schema() -> JazzSchema {
    build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .column("branch_id", PublicColumnType::Uuid)
                    .column("title", PublicColumnType::Text)
                    .column("owner", PublicColumnType::Uuid)
                    .branch_by("branch_id")
                    .index_only(["title"])
                    .policies(public_owner_policies("owner")),
            )
            .table(
                PublicTableSchemaBuilder::new("users")
                    .column("name", PublicColumnType::Text)
                    .index_only(["name"]),
            ),
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

/// A first branch-head overlay has no legal cross-branch history parent.
/// This receipt exercises the authority boundary directly because the
/// observable contract is an accept/reject fate for an incoming commit unit:
/// the source proof must survive the client's pending storage and relay
/// transport, yet must not become a causal parent.
#[test]
fn branch_view_copy_evidence_authorizes_exact_inherited_source_without_parent() {
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("branch_id", PublicColumnType::Uuid)
                .column("title", PublicColumnType::Text)
                .column("owner", PublicColumnType::Uuid)
                .branch_by("branch_id")
                .policies(public_all_policies().with_select(public_claim_eq("owner", "sub"))),
        ),
    );
    let (_dir, mut authority) =
        open_history_complete_node_with_schema(NodeUuid::from_bytes([0x4a; 16]), schema.clone());
    let allowed = AuthorSubject::for_test_bytes([0x4b; 16]);
    let denied = AuthorSubject::for_test_bytes([0x4c; 16]);
    for subject in [allowed, denied] {
        authority.set_test_provider_claims(
            subject,
            BTreeMap::from([("sub".to_owned(), Value::Uuid(subject.test_uuid()))]),
        );
    }
    let source_row = row(0x4d);
    let base = branch_selector(0x4e);
    let head = branch_selector(0x4f);
    let head_value = uuid::Uuid::from_bytes([0x4f; 16]);
    let source_tx = authority
        .commit_mergeable_settled(
            MergeableCommit::new("todos", source_row, 10)
                .branch(base.clone())
                .cells(BTreeMap::from([
                    ("title".to_owned(), v("base")),
                    ("owner".to_owned(), Value::Uuid(allowed.test_uuid())),
                ])),
        )
        .unwrap();
    let table = schema.tables.iter().find(|table| table.name == "todos").unwrap();
    let physical_table_id = authority
        .physical_table_id_for_schema(schema.version_id(), "todos")
        .unwrap();
    // The authority has moved its write pointer through a table rename before
    // it receives the old-authored branch version below. The intent's stable
    // physical table id plus authored schema must resolve `todos`, rather than
    // interpreting the evidence through the current `tasks` spelling.
    let renamed_schema = build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("tasks")
                .column("branch_id", PublicColumnType::Uuid)
                .column("title", PublicColumnType::Text)
                .column("owner", PublicColumnType::Uuid)
                .branch_by("branch_id")
                .policies(public_all_policies().with_select(public_claim_eq("owner", "sub"))),
        ),
    );
    let renamed = SchemaVersion::new(renamed_schema);
    publish_schema_lineage(
        &mut authority,
        renamed.clone(),
        MigrationLens::new(
            schema.version_id(),
            renamed.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "tasks".to_owned(),
                ops: vec![LensOp::RenameTable {
                    from: "todos".to_owned(),
                    to: "tasks".to_owned(),
                }],
            }],
        )
        .expect("valid table-rename lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    authority
        .apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
            author: AuthorSubject::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 1,
                schema: renamed.id,
            },
        })
        .unwrap();
    let base_key = schema.project_branch_view_selector(table, &base).unwrap().0;
    let head_key = schema.project_branch_view_selector(table, &head).unwrap().0;
    let make_unit = |tx_id: TxId,
                     subject: AuthorSubject,
                     source_version: TxId,
                     target_key: BranchKey,
                     target_value: uuid::Uuid,
                     copy_base: crate::tx::BranchViewCopyBase| {
        let version = VersionRecord::from_cells(
            table,
            schema.version_id(),
            source_row,
            Vec::new(),
            subject,
            tx_id.time.physical_ms(),
            subject,
            tx_id.time.physical_ms(),
            &BTreeMap::from([
                ("branch_id".to_owned(), Value::Uuid(target_value)),
                ("title".to_owned(), v("head patch")),
                ("owner".to_owned(), Value::Uuid(allowed.test_uuid())),
            ]),
            None,
        )
        .unwrap()
        .with_branch_key(target_key.clone());
        let evidence = crate::tx::BranchViewCopyEvidence {
            version: 1,
            head: target_key,
            base: copy_base,
            table: "todos".to_owned(),
            row_uuid: source_row,
            source_version,
        };
        let tx = Transaction {
            tx_id,
            kind: TxKind::Mergeable,
            n_total_writes: 1,
            made_by: subject,
            permission_subject: None,
            base_snapshot: None,
            row_read_set: None,
            absent_read_set: None,
            predicate_read_set: None,
            user_metadata_json: None,
            contribution_merge: Some(crate::tx::ContributionMergeProvenance {
                source: BranchKey::default(),
                target: BranchKey::default(),
                substitutions: Vec::new(),
                branch_view_copies: vec![evidence.clone()],
                branch_write_intents: vec![crate::tx::BranchWriteIntent {
                    version: 1,
                    physical_table_id,
                    authored_schema: schema.version_id(),
                    row_uuid: source_row,
                    head: evidence.head.clone(),
                    operation: crate::tx::BranchWriteOperation::ViewUpdateCopy(evidence),
                }],
            }),
        };
        (tx, vec![version])
    };

    let (allowed_tx, allowed_versions) = make_unit(
        TxId::new(TxTime::from(20), node(0x50)),
        allowed,
        source_tx,
        head_key.clone(),
        head_value,
        crate::tx::BranchViewCopyBase::Current(base_key.clone()),
    );
    let allowed_outcome =
        crate::db::block_on(authority.ingest_commit_unit(allowed_tx.clone(), allowed_versions, 20))
            .unwrap();
    settle_outcome(&mut authority, allowed_outcome).unwrap();
    assert!(matches!(
        authority.transaction_state_settled(allowed_tx.tx_id),
        Some((Fate::Accepted, Some(_), DurabilityTier::Global))
    ));

    // A public transaction can mix an inherited first-head copy with a plain
    // first-head insert.  Both operation identities are bound to their exact
    // versions, so authority admits the whole unit (and can replay it) rather
    // than treating the copy as a causal dependency or a best-effort sidecar.
    let batch_head = branch_selector(0x5b);
    let batch_head_key = schema
        .project_branch_view_selector(table, &batch_head)
        .unwrap()
        .0;
    let batch_insert_row = row(0x5c);
    let (mut batch_tx, mut batch_versions) = make_unit(
        TxId::new(TxTime::from(21), node(0x5d)),
        allowed,
        source_tx,
        batch_head_key.clone(),
        uuid::Uuid::from_bytes([0x5b; 16]),
        crate::tx::BranchViewCopyBase::Current(base_key.clone()),
    );
    batch_tx.n_total_writes = 2;
    batch_versions.push(
        VersionRecord::from_cells(
            table,
            schema.version_id(),
            batch_insert_row,
            Vec::new(),
            allowed,
            batch_tx.tx_id.time.physical_ms(),
            allowed,
            batch_tx.tx_id.time.physical_ms(),
            &BTreeMap::from([
                ("branch_id".to_owned(), Value::Uuid(uuid::Uuid::from_bytes([0x5b; 16]))),
                ("title".to_owned(), v("exact head insert")),
                ("owner".to_owned(), Value::Uuid(allowed.test_uuid())),
            ]),
            None,
        )
        .unwrap()
        .with_branch_key(batch_head_key.clone()),
    );
    batch_tx
        .contribution_merge
        .as_mut()
        .unwrap()
        .branch_write_intents
        .push(crate::tx::BranchWriteIntent {
            version: 1,
            physical_table_id,
            authored_schema: schema.version_id(),
            row_uuid: batch_insert_row,
            head: batch_head_key.clone(),
            operation: crate::tx::BranchWriteOperation::ExactHeadInsert,
        });
    batch_tx
        .contribution_merge
        .as_mut()
        .unwrap()
        .branch_write_intents
        .sort_by(|left, right| {
            (left.physical_table_id, left.authored_schema, left.row_uuid, &left.head).cmp(&(
                right.physical_table_id,
                right.authored_schema,
                right.row_uuid,
                &right.head,
            ))
        });
    let batch_outcome = crate::db::block_on(authority.ingest_commit_unit(
        batch_tx.clone(),
        batch_versions.clone(),
        21,
    ))
    .unwrap();
    settle_outcome(&mut authority, batch_outcome).unwrap();
    assert!(matches!(
        authority.transaction_state_settled(batch_tx.tx_id),
        Some((Fate::Accepted, Some(_), DurabilityTier::Global))
    ));
    let batch_versions_for_reopen = batch_versions.clone();
    let replay_outcome = crate::db::block_on(authority.ingest_commit_unit(
        batch_tx.clone(),
        batch_versions,
        21,
    ))
    .unwrap();
    settle_outcome(&mut authority, replay_outcome).unwrap();
    assert!(matches!(
        authority.transaction_state_settled(batch_tx.tx_id),
        Some((Fate::Accepted, Some(_), DurabilityTier::Global))
    ));
    assert!(authority
        .visible_current_cells_in_branch("tasks", &batch_head, source_row)
        .unwrap()
        .is_some());
    assert!(authority
        .visible_current_cells_in_branch("tasks", &batch_head, batch_insert_row)
        .unwrap()
        .is_some());

    // Stripping just the exact-head member's descriptor rejects the complete
    // unit.  Neither the authorized view copy nor the ordinary insert may
    // become durable on its own.
    let rejected_head = branch_selector(0x5e);
    let rejected_head_key = schema
        .project_branch_view_selector(table, &rejected_head)
        .unwrap()
        .0;
    let rejected_insert_row = row(0x5f);
    let (mut rejected_tx, mut rejected_versions) = make_unit(
        TxId::new(TxTime::from(24), node(0x60)),
        allowed,
        source_tx,
        rejected_head_key.clone(),
        uuid::Uuid::from_bytes([0x5e; 16]),
        crate::tx::BranchViewCopyBase::Current(base_key.clone()),
    );
    rejected_tx.n_total_writes = 2;
    rejected_versions.push(
        VersionRecord::from_cells(
            table,
            schema.version_id(),
            rejected_insert_row,
            Vec::new(),
            allowed,
            rejected_tx.tx_id.time.physical_ms(),
            allowed,
            rejected_tx.tx_id.time.physical_ms(),
            &BTreeMap::from([
                ("branch_id".to_owned(), Value::Uuid(uuid::Uuid::from_bytes([0x5e; 16]))),
                ("title".to_owned(), v("must reject atomically")),
                ("owner".to_owned(), Value::Uuid(allowed.test_uuid())),
            ]),
            None,
        )
        .unwrap()
        .with_branch_key(rejected_head_key.clone()),
    );
    let provenance = rejected_tx.contribution_merge.as_mut().unwrap();
    provenance.branch_write_intents[0].head = rejected_head_key.clone();
    // The raw payload has two branch versions but only the view-copy proof.
    // This is deliberately malformed canonical metadata, not a policy denial.
    let rejected_outcome = crate::db::block_on(authority.ingest_commit_unit(
        rejected_tx.clone(),
        rejected_versions,
        24,
    ))
    .unwrap();
    settle_outcome(&mut authority, rejected_outcome).unwrap();
    assert!(matches!(
        authority.transaction_state_settled(rejected_tx.tx_id),
        Some((Fate::Rejected(RejectionReason::AuthorizationDenied), None, DurabilityTier::Local))
    ));
    for rejected_row in [source_row, rejected_insert_row] {
        assert!(authority
            .visible_current_cells_in_branch("tasks", &rejected_head, rejected_row)
            .unwrap()
            .is_none());
    }

    // The frozen variant proves the exact source at its declared frontier,
    // rather than silently falling back to the authority's current schema or
    // current base winner.
    let snapshot_head = branch_selector(0x58);
    let snapshot_head_key = schema
        .project_branch_view_selector(table, &snapshot_head)
        .unwrap()
        .0;
    let (snapshot_tx, snapshot_versions) = make_unit(
        TxId::new(TxTime::from(22), node(0x59)),
        allowed,
        source_tx,
        snapshot_head_key,
        uuid::Uuid::from_bytes([0x58; 16]),
        crate::tx::BranchViewCopyBase::Snapshot {
            branch: base_key.clone(),
            snapshot: crate::protocol::SnapshotRef {
                owner: NodeUuid::from_bytes([0x4a; 16]),
                global_base: GlobalTime(0),
                local_base: source_tx.time,
                dots: Vec::new(),
            },
        },
    );
    let snapshot_outcome = crate::db::block_on(authority.ingest_commit_unit(
        snapshot_tx.clone(),
        snapshot_versions,
        22,
    ))
    .unwrap();
    settle_outcome(&mut authority, snapshot_outcome).unwrap();
    assert!(matches!(
        authority.transaction_state_settled(snapshot_tx.tx_id),
        Some((Fate::Accepted, Some(_), DurabilityTier::Global))
    ));

    // Simulate a competing first-head materialization winning before this
    // unit reaches authority. The descriptor is otherwise valid, but its
    // promised absent head is no longer true and must not be reclassified as
    // a safe view copy.
    let (race_tx, race_versions) = make_unit(
        TxId::new(TxTime::from(23), node(0x5a)),
        allowed,
        source_tx,
        head_key.clone(),
        head_value,
        crate::tx::BranchViewCopyBase::Current(base_key.clone()),
    );
    let race_outcome = crate::db::block_on(authority.ingest_commit_unit(
        race_tx.clone(),
        race_versions,
        23,
    ))
    .unwrap();
    settle_outcome(&mut authority, race_outcome).unwrap();
    assert!(matches!(
        authority.transaction_state_settled(race_tx.tx_id),
        Some((Fate::Rejected(RejectionReason::AuthorizationDenied), None, DurabilityTier::Local))
    ));

    // Planted sensitive negative: a raw sender cannot erase the operation
    // intent and have this first-head inherited copy reclassified as an
    // ordinary insert. If the mandatory-intent check is removed, this becomes
    // accepted under the write policy without proving source read access.
    let omitted_head = branch_selector(0x56);
    let omitted_head_key = schema
        .project_branch_view_selector(table, &omitted_head)
        .unwrap()
        .0;
    let (mut omitted_tx, omitted_versions) = make_unit(
        TxId::new(TxTime::from(25), node(0x57)),
        denied,
        source_tx,
        omitted_head_key,
        uuid::Uuid::from_bytes([0x56; 16]),
        crate::tx::BranchViewCopyBase::Current(base_key.clone()),
    );
    omitted_tx.contribution_merge = None;
    let omitted_outcome = crate::db::block_on(authority.ingest_commit_unit(
        omitted_tx.clone(),
        omitted_versions,
        25,
    ))
    .unwrap();
    settle_outcome(&mut authority, omitted_outcome).unwrap();
    assert!(matches!(
        authority.transaction_state_settled(omitted_tx.tx_id),
        Some((Fate::Rejected(RejectionReason::AuthorizationDenied), None, DurabilityTier::Local))
    ));

    // A distinct target keeps the denial independent of the accepted head
    // overlay above. The same source is physically present, but its private
    // read policy is false for this subject.
    let denied_head = branch_selector(0x51);
    let denied_head_key = schema.project_branch_view_selector(table, &denied_head).unwrap().0;
    let (denied_tx, denied_versions) = make_unit(
        TxId::new(TxTime::from(30), node(0x52)),
        denied,
        source_tx,
        denied_head_key,
        uuid::Uuid::from_bytes([0x51; 16]),
        crate::tx::BranchViewCopyBase::Current(base_key.clone()),
    );
    let denied_outcome =
        crate::db::block_on(authority.ingest_commit_unit(denied_tx.clone(), denied_versions, 30))
            .unwrap();
    settle_outcome(&mut authority, denied_outcome).unwrap();
    let denied_state = authority.transaction_state_settled(denied_tx.tx_id);
    assert!(matches!(
        denied_state,
        Some((Fate::Rejected(RejectionReason::AuthorizationDenied), None, DurabilityTier::Local))
    ), "denied inherited source must reject, got {denied_state:?}");

    // Planted descriptor corruption: the target is otherwise a valid fresh
    // overlay for an allowed writer, but the claimed base witness is not the
    // exact winner. Admission must fail closed rather than treating evidence
    // as decorative metadata.
    let tampered_head = branch_selector(0x53);
    let tampered_head_key = schema
        .project_branch_view_selector(table, &tampered_head)
        .unwrap()
        .0;
    let (mut tampered_tx, tampered_versions) = make_unit(
        TxId::new(TxTime::from(40), node(0x54)),
        allowed,
        source_tx,
        tampered_head_key,
        uuid::Uuid::from_bytes([0x53; 16]),
        crate::tx::BranchViewCopyBase::Current(base_key.clone()),
    );
    tampered_tx
        .contribution_merge
        .as_mut()
        .unwrap()
        .branch_write_intents[0]
        .operation = crate::tx::BranchWriteOperation::ViewUpdateCopy(
        crate::tx::BranchViewCopyEvidence {
            source_version: TxId::new(TxTime::from(999), node(0x55)),
            ..tampered_tx
                .contribution_merge
                .as_ref()
                .unwrap()
                .branch_view_copies[0]
                .clone()
        },
    );
    let tampered_outcome = crate::db::block_on(authority.ingest_commit_unit(
        tampered_tx.clone(),
        tampered_versions,
        40,
    ))
    .unwrap();
    settle_outcome(&mut authority, tampered_outcome).unwrap();
    assert!(matches!(
        authority.transaction_state_settled(tampered_tx.tx_id),
        Some((Fate::Rejected(RejectionReason::MalformedCommit(_)), None, DurabilityTier::Local))
    ));

    // A raw transaction cannot smuggle noncanonical calculated provenance
    // alongside otherwise valid branch-copy metadata. This is structural
    // malformed input, distinct from the earlier canonical-but-unreadable
    // `denied_tx` receipt, which remains AuthorizationDenied.
    let malformed_head = branch_selector(0x61);
    let malformed_head_key = schema
        .project_branch_view_selector(table, &malformed_head)
        .unwrap()
        .0;
    let (mut malformed_tx, _malformed_versions) = make_unit(
        TxId::new(TxTime::from(42), node(0x62)),
        allowed,
        source_tx,
        malformed_head_key,
        uuid::Uuid::from_bytes([0x61; 16]),
        crate::tx::BranchViewCopyBase::Current(base_key.clone()),
    );
    let duplicate_dot = crate::tx::ContributionDot {
        tx_id: source_tx,
        coordinate: crate::tx::ContributionCoordinate {
            branch_key: BranchKey::default(),
            table: "todos".to_owned(),
            row_uuid: source_row,
            layer: crate::tx::MergeAspect::Content,
            component: crate::tx::ContributionComponent::Column("title".to_owned()),
        },
    };
    malformed_tx
        .contribution_merge
        .as_mut()
        .unwrap()
        .substitutions = vec![crate::tx::ContributionSubstitution {
        target: duplicate_dot.coordinate.clone(),
        sources: vec![duplicate_dot.clone(), duplicate_dot],
    }];
    malformed_tx
        .contribution_merge
        .as_mut()
        .unwrap()
        .source = BranchKey {
        values: vec![
            ("z".to_owned(), crate::protocol::BranchColumnValue(vec![1, u8::MAX])),
            ("a".to_owned(), crate::protocol::BranchColumnValue(vec![1, u8::MAX])),
        ],
    };
    // Pair the malformed metadata with an otherwise ordinary missing parent.
    // Provenance validation must happen before orphan parking, so the unit
    // settles immediately and leaves no parked residue.
    let missing_parent = TxId::new(TxTime::from(41), node(0x63));
    let malformed_versions = vec![
        VersionRecord::from_cells(
            table,
            schema.version_id(),
            source_row,
            vec![missing_parent],
            allowed,
            malformed_tx.tx_id.time.physical_ms(),
            allowed,
            malformed_tx.tx_id.time.physical_ms(),
            &BTreeMap::from([
                ("branch_id".to_owned(), Value::Uuid(uuid::Uuid::from_bytes([0x61; 16]))),
                ("title".to_owned(), v("head patch")),
                ("owner".to_owned(), Value::Uuid(allowed.test_uuid())),
            ]),
            None,
        )
        .unwrap()
        .with_branch_key(
            schema
                .project_branch_view_selector(table, &malformed_head)
                .unwrap()
                .0,
        ),
    ];
    let malformed_outcome = crate::db::block_on(authority.ingest_commit_unit(
        malformed_tx.clone(),
        malformed_versions,
        42,
    ))
    .unwrap();
    settle_outcome(&mut authority, malformed_outcome).unwrap();
    assert!(matches!(
        authority.transaction_state_settled(malformed_tx.tx_id),
        Some((Fate::Rejected(RejectionReason::MalformedCommit(_)), None, DurabilityTier::Local))
    ));
    assert!(
        !authority.parking.parked_commit_units.contains_key(&malformed_tx.tx_id),
        "malformed provenance must not leave an orphan parked behind its terminal fate"
    );
    assert!(
        authority
            .transaction_record(malformed_tx.tx_id)
            .unwrap()
            .contribution_merge
            .is_none(),
        "the terminal malformed receipt must not persist unencodable provenance"
    );

    // Durable authority state retains the exact descriptor. A reopened
    // authority therefore keeps the accepted outcome and treats a relayed
    // retry of the same unit as idempotent rather than losing its source proof.
    drop(authority);
    let mut reopened = reopen_history_complete_node_at(
        &_dir,
        NodeUuid::from_bytes([0x4a; 16]),
        schema.clone(),
    );
    assert_eq!(
        reopened.transaction_record(batch_tx.tx_id).unwrap().contribution_merge,
        batch_tx.contribution_merge
    );
    let reopened_outcome = crate::db::block_on(reopened.ingest_commit_unit(
        batch_tx.clone(),
        batch_versions_for_reopen,
        41,
    ))
    .unwrap();
    settle_outcome(&mut reopened, reopened_outcome).unwrap();
    assert!(matches!(
        reopened.transaction_state_settled(batch_tx.tx_id),
        Some((Fate::Accepted, Some(_), DurabilityTier::Global))
    ));
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
    let _base_tx = node
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
                .deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    let head_delete = node.commit_mergeable_settled(
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
            .parents(vec![head_delete])
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
    let LocalMaintainedViewSubscriptionUpdate::Structured {
        terminal_operations,
    } = update
    else {
        panic!("the compiler-owned root collector must publish branch restoration");
    };
    let inserted = terminal_operations
        .iter()
        .filter_map(|operation| match &operation.edit {
            groove::ivm::TerminalEdit::Insert { value, .. } => {
                Some(OwnedRecord::new(value.clone(), operation.root_descriptor.clone()))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(inserted.len(), 1);
    assert_eq!(
        inserted[0].get("row_uuid"),
        Ok(Value::Uuid(row_uuid.0)),
        "the terminal collector must restore the frozen root row"
    );
    assert!(
        terminal_operations
            .iter()
            .all(|operation| !matches!(operation.edit, groove::ivm::TerminalEdit::Remove { .. })),
        "restoring an empty frozen relation must not remove another root"
    );
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
    let LocalMaintainedViewSubscriptionUpdate::Structured {
        terminal_operations,
    } = update
    else {
        panic!("the compiler-owned root collector must publish branch restoration");
    };
    let restored = terminal_operations
        .iter()
        .find_map(|operation| match &operation.edit {
            groove::ivm::TerminalEdit::Insert { value, .. }
            | groove::ivm::TerminalEdit::Update { value, .. } => {
                Some(OwnedRecord::new(value.clone(), operation.root_descriptor.clone()))
            }
            _ => None,
        })
        .expect("head rejection must replace the root terminal payload");
    assert_eq!(restored.get("row_uuid"), Ok(Value::Uuid(row_uuid.0)));
    let title = restored.get("title").expect("decode restored title");
    assert!(
        title == Value::String("frozen base".to_owned())
            || title == Value::Nullable(Some(Box::new(Value::String("frozen base".to_owned())))),
        "the root terminal must carry the frozen-base title"
    );

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
    let LocalMaintainedViewSubscriptionUpdate::Structured {
        terminal_operations,
    } = replacement
    else {
        panic!("the compiler-owned root collector must publish replacement-head content");
    };
    let replacement = terminal_operations
        .iter()
        .find_map(|operation| match &operation.edit {
            groove::ivm::TerminalEdit::Insert { value, .. }
            | groove::ivm::TerminalEdit::Update { value, .. } => {
                Some(OwnedRecord::new(value.clone(), operation.root_descriptor.clone()))
            }
            _ => None,
        })
        .expect("replacement head must replace the root terminal payload");
    let title = replacement.get("title").expect("decode replacement title");
    assert!(
        title == Value::String("replacement head".to_owned())
            || title
                == Value::Nullable(Some(Box::new(Value::String("replacement head".to_owned())))),
        "the root terminal must carry replacement-head content"
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

/// Internal maintained-view witness receipt: a branched writer's large
/// immutable history record must reload from that exact branch, never from an
/// implicit default coordinate. This targets the pre-serialization storage
/// identity boundary that public queries cannot directly expose.
#[test]
fn maintained_witness_reloads_the_exact_large_nondefault_branch_version() {
    // `canonical_history_version_for_maintained_witness` is used before a
    // maintained result is serialized. A physical row can have both default
    // and non-default branch versions in one transaction, so table/row/tx
    // coordinates alone are insufficient: falling back to the default branch
    // silently ships a different immutable VersionRecord.
    let schema = branch_view_schema();
    let (_dir, mut node) =
        open_history_complete_node_with_schema(NodeUuid::from_bytes([0x70; 16]), schema.clone());
    let row_uuid = row(0x71);
    let branch = branch_selector(0x72);
    let owner = AuthorSubject::for_test_bytes([0x73; 16]);
    let cells = |title: String| {
        BTreeMap::from([
            ("title".to_owned(), Value::String(title)),
            ("owner".to_owned(), Value::Uuid(owner.test_uuid())),
        ])
    };
    // Keep this below the large-value promotion threshold: the regression is
    // about reloading a large *VersionRecord* from history, not about a query
    // evaluator materializing an application large value first.
    let branch_title = "non-default branch canonical body".repeat(1_500);
    let tx_id = node
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 10)
                .branch(branch.clone())
                .cells(cells(branch_title.clone())),
        )
        .unwrap();
    let versions = futures::executor::block_on(node.query_versions_for_tx(tx_id)).unwrap();
    let branched = versions
        .iter()
        .find(|version| version.branch_key().is_canonical() && *version.branch_key() != BranchKey::default())
        .expect("non-default branch row is persisted")
        .clone();
    let storage_table = node
        .version_storage_sources_for_layer(branched.table(), branched.layer())
        .unwrap()
        .into_iter()
        .next()
        .expect("content versions have history storage");
    assert!(futures::executor::block_on(node.query_version_by_alias_with_storage_in_schema(
        schema.version_id(),
        branched.table(),
        &storage_table,
        &BranchKey::default(),
        branched.row_uuid(),
        branched.tx_time(),
        branched.tx_node_alias(),
    ))
    .unwrap()
    .is_none(), "a missing branch coordinate must never silently use a default branch");
    // Model the maintained graph's real projection boundary: its history
    // descriptor is unchanged, but a selected-out authored cell is represented
    // by typed null. Falling through to this in-memory witness is forbidden;
    // recovery must find the complete immutable row under the exact branch.
    let mut partial_witness = branched.clone();
    let descriptor = partial_witness.record.descriptor().clone();
    let mut values = partial_witness.record.to_values().unwrap();
    let title_position = node.catalogue.schema.tables[0]
        .columns
        .iter()
        .position(|column| column.name == "title")
        .unwrap();
    values[crate::node::codec::HistoryRowRecord::USER_CELLS + title_position] =
        Value::Nullable(None);
    partial_witness.record = groove::records::OwnedRecord::new(
        descriptor.create(&values).unwrap(),
        descriptor,
    );
    assert_eq!(
        partial_witness
            .cell(&node.catalogue.schema.tables[0], "title")
            .unwrap(),
        None,
        "the planted maintained witness must be observably partial"
    );
    let canonical = futures::executor::block_on(
        node.canonical_history_version_for_maintained_witness(&partial_witness),
    )
    .unwrap();
    let canonical_wire = node.version_record_from_row(&canonical).unwrap();
    assert_eq!(canonical.branch_key(), branched.branch_key());
    assert_eq!(canonical_wire.branch_key(), branched.branch_key());
    assert_eq!(canonical_wire.cell_at(1), Some(Value::String(branch_title)));
}

#[test]
fn branch_coordinates_use_one_canonical_prefix_in_memory_and_after_rocks_reopen() {
    let schema = branch_view_schema();
    let branch = branch_selector(0x70);
    let sibling_branch = branch_selector(0x73);
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
    // A fresh node installs the frozen V1 branch-prefixed layout directly.
    for (table, indexed_column) in [("users", "name"), ("todos", "title")] {
        let mapping =
            memory.catalogue.physical_mappings[&schema.version_id()].tables[table].clone();
        let v1_index = physical_current_index_name(mapping.columns[indexed_column]);
        assert_eq!(
            v1_index,
            format!("by_physical_app_v1_{}", mapping.columns[indexed_column].0),
            "the branch-prefixed current-index identity is frozen at V1"
        );
        for storage_table in [
            physical_ahead_current_table_name(mapping.table_id),
            physical_global_current_table_name(mapping.table_id),
        ] {
            let indices = &memory.database.table_schema(&storage_table).unwrap().indices;
            assert!(indices.iter().any(|index| index.name == v1_index));
        }
    }
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

    // The same application RowUuid is independently addressable in another
    // branch. Keep the indexed user value identical so this receipt proves the
    // canonical branch prefix, rather than relying on title discrimination.
    let sibling_tx = rocks
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, 30)
                .branch(sibling_branch.clone())
                .cells(cells()),
        )
        .unwrap();

    let key = schema
        .project_branch_view_selector(&schema.tables[0], &branch)
        .unwrap()
        .0;
    let sibling_key = schema
        .project_branch_view_selector(&schema.tables[0], &sibling_branch)
        .unwrap()
        .0;
    let mapping = rocks.catalogue.physical_mappings[&schema.version_id()].tables["todos"].clone();
    let table_id = mapping.table_id;
    let title_index = physical_current_index_name(mapping.columns["title"]);
    let expected_index_columns = vec![
        "branch_key".to_owned(),
        physical_user_column_field(mapping.columns["title"]),
    ];
    for storage_table in [
        physical_ahead_current_table_name(table_id),
        physical_global_current_table_name(table_id),
    ] {
        assert_eq!(
            rocks
                .database
                .table_schema(&storage_table)
                .unwrap()
                .indices
                .iter()
                .find(|index| index.name == title_index)
                .unwrap()
                .columns,
            expected_index_columns,
            "physical current indexes must put the canonical branch key before their user key"
        );
    }
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
    for branch_key in [&key, &sibling_key] {
        assert_eq!(
            rocks
                .database
                .index_scan_raw(
                    &physical_ahead_current_table_name(table_id),
                    &title_index,
                    &[Value::Bytes(branch_key.canonical_bytes())],
                )
                .unwrap()
                .len(),
            1,
            "same RowUuid/title rows must remain in distinct canonical branch index prefixes"
        );
    }

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
            sibling_tx,
            Fate::Accepted,
            Some(GlobalTime(3)),
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
    for branch_key in [&key, &sibling_key] {
        assert_eq!(
            rocks
                .database
                .index_scan_raw(
                    &physical_global_current_table_name(table_id),
                    &title_index,
                    &[Value::Bytes(branch_key.canonical_bytes())],
                )
                .unwrap()
                .len(),
            1,
            "global-current rebuild must preserve distinct canonical branch index prefixes"
        );
    }
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
    assert_eq!(
        reopened
            .query_row_versions_in_branch("todos", &sibling_key, row_uuid)
            .unwrap()
            .len(),
        1,
        "reopen must not alias a sibling branch history for the same RowUuid"
    );
    assert_eq!(
        reopened
            .visible_current_cells_in_branch("todos", &sibling_branch, row_uuid)
            .unwrap()
            .unwrap()["title"],
        v("branch receipt"),
        "the sibling branch remains visible after rebuilding current/index state"
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
    let mut provenance = ContributionMergeProvenance::canonical(
        source_key,
        target_key.clone(),
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
    // The common commit path adds the exact authored branch operation without
    // changing the calculated source substitutions or adding other coordinates.
    provenance.branch_write_intents = vec![crate::tx::BranchWriteIntent {
        version: 1,
        physical_table_id: node.catalogue.physical_mappings[&schema.version_id()].tables["todos"].table_id,
        authored_schema: schema.version_id(),
        row_uuid,
        head: target_key,
        operation: crate::tx::BranchWriteOperation::ExactHeadInsert,
    }];
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
    let LocalMaintainedViewSubscriptionUpdate::Structured {
        terminal_operations,
    } = update
    else {
        panic!("the compiler-owned root collector must publish the live-base edit");
    };
    assert!(
        terminal_operations.iter().any(|operation| {
            matches!(
                operation.edit,
                groove::ivm::TerminalEdit::Insert { .. } | groove::ivm::TerminalEdit::Update { .. }
            )
        }),
        "the live-base edit must replace the root terminal payload"
    );
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
        ).expect("valid migration lens"),
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
        ).expect("valid migration lens"),
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
        // These deliberately malformed targets cannot be authored by the
        // authority factory.  Keep the fixture explicit so this test reaches
        // the branch-schema validator rather than weakening that factory.
        let error = core.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(SchemaLineagePublication::new_genesis_fixture(
            target.clone(),
            MigrationLens::new(
                source.version_id(),
                target.id,
                vec![TableLens {
                    source_table: "todos".to_owned(),
                    target_table: "todos".to_owned(),
                    ops: Vec::new(),
                }],
            ).expect("valid migration lens"),
            Vec::<String>::new(),
            Vec::<String>::new(),
        )),
        })
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
    ).expect("valid migration lens");

    NodeState::<RocksDbStorage>::validate_migration_lens_between(&lens, &source, &target)
        .expect("a branch column can be added monotonically with an immutable default");
}
