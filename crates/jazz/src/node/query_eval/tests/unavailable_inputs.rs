//! Internal setup is necessary: the unavailable-input primitive deliberately
//! has no network/public mutation API yet. Public schema/query builders and
//! ordinary one-shot/maintained results exercise its observable behavior.

use super::*;

fn fixture() -> (tempfile::TempDir, NodeState<RocksDbStorage>, JazzSchema) {
    let schema = public_query_eval_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("parents")
                    .column("label", PublicColumnType::Text)
                    .policies(PublicTablePolicies::new().with_select(PublicPolicyExpr::True)),
            )
            .table(
                PublicTableSchemaBuilder::new("children")
                    .fk_column("parent", "parents")
                    .policies(PublicTablePolicies::new().with_select(PublicPolicyExpr::True)),
            ),
    );
    let (dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([91; 16]), schema.clone());
    for (id, label) in [(row(1), "first"), (row(2), "second")] {
        node.commit_mergeable_settled(
            MergeableCommit::new("parents", id, 1)
                .made_by(AuthorSubject::SYSTEM)
                .cells(BTreeMap::from([(
                    "label".to_owned(),
                    Value::String(label.to_owned()),
                )])),
        )
        .unwrap();
    }
    node.commit_mergeable_settled(
        MergeableCommit::new("children", row(3), 2)
            .made_by(AuthorSubject::SYSTEM)
            .cells(BTreeMap::from([(
                "parent".to_owned(),
                Value::Uuid(row(1).0),
            )])),
    )
    .unwrap();
    (dir, node, schema)
}

fn read(
    node: &mut NodeState<RocksDbStorage>,
    schema: &JazzSchema,
    query: Query,
    who: AuthorSubject,
) -> Vec<CurrentRow> {
    let shape = query.validate(schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    node.query_rows_for_client(&shape, &binding, DurabilityTier::Local, who)
        .unwrap()
}

fn parent_ids(
    node: &mut NodeState<RocksDbStorage>,
    schema: &JazzSchema,
    who: AuthorSubject,
) -> BTreeSet<RowUuid> {
    read(node, schema, Query::from("parents"), who)
        .into_iter()
        .map(|row| row.row_uuid())
        .collect()
}

/// Alice's exact claim snapshot is unavailable; Bob, Alice's other snapshot,
/// and the SYSTEM storage owner keep their ordinary cached rows.
/// alice/blue ──mark row 1──► blue source only
/// alice/red, bob, SYSTEM ──read──► rows 1 and 2
#[test]
fn local_unavailable_inputs_isolate_subject_and_exact_claims() {
    let (_dir, mut node, schema) = fixture();
    let alice = author(1);
    let bob = author(2);
    let blue = BTreeMap::from([("color".to_owned(), Value::String("blue".to_owned()))]);
    let red = BTreeMap::from([("color".to_owned(), Value::String("red".to_owned()))]);
    let scope = {
        let mut scoped = node.scoped_active_session_claims(alice, blue.clone());
        assert_eq!(parent_ids(&mut scoped, &schema, alice).len(), 2);
        scoped.local_read_policy_binding(alice).unwrap()
    };
    assert!(
        node.set_local_row_unavailable(&scope, "parents", row(1), true)
            .unwrap()
    );
    assert!(
        !node
            .set_local_row_unavailable(&scope, "parents", row(1), true)
            .unwrap()
    );
    {
        let mut scoped = node.scoped_active_session_claims(alice, blue);
        assert_eq!(
            parent_ids(&mut scoped, &schema, alice),
            BTreeSet::from([row(2)])
        );
    }
    {
        let mut scoped = node.scoped_active_session_claims(alice, red);
        assert_eq!(
            parent_ids(&mut scoped, &schema, alice),
            BTreeSet::from([row(1), row(2)])
        );
    }
    assert_eq!(parent_ids(&mut node, &schema, bob).len(), 2);
    assert_eq!(
        parent_ids(&mut node, &schema, AuthorSubject::SYSTEM).len(),
        2
    );
    assert_eq!(
        node.current_rows("parents", DurabilityTier::Local)
            .unwrap()
            .len(),
        2
    );
    assert!(
        node.local_read_policy_binding(AuthorSubject::SYSTEM)
            .is_none()
    );
}

/// Alice's child exclusion changes the join and count inputs, and her parent
/// exclusion selects the next top row. Bob's count is unaffected.
#[test]
fn local_unavailable_inputs_filter_before_join_aggregate_and_window() {
    let (_dir, mut node, schema) = fixture();
    let alice = author(1);
    let scope = node.local_read_policy_binding(alice).unwrap();
    let joined = Query::from("parents").join_via("children", "parent", []);
    assert_eq!(read(&mut node, &schema, joined.clone(), alice).len(), 1);
    node.set_local_row_unavailable(&scope, "children", row(3), true)
        .unwrap();
    assert!(read(&mut node, &schema, joined.clone(), alice).is_empty());
    assert_eq!(
        read(&mut node, &schema, Query::from("children").count(), alice)[0]
            .test_cells_by_descriptor()["count"],
        Value::U64(0)
    );
    assert_eq!(
        read(
            &mut node,
            &schema,
            Query::from("children").count(),
            author(2)
        )[0]
        .test_cells_by_descriptor()["count"],
        Value::U64(1)
    );
    node.set_local_row_unavailable(&scope, "children", row(3), false)
        .unwrap();
    assert_eq!(read(&mut node, &schema, joined, alice).len(), 1);
    node.set_local_row_unavailable(&scope, "parents", row(1), true)
        .unwrap();
    let window = Query::from("parents")
        .order_by("label", OrderDirection::Asc)
        .limit(1);
    assert_eq!(
        read(&mut node, &schema, window, alice)[0].row_uuid(),
        row(2)
    );
}

/// Alice's already-open joined subscription retracts and readmits its parent
/// when the child source's mutable unavailable input changes.
/// subscribe [parent 1] ──mark child 3──► [] ──clear child 3──► [parent 1]
#[test]
fn local_unavailable_inputs_retract_and_readmit_maintained_rows() {
    let (_dir, mut node, schema) = fixture();
    let alice = author(1);
    let shape = Query::from("parents")
        .join_via("children", "parent", [])
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let (shape, binding, plan) = node
        .prepare_query_binding_for_link_in_authorization_mode(
            &shape,
            &binding,
            DurabilityTier::Local,
            alice,
            QueryAuthorizationMode::ClientLocal,
        )
        .unwrap();
    let (mut subscription, initial) = node
        .open_maintained_view_subscription_in_authorization_mode(
            &shape,
            &binding,
            alice,
            DurabilityTier::Local,
            &ReadViewSpec::default(),
            Some(plan),
            QueryAuthorizationMode::ClientLocal,
        )
        .unwrap();
    assert_eq!(initial.root_count, 1);
    let scope = node.local_read_policy_binding(alice).unwrap();
    node.set_local_row_unavailable(&scope, "children", row(3), true)
        .unwrap();
    let removed = node
        .drain_local_maintained_view_subscription(&mut subscription, None)
        .unwrap()
        .expect("child withdrawal emits a parent retraction");
    let LocalMaintainedViewSubscriptionUpdate::Structured {
        terminal_operations,
    } = removed
    else {
        panic!("ordinary parent output uses structured terminals");
    };
    assert_eq!(terminal_operations.len(), 1);
    assert!(matches!(
        terminal_operations[0].edit,
        groove::ivm::TerminalEdit::Remove { .. }
    ));
    node.set_local_row_unavailable(&scope, "children", row(3), false)
        .unwrap();
    let added = node
        .drain_local_maintained_view_subscription(&mut subscription, None)
        .unwrap()
        .expect("fresh child admission emits a parent insertion");
    let LocalMaintainedViewSubscriptionUpdate::Structured {
        terminal_operations,
    } = added
    else {
        panic!("ordinary parent output uses structured terminals");
    };
    assert_eq!(terminal_operations.len(), 1);
    assert!(matches!(
        terminal_operations[0].edit,
        groove::ivm::TerminalEdit::Insert { .. }
    ));
}

/// Alice's app exclusion cannot confirm itself in a fresh permission probe:
/// the serving policy still allows the stored row, which can be readmitted.
#[test]
fn local_unavailable_inputs_never_filter_authorization_probes() {
    let (_dir, mut node, schema) = fixture();
    let alice = author(1);
    let scope = node.local_read_policy_binding(alice).unwrap();
    node.set_local_row_unavailable(&scope, "parents", row(1), true)
        .unwrap();
    assert_eq!(
        parent_ids(&mut node, &schema, alice),
        BTreeSet::from([row(2)])
    );
    assert!(
        node.dry_run_read_current_allows("parents", row(1), alice)
            .unwrap()
    );
    let shape = Query::from("parents").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    assert_eq!(
        node.query_rows_for_link(&shape, &binding, DurabilityTier::Local, alice)
            .unwrap()
            .len(),
        2
    );
}

/// Alice's current-view prototype exclusion leaves an explicit historical
/// snapshot unchanged; extending withdrawal to history needs its own contract.
#[test]
fn local_unavailable_inputs_do_not_reinterpret_historical_snapshots() {
    let (_dir, mut node, schema) = fixture();
    let alice = author(1);
    let shape = Query::from("parents").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let read_view = ReadViewSpec {
        source: crate::protocol::ReadViewSourceSpec::Snapshot {
            snapshot: crate::protocol::SnapshotRef {
                owner: node.node_uuid,
                global_base: GlobalTime(0),
                local_base: TxTime(u64::MAX),
                dots: Vec::new(),
            },
        },
    };
    let before = node
        .query_relation_snapshot_for_client(
            &shape,
            &binding,
            DurabilityTier::Local,
            alice,
            &read_view,
        )
        .unwrap();
    assert_eq!(before.rows.len(), 2);
    let scope = node.local_read_policy_binding(alice).unwrap();
    node.set_local_row_unavailable(&scope, "parents", row(1), true)
        .unwrap();
    assert_eq!(
        parent_ids(&mut node, &schema, alice),
        BTreeSet::from([row(2)])
    );
    let after = node
        .query_relation_snapshot_for_client(
            &shape,
            &binding,
            DurabilityTier::Local,
            alice,
            &read_view,
        )
        .unwrap();
    assert_eq!(after.rows.len(), 2);
}

/// Alice's includeDeleted read must not reveal an unavailable live cached row;
/// the same row returns after fresh admission. Bob keeps his independent view.
#[test]
fn local_unavailable_inputs_also_filter_include_deleted_app_sources() {
    let (_dir, mut node, schema) = fixture();
    let alice = author(1);
    let shape = Query::from("parents").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let scope = node.local_read_policy_binding(alice).unwrap();
    node.set_local_row_unavailable(&scope, "parents", row(1), true)
        .unwrap();
    let read = |node: &mut NodeState<RocksDbStorage>, identity| {
        node.query_rows_including_deleted_in_authorization_mode(
            &shape,
            &binding,
            DurabilityTier::Local,
            None,
            identity,
            QueryAuthorizationMode::ClientLocal,
        )
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>()
    };
    assert_eq!(read(&mut node, alice), BTreeSet::from([row(2)]));
    assert_eq!(read(&mut node, author(2)), BTreeSet::from([row(1), row(2)]));
    node.set_local_row_unavailable(&scope, "parents", row(1), false)
        .unwrap();
    assert_eq!(read(&mut node, alice), BTreeSet::from([row(1), row(2)]));
}

/// Alice opens a cold Edge receiver before RegisterShape. Later authority
/// inputs and local unavailable markers must both reach its existing graph.
/// cold [] ──admitted row──► [1] ──unavailable──► [] ──readmit──► [1]
#[test]
fn local_unavailable_inputs_follow_cold_current_edge_receivers() {
    let (_dir, mut node, schema) = fixture();
    let alice = author(1);
    let shape = Query::from("parents").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let (mut subscription, _) = node
        .open_maintained_view_subscription_in_authorization_mode(
            &shape,
            &binding,
            alice,
            DurabilityTier::Edge,
            &ReadViewSpec::default(),
            None,
            QueryAuthorizationMode::ClientLocal,
        )
        .unwrap();
    assert!(
        node.query.registered_shape_options.is_empty(),
        "cold opening has no registration metadata"
    );
    let current = node
        .current_rows("parents", DurabilityTier::Local)
        .unwrap()
        .into_iter()
        .find(|current| current.row_uuid() == row(1))
        .unwrap();
    let alias = node
        .ensure_schema_version_alias(node.catalogue.current_schema_version_id)
        .unwrap();
    // Internal setup supplies one synthetic admitted covered input. Receipt
    // verification is intentionally outside this source primitive's scope.
    let replacements = subscription
        .covered_input_receiver
        .sources
        .values()
        .map(|source| InputSourceReplacement {
            id: source.id,
            descriptor: source.descriptor.clone(),
            records: vec![
                covered_input_record(
                    node.table("parents").unwrap(),
                    &source.descriptor,
                    &current,
                    alias,
                    &BranchKey::default(),
                )
                .unwrap(),
            ],
        })
        .collect::<Vec<_>>();
    node.database.replace_input_sources(replacements).unwrap();
    assert!(
        node.drain_local_maintained_view_subscription(&mut subscription, None)
            .unwrap()
            .is_some()
    );
    let scope = node.local_read_policy_binding(alice).unwrap();
    for (unavailable, removing) in [(true, true), (false, false)] {
        node.set_local_row_unavailable(&scope, "parents", row(1), unavailable)
            .unwrap();
        let update = node
            .drain_local_maintained_view_subscription(&mut subscription, None)
            .unwrap()
            .expect("cold receiver keeps its unavailable input connected");
        let LocalMaintainedViewSubscriptionUpdate::Structured {
            terminal_operations,
        } = update
        else {
            panic!("parent output uses structured terminals");
        };
        assert_eq!(terminal_operations.len(), 1);
        assert_eq!(
            matches!(
                terminal_operations[0].edit,
                groove::ivm::TerminalEdit::Remove { .. }
            ),
            removing
        );
    }
}

/// Alice's current includeDeleted take must find the next readable index
/// candidate after the first physical candidate becomes unavailable.
#[test]
fn local_unavailable_inputs_keep_include_deleted_limit_after_exclusion() {
    let schema = public_query_eval_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("docs")
                .column("bucket", PublicColumnType::Text)
                .index_only(["bucket"]),
        ),
    );
    let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([92; 16]), schema.clone());
    for n in 1..=3 {
        commit_global_cells(
            &mut node,
            "docs",
            row(n),
            BTreeMap::from([("bucket".to_owned(), Value::String("same".to_owned()))]),
            n as u64,
            n as u64,
        );
    }
    let alice = author(1);
    let scope = node.local_read_policy_binding(alice).unwrap();
    node.set_local_row_unavailable(&scope, "docs", row(1), true)
        .unwrap();
    let query = Query::from("docs")
        .filter(eq(col("bucket"), lit("same")))
        .limit(1);
    for query in [query.clone(), query.order_by("id", OrderDirection::Asc)] {
        let shape = query.validate(&schema).unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let rows = node
            .query_rows_including_deleted_in_authorization_mode(
                &shape,
                &binding,
                DurabilityTier::Global,
                None,
                alice,
                QueryAuthorizationMode::ClientLocal,
            )
            .unwrap();
        assert_eq!(
            rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
            vec![row(2)]
        );
    }
}

fn reopen_availability_node(
    dir: &tempfile::TempDir,
    schema: &JazzSchema,
) -> NodeState<RocksDbStorage> {
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage =
        RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync).unwrap();
    NodeState::new(NodeUuid::from_bytes([91; 16]), schema.clone(), storage).unwrap()
}

fn availability_watermark(seq: u64) -> LocalAvailabilityWatermark {
    LocalAvailabilityWatermark {
        core: NodeUuid::from_bytes([92; 16]),
        core_epoch: 3,
        claims_revision: 4,
        policy_epoch: 5,
        settled_through: GlobalTime(6),
        authorization_progress: seq,
    }
}

/// Internal receipt injection is necessary until the separate network router
/// is integrated. Ordinary queries verify persisted exclusion and readmission.
/// unavailable -> reopen -> hidden; readable -> reopen -> stale reply ignored.
#[test]
fn local_availability_receipts_survive_reopen_and_retain_readmission_watermarks() {
    let (dir, mut node, schema) = fixture();
    let alice = author(1);
    let scope = node.local_read_policy_binding(alice).unwrap();
    let table = node
        .local_availability_table_id(schema.version_id(), "parents")
        .unwrap();
    let unavailable = [(table, row(1), LocalRowAvailability::CurrentUnavailable)];
    let readable = [(table, row(1), LocalRowAvailability::Readable)];
    let watermark = availability_watermark(1);
    node.activate_local_availability_authority(scope.clone(), watermark.core, watermark.core_epoch)
        .unwrap();
    assert!(
        node.apply_verified_local_row_availability(&scope, watermark, &unavailable)
            .unwrap()
    );
    assert_eq!(
        parent_ids(&mut node, &schema, alice),
        BTreeSet::from([row(2)])
    );
    drop(node);
    let mut node = reopen_availability_node(&dir, &schema);
    assert_eq!(
        parent_ids(&mut node, &schema, alice),
        BTreeSet::from([row(2)])
    );
    assert_eq!(parent_ids(&mut node, &schema, author(2)).len(), 2);
    assert_eq!(
        parent_ids(&mut node, &schema, AuthorSubject::SYSTEM).len(),
        2
    );
    assert!(
        !node
            .apply_verified_local_row_availability(&scope, availability_watermark(2), &readable)
            .unwrap()
    );
    node.activate_local_availability_authority(scope.clone(), watermark.core, watermark.core_epoch)
        .unwrap();
    assert!(
        node.apply_verified_local_row_availability(&scope, availability_watermark(2), &readable)
            .unwrap()
    );
    assert_eq!(parent_ids(&mut node, &schema, alice).len(), 2);
    drop(node);
    let mut node = reopen_availability_node(&dir, &schema);
    node.activate_local_availability_authority(scope.clone(), watermark.core, watermark.core_epoch)
        .unwrap();
    assert!(
        !node
            .apply_verified_local_row_availability(&scope, watermark, &unavailable)
            .unwrap()
    );
    assert_eq!(parent_ids(&mut node, &schema, alice).len(), 2);
}

/// Only the route owner admits a new epoch. Its sequence/claims revision may
/// reset, while a same-Core durable catalogue position and settled cut may not.
#[test]
fn local_availability_receipts_require_admitted_epoch_and_monotone_cut() {
    let (_dir, mut node, schema) = fixture();
    let alice = author(1);
    let scope = node.local_read_policy_binding(alice).unwrap();
    let table = node
        .local_availability_table_id(schema.version_id(), "parents")
        .unwrap();
    let old = availability_watermark(20);
    node.activate_local_availability_authority(scope.clone(), old.core, old.core_epoch)
        .unwrap();
    assert!(
        node.apply_verified_local_row_availability(
            &scope,
            old,
            &[(table, row(1), LocalRowAvailability::CurrentUnavailable)]
        )
        .unwrap()
    );
    let fresh = LocalAvailabilityWatermark {
        core_epoch: 4,
        claims_revision: 1,
        authorization_progress: 1,
        ..old
    };
    let readable = [(table, row(1), LocalRowAvailability::Readable)];
    assert!(
        !node
            .apply_verified_local_row_availability(&scope, fresh, &readable)
            .unwrap()
    );
    node.activate_local_availability_authority(scope.clone(), fresh.core, fresh.core_epoch)
        .unwrap();
    assert!(
        !node
            .apply_verified_local_row_availability(
                &scope,
                LocalAvailabilityWatermark {
                    settled_through: GlobalTime(5),
                    ..fresh
                },
                &readable
            )
            .unwrap()
    );
    assert_eq!(
        parent_ids(&mut node, &schema, alice),
        BTreeSet::from([row(2)])
    );
    assert!(
        node.apply_verified_local_row_availability(&scope, fresh, &readable)
            .unwrap()
    );
    assert_eq!(parent_ids(&mut node, &schema, alice).len(), 2);
    assert!(
        !node
            .apply_verified_local_row_availability(
                &scope,
                old,
                &[(table, row(1), LocalRowAvailability::CurrentUnavailable)]
            )
            .unwrap()
    );
    assert_eq!(parent_ids(&mut node, &schema, alice).len(), 2);
    let failover = LocalAvailabilityWatermark {
        core: NodeUuid::from_bytes([93; 16]),
        settled_through: GlobalTime(5),
        ..fresh
    };
    node.activate_local_availability_authority(scope.clone(), failover.core, failover.core_epoch)
        .unwrap();
    assert!(
        !node
            .apply_verified_local_row_availability(
                &scope,
                failover,
                &[(table, row(1), LocalRowAvailability::CurrentUnavailable)]
            )
            .unwrap()
    );
    assert_eq!(parent_ids(&mut node, &schema, alice).len(), 2);
}

/// Internal lifecycle invocation requires all scope graphs to have stopped.
/// A subsequent ordinary read must lazily restore retained unavailable rows.
#[test]
fn local_availability_input_retirement_retains_exclusion() {
    let (_dir, mut node, schema) = fixture();
    let alice = author(1);
    let scope = node.local_read_policy_binding(alice).unwrap();
    assert_eq!(parent_ids(&mut node, &schema, alice).len(), 2);
    node.set_local_row_unavailable(&scope, "parents", row(1), true)
        .unwrap();
    node.retire_local_availability_scope_inputs(&scope).unwrap();
    assert_eq!(
        parent_ids(&mut node, &schema, alice),
        BTreeSet::from([row(2)])
    );
}

/// Public schema/lens builders preserve physical table identity across a rename;
/// changing the projection must not resurrect the unavailable cached input.
#[test]
fn local_availability_receipts_follow_global_table_through_schema_rename() {
    let (_dir, mut node, schema) = fixture();
    let alice = author(1);
    let scope = node.local_read_policy_binding(alice).unwrap();
    node.set_local_row_unavailable(&scope, "parents", row(1), true)
        .unwrap();
    let evolved = public_query_eval_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("renamed")
                    .column("label", PublicColumnType::Text)
                    .policies(PublicTablePolicies::new().with_select(PublicPolicyExpr::True)),
            )
            .table(
                PublicTableSchemaBuilder::new("children")
                    .fk_column("parent", "renamed")
                    .policies(PublicTablePolicies::new().with_select(PublicPolicyExpr::True)),
            ),
    );
    let payload = SchemaVersion::new(evolved.clone());
    let lens = MigrationLens::new(
        schema.version_id(),
        payload.id,
        vec![
            TableLens {
                source_table: "parents".to_owned(),
                target_table: "renamed".to_owned(),
                ops: vec![LensOp::RenameTable {
                    from: "parents".to_owned(),
                    to: "renamed".to_owned(),
                }],
            },
            TableLens {
                source_table: "children".to_owned(),
                target_table: "children".to_owned(),
                ops: vec![],
            },
        ],
    )
    .unwrap();
    let publication = node
        .author_schema_lineage_publication(
            payload.clone(),
            lens,
            Vec::<String>::new(),
            Vec::<String>::new(),
        )
        .unwrap();
    node.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
        author: AuthorSubject::SYSTEM,
        catalogue_seq: 1,
        publication: Box::new(publication),
    })
    .unwrap();
    node.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: payload.id,
        },
    })
    .unwrap();
    assert_eq!(
        node.local_availability_table_id(schema.version_id(), "parents")
            .unwrap(),
        node.local_availability_table_id(payload.id, "renamed")
            .unwrap()
    );
    assert_eq!(
        read(&mut node, &evolved, Query::from("renamed"), alice)
            .into_iter()
            .map(|r| r.row_uuid())
            .collect::<Vec<_>>(),
        vec![row(2)]
    );
}

/// Internal route admission is necessary to cover contexts which have not yet
/// opened an app graph. Their capacity must still bound ordinary source opens.
#[test]
fn local_availability_context_capacity_includes_request_only_admissions() {
    let (_dir, mut node, schema) = fixture();
    let first = node.local_read_policy_binding(author(1)).unwrap();
    for i in 0..crate::authorization_scope::MAX_AUTHORIZATION_SCOPES {
        let scope = node.local_read_policy_binding(author(i as u8)).unwrap();
        node.activate_local_availability_authority(scope, NodeUuid::from_bytes([92; 16]), 1)
            .unwrap();
    }
    let extra = AuthorSubject::for_test_bytes([99, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
    let scope = node.local_read_policy_binding(extra).unwrap();
    assert!(
        node.activate_local_availability_authority(
            scope.clone(),
            NodeUuid::from_bytes([92; 16]),
            1
        )
        .is_err()
    );
    let shape = Query::from("parents").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    assert!(
        node.query_rows_for_client(&shape, &binding, DurabilityTier::Local, extra)
            .is_err()
    );
    node.retire_local_availability_scope_inputs(&first).unwrap();
    assert_eq!(parent_ids(&mut node, &schema, extra).len(), 2);
}

/// Internal suspension is necessary to drop precisely after the metadata write,
/// before the IVM input change. Ordinary reads must fail closed until reopen.
#[test]
fn cancelled_local_availability_apply_blocks_reads_until_reopen() {
    use futures::FutureExt;
    let (dir, mut node, schema) = fixture();
    let alice = author(1);
    let shape = Query::from("parents").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let (shape, binding, plan) = node
        .prepare_query_binding_for_link_in_authorization_mode(
            &shape,
            &binding,
            DurabilityTier::Local,
            alice,
            QueryAuthorizationMode::ClientLocal,
        )
        .unwrap();
    let (mut subscription, initial) = node
        .open_maintained_view_subscription_in_authorization_mode(
            &shape,
            &binding,
            alice,
            DurabilityTier::Local,
            &ReadViewSpec::default(),
            Some(plan),
            QueryAuthorizationMode::ClientLocal,
        )
        .unwrap();
    assert_eq!(initial.root_count, 2);
    let scope = node.local_read_policy_binding(alice).unwrap();
    let table = node
        .local_availability_table_id(schema.version_id(), "parents")
        .unwrap();
    let watermark = availability_watermark(1);
    node.activate_local_availability_authority(scope.clone(), watermark.core, watermark.core_epoch)
        .unwrap();
    let persisted = std::cell::Cell::new(false);
    let outcomes = [(table, row(1), LocalRowAvailability::CurrentUnavailable)];
    let apply =
        node.apply_local_row_availability_after_persist(&scope, watermark, &outcomes, async {
            persisted.set(true);
            std::future::pending::<()>().await;
        });
    assert!(apply.now_or_never().is_none()); // Drops the suspended apply.
    assert!(
        persisted.get(),
        "the receipt write completed before cancellation"
    );
    node.query_rows_for_client(&shape, &binding, DurabilityTier::Local, alice)
        .unwrap_err();
    assert!(
        futures::executor::block_on(
            node.drain_local_maintained_view_subscription(&mut subscription, None)
        )
        .is_err(),
        "maintained drain must reject a cancelled apply"
    );
    assert!(matches!(
        node.database.ensure_usable(),
        Err(groove::db::Error::DatabasePoisoned)
    ));
    drop(subscription);
    drop(node);
    let mut node = reopen_availability_node(&dir, &schema);
    assert_eq!(
        parent_ids(&mut node, &schema, alice),
        BTreeSet::from([row(2)])
    );
    assert_eq!(parent_ids(&mut node, &schema, author(2)).len(), 2);
    assert_eq!(
        parent_ids(&mut node, &schema, AuthorSubject::SYSTEM).len(),
        2
    );
}
