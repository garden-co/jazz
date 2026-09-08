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
