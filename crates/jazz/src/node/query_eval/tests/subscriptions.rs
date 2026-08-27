//! subscriptions query-evaluation tests.

use super::*;

fn graph_contains_point_scan(graph: &GraphBuilder) -> bool {
    match graph {
        GraphBuilder::Table {
            scan: Some(groove::ivm::StaticScanSpec::Point(_)),
            ..
        } => true,
        GraphBuilder::Recursive { seed, step, .. } => {
            graph_contains_point_scan(seed) || graph_contains_point_scan(step)
        }
        GraphBuilder::Filter { input, .. }
        | GraphBuilder::UnwrapNullable { input, .. }
        | GraphBuilder::Unnest { input, .. }
        | GraphBuilder::VariantProject { input, .. }
        | GraphBuilder::Project { input, .. }
        | GraphBuilder::StreamingChecksum { input, .. }
        | GraphBuilder::ArgMaxBy { input, .. }
        | GraphBuilder::ArgMinBy { input, .. }
        | GraphBuilder::TopBy { input, .. }
        | GraphBuilder::CollectBy { input, .. }
        | GraphBuilder::Aggregate { input, .. } => graph_contains_point_scan(input),
        GraphBuilder::Union { inputs } => inputs.iter().any(graph_contains_point_scan),
        GraphBuilder::Join { left, right, .. }
        | GraphBuilder::SemiJoin { left, right, .. }
        | GraphBuilder::AntiJoin { left, right, .. } => {
            graph_contains_point_scan(left) || graph_contains_point_scan(right)
        }
        GraphBuilder::Table { .. }
        | GraphBuilder::InlineRecords { .. }
        | GraphBuilder::Index { .. }
        | GraphBuilder::FrontierSource { .. }
        | GraphBuilder::BindingSource { .. } => false,
    }
}

#[test]
fn maintained_physical_point_hydration_uses_only_its_current_row_source() {
    let (_dir, mut node) = open_node();
    let alice = author(1);
    let target = row(0);
    commit_global_issue(&mut node, 0, "open", alice, 1);
    commit_global_issue(&mut node, 1, "open", alice, 2);

    let shape = Query::from("issues")
        .filter(eq(col("id"), lit(Value::Uuid(target.0))))
        .validate(&node.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let program = node
        .compile_current_query_program_for_read_view(
            &shape,
            &binding,
            DurabilityTier::Global,
            AuthorSubject::SYSTEM,
            CurrentQueryProgramOutput::MaintainedView,
            &ReadViewSpec::default(),
        )
        .expect("compile maintained physical-point program");
    assert!(
        program
            .lowered
            .terminals
            .iter()
            .any(|terminal| graph_contains_point_scan(&terminal.graph)),
        "a maintained physical-point program must retain an exact physical source cap"
    );
    let (receiver, maintained, _schemas, transitions, _tables, _incomplete) = node
        .open_seeded_maintained_subscription_view(
            &shape,
            &binding,
            AuthorSubject::SYSTEM,
            DurabilityTier::Global,
            &ReadViewSpec::default(),
        )
        .unwrap();

    assert_eq!(
        maintained
            .active_result_members()
            .iter()
            .filter_map(crate::protocol::ResultMemberEntry::as_row)
            .map(|(_, row, _)| row)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([target])
    );
    assert_eq!(
        transitions
            .adds
            .iter()
            .filter_map(crate::protocol::ResultMemberEntry::as_row)
            .map(|(_, row, _)| row)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([target])
    );
    node.unsubscribe_groove_subscription(receiver.id());
}

#[test]
fn maintained_policy_point_subscription_keeps_full_current_source_for_deletion_liveness() {
    let schema = owner_policy_schema();
    let (_dir, node) = open_node_with_uuid(NodeUuid::from_bytes([0xc2; 16]), schema.clone());
    let target = row(0x71);
    let shape = Query::from("issues")
        .filter(eq(col("id"), lit(Value::Uuid(target.0))))
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    assert!(
        node.current_query_primary_key_access_paths(&shape, &binding)
            .unwrap()
            .is_empty(),
        "policy-scoped maintained rows must retain their full source so deletion markers can remove them"
    );
}

/// A policy-bearing resource query retains exact scans for literal recursive
/// access and edge rows, while its own current row source remains uncapped.
///
/// alice ──query resource policy──► resources full current source
///       └──literal reachable filters──► exact access + edge sources
#[test]
fn policy_root_retains_reachable_point_access_paths() {
    let schema = public_query_eval_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("teams").column("name", PublicColumnType::Text))
            .table(
                PublicTableSchemaBuilder::new("resources")
                    .column("name", PublicColumnType::Text)
                    .column("owner", PublicColumnType::Uuid)
                    .policies(PublicTablePolicies::new().with_select(
                        PublicPolicyExpr::eq_session(
                            "owner",
                            vec!["claims".to_owned(), "sub".to_owned()],
                        ),
                    )),
            )
            .table(
                PublicTableSchemaBuilder::new("resourceAccess")
                    .fk_column("resource", "resources")
                    .fk_column("team", "teams"),
            )
            .table(
                PublicTableSchemaBuilder::new("teamMemberships")
                    .fk_column("member", "teams")
                    .fk_column("parent", "teams"),
            ),
    );
    let (_dir, node) = open_node_with_uuid(NodeUuid::from_bytes([0xc4; 16]), schema.clone());
    let access = row(0xc5);
    let edge = row(0xc6);
    let shape = Query::from("resources")
        .reachable_via_with_access_filters(
            "resourceAccess",
            "resource",
            "team",
            param("team"),
            [eq(col("id"), lit(Value::Uuid(access.0)))],
            "teamMemberships",
            "member",
            "parent",
            [eq(col("id"), lit(Value::Uuid(edge.0)))],
        )
        .validate_runtime(&schema)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([(
            "team".to_owned(),
            Value::Uuid(row(0xc7).0),
        )]))
        .unwrap();
    let paths = node
        .current_query_primary_key_access_paths(&shape, &binding)
        .unwrap();
    let reachable = &shape.query().reachable[0];

    assert!(
        !paths.contains_key(&root_source_id("resources")),
        "the policy-bearing root must retain its complete current source"
    );
    assert!(
        matches!(
            paths.get(&reachable_access_source_id(reachable, "reachable:0")),
            Some(CurrentAccessPath::PrimaryKey(values)) if values == &[Value::Uuid(access.0)]
        ),
        "the literal access edge remains independently safe to point-scan"
    );
    assert!(
        matches!(
            paths.get(&reachable_edge_source_id(reachable, "reachable:0")),
            Some(CurrentAccessPath::PrimaryKey(values)) if values == &[Value::Uuid(edge.0)]
        ),
        "the literal recursive edge remains independently safe to point-scan"
    );
}

#[test]
fn maintained_policy_point_subscription_retracts_for_delete_and_owner_transfer() {
    let schema = owner_policy_schema();
    let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0xc3; 16]), schema.clone());
    let owner = author(0x72);
    let other_owner = author(0x73);
    node.set_session_claims(
        owner,
        BTreeMap::from([("sub".to_owned(), Value::Uuid(owner.test_uuid()))]),
    );
    let shape = Query::from("issues")
        .filter(eq(col("id"), lit(Value::Uuid(row(0x72).0))))
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut peer = PeerState::client_link(owner);

    let target = row(0x72);
    let initial_tx = commit_global_cells(
        &mut node,
        "issues",
        target,
        BTreeMap::from([
            ("title".to_owned(), Value::String("delete me".to_owned())),
            ("assignee".to_owned(), Value::Uuid(owner.test_uuid())),
            ("requiresAdmin".to_owned(), Value::Bool(false)),
        ]),
        1,
        1,
    );
    let initial = peer.rehydrate_query(&mut node, &shape, &binding).unwrap();
    assert!(matches!(
        initial,
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload { result_member_adds, .. })
            if result_member_adds.iter().filter_map(crate::protocol::ResultMemberEntry::as_row).any(|(_, row_uuid, tx_id)| row_uuid == target && tx_id == initial_tx)
    ));
    commit_global_cells(
        &mut node,
        "issues",
        target,
        BTreeMap::from([
            ("title".to_owned(), Value::String("transferred".to_owned())),
            ("assignee".to_owned(), Value::Uuid(other_owner.test_uuid())),
            ("requiresAdmin".to_owned(), Value::Bool(false)),
        ]),
        2,
        2,
    );
    let transfer_update = peer.query_update(&mut node, &shape, &binding).unwrap();
    assert!(matches!(
        transfer_update,
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload { result_member_removes, .. })
            if result_member_removes.iter().filter_map(crate::protocol::ResultMemberEntry::as_row).any(|(_, row_uuid, tx_id)| row_uuid == target && tx_id == initial_tx)
    ));

    let restored_tx = commit_global_cells(
        &mut node,
        "issues",
        target,
        BTreeMap::from([
            ("title".to_owned(), Value::String("transfer me".to_owned())),
            ("assignee".to_owned(), Value::Uuid(owner.test_uuid())),
            ("requiresAdmin".to_owned(), Value::Bool(false)),
        ]),
        3,
        3,
    );
    let regrant = peer.query_update(&mut node, &shape, &binding).unwrap();
    assert!(matches!(
        regrant,
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload { result_member_adds, .. })
            if result_member_adds.iter().filter_map(crate::protocol::ResultMemberEntry::as_row).any(|(_, row_uuid, tx_id)| row_uuid == target && tx_id == restored_tx)
    ));
    delete_global(&mut node, "issues", target, 4, 4);
    let delete_update = peer.query_update(&mut node, &shape, &binding).unwrap();
    assert!(matches!(
        delete_update,
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload { result_member_removes, .. })
            if result_member_removes.iter().filter_map(crate::protocol::ResultMemberEntry::as_row).any(|(_, row_uuid, tx_id)| row_uuid == target && tx_id == restored_tx)
    ));
}

#[test]
fn query_subscription_result_sets_track_bindings_and_rehydrate() {
    let (_server_dir, mut server) = open_node();
    let (_reader_dir, mut reader) = open_node();
    let alice = author(1);
    let bob = author(2);
    let shape = Query::from("issues")
        .filter(eq(col("assignee"), param("user")))
        .validate(&schema())
        .unwrap();
    let alice_binding = shape
        .bind(BTreeMap::from([(
            "user".to_owned(),
            Value::Uuid(alice.test_uuid()),
        )]))
        .unwrap();
    let bob_binding = shape
        .bind(BTreeMap::from([(
            "user".to_owned(),
            Value::Uuid(bob.test_uuid()),
        )]))
        .unwrap();

    register_query_shape(&mut server, &shape, RegisterShapeOptions::default());
    subscribe_query_binding(&mut server, &shape, &alice_binding);
    subscribe_query_binding(&mut server, &shape, &bob_binding);
    register_query_shape(&mut reader, &shape, RegisterShapeOptions::default());
    subscribe_query_binding(&mut reader, &shape, &alice_binding);
    subscribe_query_binding(&mut reader, &shape, &bob_binding);

    let mut peer = PeerState::new();
    commit_global_issue(&mut server, 0, "open", alice, 1);
    commit_global_issue(&mut server, 1, "open", bob, 2);
    let alice_initial = peer
        .rehydrate_query(&mut server, &shape, &alice_binding)
        .unwrap();
    reader.apply_sync_message_settled(alice_initial).unwrap();
    let bob_initial = peer
        .rehydrate_query(&mut server, &shape, &bob_binding)
        .unwrap();
    reader.apply_sync_message_settled(bob_initial).unwrap();

    assert_eq!(
        reader
            .query_rows(&shape, &alice_binding, DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0)])
    );
    assert_eq!(
        reader
            .query_rows(&shape, &bob_binding, DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(1)])
    );

    commit_global_issue(&mut server, 2, "open", alice, 3);
    let alice_delta = peer
        .query_update(&mut server, &shape, &alice_binding)
        .unwrap();
    reader.apply_sync_message_settled(alice_delta).unwrap();
    let bob_delta = peer
        .query_update(&mut server, &shape, &bob_binding)
        .unwrap();
    reader.apply_sync_message_settled(bob_delta).unwrap();
    assert_eq!(
        reader
            .query_rows(&shape, &alice_binding, DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0), row(2)])
    );

    server
        .apply_sync_message_settled(SyncMessage::Unsubscribe {
            subscription: SubscriptionKey {
                shape_id: shape.shape_id(),
                binding_id: alice_binding.binding_id(),
                read_view: Default::default(),
            },
        })
        .unwrap();
    peer.forget_query_binding(&shape, &alice_binding);
    commit_global_issue(&mut server, 3, "open", alice, 4);
    let removed_delta = peer
        .query_update(&mut server, &shape, &alice_binding)
        .unwrap();
    assert!(matches!(
        removed_delta,
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            result_member_adds,
            result_member_removes,
            ..
        }) if result_member_adds.is_empty() && result_member_removes.is_empty()
    ));

    let reset = peer
        .rehydrate_query(&mut server, &shape, &alice_binding)
        .unwrap();
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        reset_result_set, ..
    }) = &reset
    else {
        panic!("expected view update");
    };
    assert!(reset_result_set);
    reader.apply_sync_message_settled(reset).unwrap();
    assert_eq!(
        reader
            .query_rows(&shape, &alice_binding, DurabilityTier::Global)
            .unwrap()
            .len(),
        3
    );
}

#[test]
fn settled_binding_view_sources_provide_source_coverage_metadata() {
    let (_server_dir, mut server) = open_node();
    let (_reader_dir, mut reader) = open_node();
    let alice = author(1);
    let shape = Query::from("users")
        .filter(eq(col("name"), param("name")))
        .validate(&schema())
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([(
            "name".to_owned(),
            Value::String("alice".to_owned()),
        )]))
        .unwrap();

    register_query_shape(&mut server, &shape, RegisterShapeOptions::default());
    subscribe_query_binding(&mut server, &shape, &binding);
    register_query_shape(&mut reader, &shape, RegisterShapeOptions::default());
    subscribe_query_binding(&mut reader, &shape, &binding);

    commit_global_user(&mut server, alice, "alice", 1);
    let mut peer = PeerState::new();
    let initial = peer.rehydrate_query(&mut server, &shape, &binding).unwrap();
    reader.apply_sync_message_settled(initial).unwrap();

    let settled_binding_view = reader
        .settled_binding_view_key_for_query(&shape, &binding)
        .unwrap()
        .expect("receiver should have a settled binding view after rehydrate");
    let mut request = reader
        .current_query_program_request(
            &shape,
            &binding,
            DurabilityTier::Global,
            AuthorSubject::SYSTEM,
            CurrentQueryProgramOutput::AppRows,
            &ReadViewSpec::default(),
            Some(settled_binding_view),
            QueryAuthorizationMode::TrustedServing,
        )
        .unwrap();
    request
        .output
        .facts
        .insert(ProgramFactKey::SourceCoverage(CoverageScope::Program));

    let program = reader
        .compile_query_program_request(request)
        .expect("settled binding-view source should lower source coverage facts");
    assert!(
        matches!(
            &program.lowered.output,
            ProgramOutputSchemas::RowSet(terminals)
                if terminals.iter().any(|terminal| matches!(
                    terminal,
                    OutputTerminalSchema::Fact(ProgramFactOutput {
                        key: ProgramFactKey::SourceCoverage(CoverageScope::Program),
                        ..
                    })
                ))
        ),
        "compiled program should include a source coverage terminal"
    );
}

#[test]
fn settled_binding_view_root_with_reference_include_sources_lowers() {
    // A settled binding view contains root result membership only. Shapes
    // with implicit reference closures need auxiliary source coverage too,
    // so the mixed settled-root/current-auxiliary read set must still be
    // able to lower coverage facts.
    let (_server_dir, mut server) = open_node();
    let (_reader_dir, mut reader) = open_node();
    let alice = author(1);
    let shape = Query::from("issues")
        .filter(eq(col("assignee"), param("user")))
        .include("assignee")
        .validate(&schema())
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([(
            "user".to_owned(),
            Value::Uuid(alice.test_uuid()),
        )]))
        .unwrap();

    register_query_shape(&mut server, &shape, RegisterShapeOptions::default());
    subscribe_query_binding(&mut server, &shape, &binding);
    register_query_shape(&mut reader, &shape, RegisterShapeOptions::default());
    subscribe_query_binding(&mut reader, &shape, &binding);

    commit_global_cells(
        &mut server,
        "users",
        RowUuid(alice.test_uuid()),
        BTreeMap::from([("name".to_owned(), Value::String("alice".to_owned()))]),
        1,
        1,
    );
    commit_global_issue(&mut server, 0, "open", alice, 2);
    let mut peer = PeerState::new();
    let initial = peer.rehydrate_query(&mut server, &shape, &binding).unwrap();
    reader.apply_sync_message_settled(initial).unwrap();

    let settled_binding_view = reader
        .settled_binding_view_key_for_query(&shape, &binding)
        .unwrap()
        .expect("receiver should have a settled binding view after rehydrate");
    reader.catalogue.current_schema_version_alias = None;
    let request = reader
        .current_query_program_request(
            &shape,
            &binding,
            DurabilityTier::Global,
            alice,
            CurrentQueryProgramOutput::MaintainedView,
            &ReadViewSpec::default(),
            Some(settled_binding_view),
            QueryAuthorizationMode::TrustedServing,
        )
        .unwrap();
    let mut request = request;
    request
        .output
        .facts
        .insert(ProgramFactKey::SourceCoverage(CoverageScope::Program));

    let sources = format!("{:?}", request.reads);
    assert!(sources.contains("SettledBindingView"), "{sources}");
    assert!(sources.contains("VisibleCurrent"), "{sources}");
    reader
        .compile_query_program_request(request)
        .expect("settled binding-view root with current include sources should lower");
}

#[test]
fn query_subscription_ships_provenance_closure_for_local_evaluation() {
    let (_server_dir, mut server) = open_node();
    let (_reader_dir, mut reader) = open_node();
    let alice = author(1);
    let bob = author(2);
    commit_global_user(&mut server, alice, "alice", 1);
    commit_global_user(&mut server, bob, "bob", 2);
    commit_global_issue(&mut server, 0, "open", bob, 3);
    commit_global_issue(&mut server, 1, "open", bob, 4);
    commit_global_member(&mut server, 0, row(0), alice, 5);
    commit_global_member(&mut server, 1, row(1), bob, 6);

    let shape = Query::from("issues")
        .join_via("issue_members", "issue", [eq(col("user"), param("user"))])
        .include("assignee")
        .validate(&schema())
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([(
            "user".to_owned(),
            Value::Uuid(alice.test_uuid()),
        )]))
        .unwrap();
    register_shape_binding_for_receiver(&mut reader, &shape, &binding);
    let mut peer = PeerState::new();
    let update = peer.rehydrate_query(&mut server, &shape, &binding).unwrap();
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds, ..
    }) = &update
    else {
        panic!("expected view update");
    };
    let result_set_tables = result_member_adds
        .iter()
        .filter_map(crate::protocol::ResultMemberEntry::as_row)
        .map(|(table, _, _)| table.to_string())
        .collect::<BTreeSet<_>>();
    assert_eq!(
        result_set_tables,
        BTreeSet::from([
            "issues".to_owned(),
            "issue_members".to_owned(),
            "users".to_owned(),
        ])
    );
    reader.apply_sync_message_settled(update).unwrap();

    let local_rows = reader
        .query_rows(&shape, &binding, DurabilityTier::Local)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();
    assert_eq!(local_rows, BTreeSet::from([row(0)]));
    let settled_rows = reader
        .query_rows(&shape, &binding, DurabilityTier::Global)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();
    assert_eq!(settled_rows, BTreeSet::from([row(0)]));
}
