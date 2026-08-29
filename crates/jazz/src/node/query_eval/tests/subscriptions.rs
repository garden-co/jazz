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
        GraphBuilder::Union { inputs } => {
            inputs.iter().any(|input| graph_contains_point_scan(input))
        }
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

/// Storage-backed scalar membership ships the exact deletion-register winner
/// alongside a restored row, so a separate reader's later ordinary lookup
/// agrees with the subscription.
///
/// server ──insert/delete/restore──► peer ──ViewUpdate──► reader
#[test]
fn storage_backed_maintained_delivery_keeps_implicit_reference_witnesses_and_rehydrates_scalar_rows()
 {
    // A public query without `.include()` can still carry an implicit root
    // reference closure.  It is not safe to drop witnesses for that shape:
    // a separate receiver needs the referenced user body to render an issue.
    let (_issue_dir, mut issue_node) = open_node();
    let issue_shape = Query::from("issues")
        .filter(eq(col("assignee"), param("user")))
        .validate(&issue_node.catalogue.schema)
        .expect("validate reference-bearing query");
    let issue_binding = issue_shape
        .bind(BTreeMap::from([(
            "user".to_owned(),
            Value::Uuid(author(1).test_uuid()),
        )]))
        .expect("bind reference-bearing query");
    let issue_program = issue_node
        .compile_current_query_program_for_read_view(
            &issue_shape,
            &issue_binding,
            DurabilityTier::Global,
            AuthorSubject::SYSTEM,
            CurrentQueryProgramOutput::MaintainedView,
            &ReadViewSpec::default(),
        )
        .expect("compile reference-bearing maintained query");
    assert!(
        issue_program
            .request
            .output
            .facts
            .contains(&crate::node::query_engine::ProgramFactKey::VersionWitnesses),
        "implicit normalized reference closures keep self-contained witnesses"
    );

    // The optimized path remains available to a genuinely scalar root table,
    // including the actual remote initial and incremental receiver path.
    let scalar_schema = public_query_eval_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("notes").column("title", PublicColumnType::Text)),
    );
    let (_server_dir, mut server) =
        open_node_with_uuid(NodeUuid::from_bytes([0x41; 16]), scalar_schema.clone());
    let (_reader_dir, mut reader) =
        open_node_with_uuid(NodeUuid::from_bytes([0x42; 16]), scalar_schema);
    let shape = Query::from("notes")
        .validate(&server.catalogue.schema)
        .expect("validate scalar query");
    let binding = shape.bind(BTreeMap::new()).expect("bind scalar query");
    let program = server
        .compile_current_query_program_for_read_view(
            &shape,
            &binding,
            DurabilityTier::Global,
            AuthorSubject::SYSTEM,
            CurrentQueryProgramOutput::MaintainedView,
            &ReadViewSpec::default(),
        )
        .expect("compile scalar maintained query");
    assert!(
        !program
            .request
            .output
            .facts
            .contains(&crate::node::query_engine::ProgramFactKey::VersionWitnesses),
        "scalar root query uses storage-backed result materialization"
    );
    for node in [&mut server, &mut reader] {
        register_query_shape(node, &shape, RegisterShapeOptions::default());
        subscribe_query_binding(node, &shape, &binding);
    }
    commit_global_cells(
        &mut server,
        "notes",
        row(0),
        BTreeMap::from([("title".to_owned(), Value::String("first".to_owned()))]),
        1,
        1,
    );
    let mut peer = PeerState::new();
    let initial = peer
        .rehydrate_query(&mut server, &shape, &binding)
        .expect("serve scalar initial update");
    reader
        .apply_sync_message_settled(initial)
        .expect("separate reader applies scalar initial update");
    commit_global_cells(
        &mut server,
        "notes",
        row(1),
        BTreeMap::from([("title".to_owned(), Value::String("second".to_owned()))]),
        2,
        2,
    );
    let delta = peer
        .query_update(&mut server, &shape, &binding)
        .expect("serve scalar incremental update");
    reader
        .apply_sync_message_settled(delta)
        .expect("separate reader applies scalar incremental update");
    assert_eq!(
        reader
            .query_rows(&shape, &binding, DurabilityTier::Global)
            .expect("read separately materialized scalar rows")
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0), row(1)])
    );

    // Stream B removals never need to load a newer winner, while restoration
    // must load exactly the restored content witness from the authoritative
    // store. Exercise both across the separate receiver boundary.
    delete_global(&mut server, "notes", row(0), 3, 3);
    let removal = peer
        .query_update(&mut server, &shape, &binding)
        .expect("serve scalar removal update");
    reader
        .apply_sync_message_settled(removal)
        .expect("separate reader applies scalar removal update");
    assert_eq!(
        reader
            .query_rows(&shape, &binding, DurabilityTier::Global)
            .expect("read scalar rows after deletion")
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(1)])
    );
    let restore_tx = server
        .commit_mergeable_settled(
            MergeableCommit::new("notes", row(0), 4)
                .made_by(AuthorSubject::SYSTEM)
                .deletion(crate::tx::DeletionEvent::Restored),
        )
        .expect("restore scalar row");
    server
        .apply_fate_update(
            restore_tx,
            Fate::Accepted,
            Some(GlobalTime(4)),
            Some(DurabilityTier::Global),
        )
        .expect("accept scalar restore");
    let restore = peer
        .query_update(&mut server, &shape, &binding)
        .expect("serve scalar restoration update");
    let restore_bundles = match &restore {
        SyncMessage::ViewUpdate(payload) => {
            crate::protocol::expand_version_carriers(&payload.version_carriers)
                .expect("restore update carriers should expand")
        }
        _ => panic!("scalar subscription must produce a view update"),
    };
    assert!(
        restore_bundles.iter().any(|bundle| {
            bundle.tx.tx_id == restore_tx
                && bundle.versions.iter().any(|version| {
                    version.table() == "notes"
                        && version.row_uuid() == row(0)
                        && version.deletion() == Some(crate::tx::DeletionEvent::Restored)
                })
        }),
        "the storage-backed restore must ship its deletion-register winner, not merely re-add the old content member"
    );
    reader
        .apply_sync_message_settled(restore)
        .expect("separate reader applies scalar restoration update");
    assert_eq!(
        reader
            .query_rows(&shape, &binding, DurabilityTier::Global)
            .expect("read scalar rows after restoration")
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0), row(1)])
    );
}

/// Storage-backed scalar subscriptions resolve deletion/restore winners at
/// their own Local or Edge current frontier instead of accidentally reading
/// the Global register.
///
/// server ──tier-accepted insert/delete/restore──► peer ──ViewUpdate──► reader
/// server ──later Local delete (Edge only)───────► Edge frontier unchanged
#[test]
fn storage_backed_maintained_deletion_winners_follow_local_and_edge_frontiers() {
    for tier in [DurabilityTier::Local, DurabilityTier::Edge] {
        let scalar_schema =
            public_query_eval_schema(PublicSchemaBuilder::new().table(
                PublicTableSchemaBuilder::new("notes").column("title", PublicColumnType::Text),
            ));
        let (_server_dir, mut server) = open_node_with_uuid(
            NodeUuid::from_bytes([0x50 + tier as u8; 16]),
            scalar_schema.clone(),
        );
        let (_reader_dir, mut reader) =
            open_node_with_uuid(NodeUuid::from_bytes([0x60 + tier as u8; 16]), scalar_schema);
        let shape = Query::from("notes")
            .validate(&server.catalogue.schema)
            .expect("validate scalar tier query");
        let binding = shape.bind(BTreeMap::new()).expect("bind scalar tier query");
        let program = server
            .compile_current_query_program_for_read_view(
                &shape,
                &binding,
                tier,
                AuthorSubject::SYSTEM,
                CurrentQueryProgramOutput::MaintainedView,
                &ReadViewSpec::default(),
            )
            .expect("compile tiered scalar maintained query");
        assert!(
            !program
                .request
                .output
                .facts
                .contains(&crate::node::query_engine::ProgramFactKey::VersionWitnesses)
                && !program
                    .request
                    .output
                    .facts
                    .contains(&crate::node::query_engine::ProgramFactKey::ReplacementWitnesses),
            "{tier:?} scalar maintained view uses storage-backed witnesses"
        );
        let opts = RegisterShapeOptions {
            tier,
            ..RegisterShapeOptions::default()
        };
        let subscription = SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: opts.read_view_key(),
        };
        for node in [&mut server, &mut reader] {
            register_query_shape(node, &shape, opts.clone());
            subscribe_query_binding_with_opts(node, &shape, &binding, opts.clone());
        }

        let commit = |node: &mut NodeState<RocksDbStorage>, now_ms, deletion| {
            let mut write =
                MergeableCommit::new("notes", row(0), now_ms).made_by(AuthorSubject::SYSTEM);
            if let Some(deletion) = deletion {
                write = write.deletion(deletion);
            } else {
                write = write.cells(BTreeMap::from([(
                    "title".to_owned(),
                    Value::String("tiered".to_owned()),
                )]));
            }
            let tx_id = node
                .commit_mergeable_settled(write)
                .expect("commit tiered row");
            node.apply_fate_update(tx_id, Fate::Accepted, None, Some(tier))
                .expect("accept tiered row");
            tx_id
        };

        let initial_tx = commit(&mut server, 1, None);
        let mut peer = PeerState::new();
        let initial = peer
            .rehydrate_query_with_opts(&mut server, &shape, &binding, opts.clone())
            .expect("serve initial tiered update");
        reader
            .apply_sync_message_settled(initial)
            .expect("reader applies tiered initial update");

        let deletion_tx = commit(&mut server, 2, Some(crate::tx::DeletionEvent::Deleted));
        let deletion = peer
            .query_update_for_subscription_with_opts(
                &mut server,
                subscription,
                &shape,
                &binding,
                opts.clone(),
            )
            .expect("serve tiered deletion update")
            .expect("tiered deletion changes membership");
        reader
            .apply_sync_message_settled(deletion)
            .expect("reader applies tiered deletion update");
        assert!(
            reader
                .query_rows(&shape, &binding, tier)
                .expect("read tiered deletion")
                .is_empty(),
            "{tier:?} deletion hides the scalar row"
        );

        let restore_tx = commit(&mut server, 3, Some(crate::tx::DeletionEvent::Restored));
        let restored = peer
            .query_update_for_subscription_with_opts(
                &mut server,
                subscription,
                &shape,
                &binding,
                opts.clone(),
            )
            .expect("serve tiered restore update")
            .expect("tiered restore changes membership");
        let restored_bundles = match &restored {
            SyncMessage::ViewUpdate(payload) => {
                crate::protocol::expand_version_carriers(&payload.version_carriers)
                    .expect("tiered restore carriers should expand")
            }
            _ => panic!("tiered scalar subscription must produce a view update"),
        };
        assert!(
            restored_bundles.iter().any(|bundle| {
                bundle.tx.tx_id == restore_tx
                    && bundle.versions.iter().any(|version| {
                        version.row_uuid() == row(0)
                            && version.deletion() == Some(crate::tx::DeletionEvent::Restored)
                    })
            }),
            "{tier:?} restore ships its visible register winner"
        );
        reader
            .apply_sync_message_settled(restored)
            .expect("reader applies tiered restore update");
        assert_eq!(
            reader
                .query_rows(&shape, &binding, tier)
                .expect("read restored tiered row")
                .into_iter()
                .map(|row| row.row_uuid())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([row(0)]),
            "{tier:?} ordinary reader lookup agrees with restored membership"
        );

        if tier == DurabilityTier::Edge {
            let local_delete = server
                .commit_mergeable_settled(
                    MergeableCommit::new("notes", row(0), 4)
                        .made_by(AuthorSubject::SYSTEM)
                        .deletion(crate::tx::DeletionEvent::Deleted),
                )
                .expect("commit Local-only deletion");
            server
                .apply_fate_update(
                    local_delete,
                    Fate::Accepted,
                    None,
                    Some(DurabilityTier::Local),
                )
                .expect("accept Local-only deletion");
            // This deliberately overwrites the ahead-current register at
            // Local. Edge's executable source filters it out and sees no
            // membership transition; its materialized reader stays visible.
            if let Some(edge_after_local) = peer
                .query_update_for_subscription_with_opts(
                    &mut server,
                    subscription,
                    &shape,
                    &binding,
                    opts,
                )
                .expect("evaluate Edge after Local-only register write")
            {
                let SyncMessage::ViewUpdate(payload) = &edge_after_local else {
                    panic!("Edge scalar subscription must produce a view update");
                };
                assert!(
                    payload.result_member_adds.is_empty()
                        && payload.result_member_removes.is_empty(),
                    "a Local-only deletion must not change Edge result membership"
                );
                reader
                    .apply_sync_message_settled(edge_after_local)
                    .expect("reader applies no-op Edge update");
            }
            assert_eq!(
                reader
                    .query_rows(&shape, &binding, DurabilityTier::Edge)
                    .expect("read Edge after Local-only register write")
                    .into_iter()
                    .map(|row| row.row_uuid())
                    .collect::<BTreeSet<_>>(),
                BTreeSet::from([row(0)]),
                "Edge remains at its prior visible register frontier"
            );
        }

        assert_ne!(initial_tx, deletion_tx, "tiered transition tx ids differ");
    }
}

/// A fresh Edge reader must receive the earlier Edge-visible restore even when
/// a newer Local-only register event occupies the raw ahead-current key.
///
/// server: Global delete t2 ──► Edge restore t3 ──► Local delete t4
///                                     │                   │
///                                     └────fresh Edge reader sees t3─────┘
#[test]
fn storage_backed_edge_restore_filters_before_ahead_current_winner_selection() {
    let scalar_schema = public_query_eval_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("notes").column("title", PublicColumnType::Text)),
    );
    let (_server_dir, mut server) =
        open_node_with_uuid(NodeUuid::from_bytes([0x73; 16]), scalar_schema.clone());
    let shape = Query::from("notes")
        .validate(&server.catalogue.schema)
        .expect("validate Edge shadow query");
    let binding = shape.bind(BTreeMap::new()).expect("bind Edge shadow query");
    let edge_opts = RegisterShapeOptions {
        tier: DurabilityTier::Edge,
        ..RegisterShapeOptions::default()
    };
    register_query_shape(&mut server, &shape, edge_opts.clone());
    subscribe_query_binding_with_opts(&mut server, &shape, &binding, edge_opts.clone());

    let content_tx = server
        .commit_mergeable_settled(
            MergeableCommit::new("notes", row(0), 1)
                .made_by(AuthorSubject::SYSTEM)
                .cells(BTreeMap::from([(
                    "title".to_owned(),
                    Value::String("visible".to_owned()),
                )])),
        )
        .expect("commit global content");
    server
        .apply_fate_update(
            content_tx,
            Fate::Accepted,
            Some(GlobalTime(1)),
            Some(DurabilityTier::Global),
        )
        .expect("accept global content");
    let global_delete_tx = server
        .commit_mergeable_settled(
            MergeableCommit::new("notes", row(0), 2)
                .made_by(AuthorSubject::SYSTEM)
                .deletion(crate::tx::DeletionEvent::Deleted),
        )
        .expect("commit global deletion");
    server
        .apply_fate_update(
            global_delete_tx,
            Fate::Accepted,
            Some(GlobalTime(2)),
            Some(DurabilityTier::Global),
        )
        .expect("accept global deletion");
    let edge_restore_tx = server
        .commit_mergeable_settled(
            MergeableCommit::new("notes", row(0), 3)
                .made_by(AuthorSubject::SYSTEM)
                .deletion(crate::tx::DeletionEvent::Restored),
        )
        .expect("commit Edge restore");
    server
        .apply_fate_update(
            edge_restore_tx,
            Fate::Accepted,
            None,
            Some(DurabilityTier::Edge),
        )
        .expect("accept Edge restore");
    let local_delete_tx = server
        .commit_mergeable_settled(
            MergeableCommit::new("notes", row(0), 4)
                .made_by(AuthorSubject::SYSTEM)
                .deletion(crate::tx::DeletionEvent::Deleted),
        )
        .expect("commit Local-only deletion");
    server
        .apply_fate_update(
            local_delete_tx,
            Fate::Accepted,
            None,
            Some(DurabilityTier::Local),
        )
        .expect("accept Local-only deletion");

    let (_reader_dir, mut reader) =
        open_node_with_uuid(NodeUuid::from_bytes([0x74; 16]), scalar_schema);
    register_query_shape(&mut reader, &shape, edge_opts.clone());
    subscribe_query_binding_with_opts(&mut reader, &shape, &binding, edge_opts.clone());
    let update = PeerState::new()
        .rehydrate_query_with_opts(&mut server, &shape, &binding, edge_opts)
        .expect("serve fresh Edge hydration");
    let bundles = match &update {
        SyncMessage::ViewUpdate(payload) => {
            crate::protocol::expand_version_carriers(&payload.version_carriers)
                .expect("fresh Edge carriers should expand")
        }
        _ => panic!("fresh Edge scalar subscription must produce a view update"),
    };
    assert!(
        bundles.iter().any(|bundle| {
            bundle.tx.tx_id == edge_restore_tx
                && bundle.versions.iter().any(|version| {
                    version.row_uuid() == row(0)
                        && version.deletion() == Some(crate::tx::DeletionEvent::Restored)
                })
        }),
        "fresh Edge hydration ships t3 instead of the Global t2 deletion or Local t4 deletion"
    );
    reader
        .apply_sync_message_settled(update)
        .expect("fresh reader applies Edge hydration");
    assert_eq!(
        reader
            .query_rows(&shape, &binding, DurabilityTier::Edge)
            .expect("fresh reader Edge lookup")
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0)]),
        "fresh reader matches the source's filter-before-argmax Edge view"
    );
}

#[test]
fn relay_authority_session_key_is_explicit_and_does_not_replace_direct_edge_source() {
    let (_dir, mut node) = open_node();
    let shape = Query::from("issues")
        .validate(&node.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let ordinary_direct = node
        .client_settled_binding_view_key_for_query(
            &shape,
            &binding,
            DurabilityTier::Edge,
            &ReadViewSpec::default(),
        )
        .expect("durable direct Edge reads use their upstream Global source");
    let expected_ordinary = BindingViewKey::new(
        shape.shape_id(),
        binding.binding_id(),
        RegisterShapeOptions::default().read_view_key(),
    );
    assert_eq!(ordinary_direct, expected_ordinary);

    node.set_relay_authority_session_owner();
    assert_eq!(
        node.client_settled_binding_view_key_for_query(
            &shape,
            &binding,
            DurabilityTier::Edge,
            &ReadViewSpec::default(),
        ),
        Some(expected_ordinary),
        "marking a worker must not retag ordinary direct Edge settlement",
    );
    let relay_authority =
        node.relay_authority_session_binding_view_key(&shape, &binding, &ReadViewSpec::default());
    assert_ne!(relay_authority, ordinary_direct);
    assert_eq!(
        relay_authority,
        BindingViewKey::new(
            shape.shape_id(),
            binding.binding_id(),
            RegisterShapeOptions {
                tier: DurabilityTier::Global,
                binding_source: BindingSource::RelayAuthoritySession,
                ..RegisterShapeOptions::default()
            }
            .read_view_key(),
        ),
        "only the relay publication path uses the distinct authority ingress key",
    );
}

#[test]
fn relay_authority_source_selection_requires_read_policy_for_exact_id() {
    fn selected(schema: JazzSchema) -> bool {
        let (_dir, mut node) =
            open_node_with_uuid(NodeUuid::from_bytes([0x35; 16]), schema.clone());
        node.set_relay_authority_session_owner();
        let shape = Query::from("docs")
            .filter(eq(col("id"), lit(Value::Uuid(row(0x36).0))))
            .validate_runtime(&schema)
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        node.relay_edge_query_requires_authority_source(&shape, &binding)
            .unwrap()
    }

    let table = || PublicTableSchemaBuilder::new("docs").column("title", PublicColumnType::Text);
    let no_policy = public_query_eval_schema(PublicSchemaBuilder::new().table(table()));
    let read_policy =
        public_query_eval_schema(PublicSchemaBuilder::new().table(
            table().policies(PublicTablePolicies::new().with_select(PublicPolicyExpr::True)),
        ));
    let write_only_policy =
        public_query_eval_schema(PublicSchemaBuilder::new().table(
            table().policies(PublicTablePolicies::new().with_insert(PublicPolicyExpr::True)),
        ));

    assert!(
        !selected(no_policy),
        "public point reads stay on the ordinary relay path"
    );
    assert!(
        selected(read_policy),
        "read-policy revocation requires authority membership"
    );
    assert!(
        !selected(write_only_policy),
        "write-only policy cannot revoke read membership and must not select a second source",
    );
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

/// A point-scoped policy subscription admits alice's issue, then retracts it
/// when ownership changes or the row is deleted.
///
/// alice ──subscribe──► node ──owner transfer/delete──► retraction
#[test]
fn maintained_policy_point_subscription_retracts_for_delete_and_owner_transfer() {
    let schema = owner_policy_schema();
    let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0xc3; 16]), schema.clone());
    let owner = author(0x72);
    let other_owner = author(0x73);
    node.set_test_provider_claims(
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
