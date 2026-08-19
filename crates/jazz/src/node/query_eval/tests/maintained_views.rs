//! maintained views query-evaluation tests.

use super::*;

#[test]
fn settled_edge_authority_preserves_an_ordinary_local_content_update() {
    let (_server_dir, mut server) = open_node();
    let (_client_dir, mut client) = open_node();
    let issue = row(0);
    let shape = Query::from("issues")
        .select(["title", "state", "assignee", "priority"])
        .order_by("title", OrderDirection::Asc)
        .validate(&schema())
        .expect("validate issues query");
    let binding = shape.bind(BTreeMap::new()).expect("bind issues query");
    let opts = RegisterShapeOptions {
        tier: DurabilityTier::Edge,
        ..RegisterShapeOptions::default()
    };
    register_query_shape(&mut server, &shape, opts.clone());
    subscribe_query_binding(&mut server, &shape, &binding);
    register_query_shape(&mut client, &shape, opts.clone());
    subscribe_query_binding(&mut client, &shape, &binding);

    let initial_tx = commit_global_issue(&mut server, 0, "open", AuthorId::SYSTEM, 1);
    let mut peer = PeerState::edge_client(AuthorId::SYSTEM);
    let initial = peer
        .rehydrate_query_with_opts(&mut server, &shape, &binding, opts.clone())
        .expect("serve initial settled issues view");
    client
        .apply_sync_message(initial)
        .expect("apply initial settled issues view");
    let binding_view = *client
        .query
        .settled_result_sets
        .keys()
        .find(|key| key.shape_id == shape.shape_id() && key.binding_id == binding.binding_id())
        .expect("applied ViewUpdate registers a settled binding view");
    assert!(client.has_settled_result_set(binding_view));

    let (local_shape, local_binding, local_plan) = client
        .prepare_query_binding_for_link_in_authorization_mode(
            &shape,
            &binding,
            DurabilityTier::Local,
            AuthorId::SYSTEM,
            QueryAuthorizationMode::ClientLocal,
        )
        .expect("prepare client-local maintained issues query");
    let (mut local, initial_snapshot) = client
        .open_maintained_view_subscription_in_authorization_mode(
            &local_shape,
            &local_binding,
            AuthorId::SYSTEM,
            DurabilityTier::Local,
            &ReadViewSpec::default(),
            Some(local_plan),
            QueryAuthorizationMode::ClientLocal,
        )
        .expect("open client-local maintained issues query");
    assert_eq!(initial_snapshot.root_count, 1);
    client.seed_local_maintained_authoritative_generation(&mut local, binding_view);

    let updated_tx = client
        .commit_mergeable(
            MergeableCommit::new("issues", issue, 2_000)
                .made_by(AuthorId::SYSTEM)
                .parents(vec![initial_tx])
                .cells(BTreeMap::from([
                    (
                        "title".to_owned(),
                        Value::String("updated title".to_owned()),
                    ),
                    ("state".to_owned(), Value::String("open".to_owned())),
                    ("assignee".to_owned(), Value::Uuid(AuthorId::SYSTEM.0)),
                    ("priority".to_owned(), Value::U64(0)),
                ])),
        )
        .expect("commit ordinary local issue update");
    let _ = updated_tx;

    let update = client
        .drain_local_maintained_view_subscription(&mut local, Some(binding_view))
        .expect("drain client-local maintained update")
        .expect("ordinary content update produces a delta");
    assert!(!update.authoritative_membership_changed);
    let issue_occurrence = OutputOccurrenceId::single_source(ObjectId::from_uuid(issue.0));
    assert!(update.added.iter().any(|(id, _)| id == &issue_occurrence));
    assert!(update.removed.iter().any(|id| id == &issue_occurrence));
    let updated = update
        .added
        .iter()
        .find(|(id, _)| id == &issue_occurrence)
        .expect("updated issue is paired as an add/remove update");
    assert_eq!(
        updated.1.cell(client.table("issues").unwrap(), "title"),
        Some(Value::String("updated title".to_owned()))
    );
}

#[test]
fn maintained_root_order_keeps_occurrence_sidecar_aligned() {
    let descriptor =
        RecordDescriptor::new([("row_uuid", ValueType::Uuid), ("user_rank", ValueType::U64)]);
    let make_row = |id: u8, rank: u64| {
        CurrentRow::new(
            "todos",
            OwnedRecord::new(
                descriptor
                    .create(&[
                        Value::Uuid(uuid::Uuid::from_bytes([id; 16])),
                        Value::U64(rank),
                    ])
                    .expect("test row"),
                descriptor,
            ),
        )
    };
    let occurrence = |id: u8| {
        OutputOccurrenceId::single_source(ObjectId::from_uuid(uuid::Uuid::from_bytes([id; 16])))
    };
    let mut rows = vec![make_row(0xa1, 3), make_row(0xb2, 1), make_row(0xc3, 2)];
    let mut occurrences = vec![occurrence(0xa1), occurrence(0xb2), occurrence(0xc3)];
    let query = Query::from("todos").order_by("rank", OrderDirection::Asc);
    let table = TableSchema::new("todos", [ColumnSchema::new("rank", ColumnType::U64)]);

    NodeState::<RocksDbStorage>::sort_query_rows_with_occurrences(
        &query,
        Some(&table),
        &mut rows,
        &mut occurrences,
    )
    .expect("sort maintained roots");

    assert_eq!(
        rows.iter().map(CurrentRow::row_uuid).collect::<Vec<_>>(),
        vec![
            RowUuid(uuid::Uuid::from_bytes([0xb2; 16])),
            RowUuid(uuid::Uuid::from_bytes([0xc3; 16])),
            RowUuid(uuid::Uuid::from_bytes([0xa1; 16]))
        ]
    );
    assert_eq!(
        occurrences,
        vec![occurrence(0xb2), occurrence(0xc3), occurrence(0xa1)]
    );
}

#[test]
fn branch_program_maintained_view_provides_branch_deletion_witness_source() {
    // A maintained branch source carries both the overlay content and its
    // deletion witness, so replacement, delete, and restore can remain
    // live without falling back to a one-shot branch read.
    let (_dir, mut node) = open_node();
    let branch_id = BranchId::from_bytes([0x42; 16]);
    node.create_branch(branch_id).unwrap();
    node.commit_mergeable_on_branch(
        branch_id,
        MergeableCommit::new("issues", row(1), 1_000).cells(BTreeMap::from([
            ("title".to_owned(), Value::String("branch issue".to_owned())),
            ("state".to_owned(), Value::String("open".to_owned())),
            ("assignee".to_owned(), Value::Uuid(author(0xa1).0)),
            ("priority".to_owned(), Value::U64(1)),
        ])),
    )
    .unwrap();

    let shape = Query::from("issues")
        .validate(&node.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let app_rows = node
        .query_rows_on_branch_query_engine(branch_id, &shape, &binding, AuthorId::SYSTEM)
        .unwrap();
    assert_eq!(
        app_rows
            .iter()
            .map(CurrentRow::row_uuid)
            .collect::<Vec<_>>(),
        vec![row(1)]
    );

    node.compile_branch_query_program_in_authorization_mode(
        branch_id,
        &shape,
        &binding,
        AuthorId::SYSTEM,
        CurrentQueryProgramOutput::MaintainedView,
        QueryAuthorizationMode::TrustedServing,
    )
    .expect("maintained branch compilation must provide deletion witnesses");
}

#[test]
fn branch_program_maintained_view_tracks_local_overlay_replacement() {
    let (_dir, mut node) = open_node();
    let branch_id = BranchId::from_bytes([0x43; 16]);
    node.create_branch(branch_id).unwrap();
    let issue = row(7);
    node.commit_mergeable_on_branch(
        branch_id,
        MergeableCommit::new("issues", issue, 1_000).cells(BTreeMap::from([
            ("title".to_owned(), Value::String("first title".to_owned())),
            ("state".to_owned(), Value::String("open".to_owned())),
            ("assignee".to_owned(), Value::Uuid(author(0xa1).0)),
            ("priority".to_owned(), Value::U64(1)),
        ])),
    )
    .unwrap();
    let shape = Query::from("issues")
        .validate(&node.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let read_view = ReadViewSpec {
        source: ReadViewSourceSpec::Branch {
            branch: branch_id.0,
        },
        ..ReadViewSpec::default()
    };
    let (mut local, initial) = node
        .open_maintained_view_subscription_in_authorization_mode(
            &shape,
            &binding,
            AuthorId::SYSTEM,
            DurabilityTier::Local,
            &read_view,
            None,
            QueryAuthorizationMode::TrustedServing,
        )
        .unwrap();
    assert_eq!(initial.root_count, 1);

    node.commit_mergeable_on_branch(
        branch_id,
        MergeableCommit::new("issues", issue, 2_000).cells(BTreeMap::from([
            ("title".to_owned(), Value::String("second title".to_owned())),
            ("state".to_owned(), Value::String("open".to_owned())),
            ("assignee".to_owned(), Value::Uuid(author(0xa1).0)),
            ("priority".to_owned(), Value::U64(1)),
        ])),
    )
    .unwrap();
    let update = node
        .drain_local_maintained_view_subscription(&mut local, None)
        .unwrap()
        .expect("branch overlay replacement must reach the maintained terminal");
    assert!(
        update.added.iter().any(|(_, row)| row.row_uuid() == issue),
        "replacement must leave a current row in the maintained result"
    );

    node.commit_mergeable_on_branch(
        branch_id,
        MergeableCommit::new("issues", issue, 3_000).deletion(DeletionEvent::Deleted),
    )
    .unwrap();
    let deletion = node
        .drain_local_maintained_view_subscription(&mut local, None)
        .unwrap()
        .expect("branch deletion must reach the maintained terminal");
    assert!(
        deletion.removed.iter().any(|occurrence| {
            *occurrence
                == crate::tools::OutputOccurrenceId::single_source(
                    crate::tools::ObjectId::from_uuid(issue.0),
                )
        }),
        "branch deletion must retract the overlay row"
    );

    node.commit_mergeable_on_branch(
        branch_id,
        MergeableCommit::new("issues", issue, 4_000).deletion(DeletionEvent::Restored),
    )
    .unwrap();
    let restoration = node
        .drain_local_maintained_view_subscription(&mut local, None)
        .unwrap()
        .expect("branch restoration must reach the maintained terminal");
    assert!(
        restoration
            .added
            .iter()
            .any(|(_, row)| row.row_uuid() == issue),
        "branch restoration must reintroduce the overlay row"
    );
}

#[test]
fn branch_program_maintained_view_survives_first_overlay_partition_write() {
    let (_dir, mut node) = open_node();
    let branch_id = BranchId::from_bytes([0x44; 16]);
    let issue = row(7);
    commit_global_issue(&mut node, 7, "open", author(0xa1), 1);
    node.create_branch(branch_id).unwrap();
    let table_id = node
        .physical_table_id_for_schema(node.catalogue.current_schema_version_id, "issues")
        .unwrap();
    assert!(
        !node
            .branches
            .branch_partitions
            .contains(&(table_id, branch_id)),
        "the durable sparse partition must not exist before the first overlay write"
    );

    let shape = Query::from("issues")
        .validate(&node.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let read_view = ReadViewSpec {
        source: ReadViewSourceSpec::Branch {
            branch: branch_id.0,
        },
        ..ReadViewSpec::default()
    };
    let (mut subscription, initial) = node
        .open_maintained_view_subscription_in_authorization_mode(
            &shape,
            &binding,
            AuthorId::SYSTEM,
            DurabilityTier::Edge,
            &read_view,
            None,
            QueryAuthorizationMode::TrustedServing,
        )
        .unwrap();
    assert_eq!(
        initial.root_count, 1,
        "frozen base is available before overlay"
    );
    assert!(
        !node
            .branches
            .branch_partitions
            .contains(&(table_id, branch_id)),
        "opening the maintained view must not publish a branch partition"
    );

    let first_overlay = node
        .commit_mergeable_on_branch(
            branch_id,
            MergeableCommit::new("issues", issue, 2_000).cells(BTreeMap::from([
                (
                    "title".to_owned(),
                    Value::String("first overlay".to_owned()),
                ),
                ("state".to_owned(), Value::String("open".to_owned())),
                ("assignee".to_owned(), Value::Uuid(author(0xa1).0)),
                ("priority".to_owned(), Value::U64(7)),
            ])),
        )
        .unwrap();
    node.apply_fate_update(
        first_overlay,
        Fate::Accepted,
        None,
        Some(DurabilityTier::Edge),
    )
    .unwrap();
    assert!(
        node.branches
            .branch_partitions
            .contains(&(table_id, branch_id)),
        "the first accepted overlay write must durably publish its partition"
    );
    let update = node
        .drain_local_maintained_view_subscription(&mut subscription, None)
        .unwrap()
        .expect("first overlay write must keep the pre-existing subscription live");
    assert!(
        update.added.iter().any(|(_, row)| {
            row.row_uuid() == issue
                && row.cell(node.table("issues").unwrap(), "title")
                    == Some(Value::String("first overlay".to_owned()))
        }),
        "the first accepted overlay write must produce its exact replacement delta"
    );
}

#[test]
fn branch_program_maintained_views_isolate_sibling_first_writes() {
    let (_dir, mut node) = open_node();
    let first_branch = BranchId::from_bytes([0x45; 16]);
    let sibling_branch = BranchId::from_bytes([0x46; 16]);
    let issue = row(7);
    commit_global_issue(&mut node, 7, "open", author(0xa1), 1);
    node.create_branch(first_branch).unwrap();
    node.create_branch(sibling_branch).unwrap();

    let shape = Query::from("issues")
        .validate(&node.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let first_view = ReadViewSpec {
        source: ReadViewSourceSpec::Branch {
            branch: first_branch.0,
        },
        ..ReadViewSpec::default()
    };
    let sibling_view = ReadViewSpec {
        source: ReadViewSourceSpec::Branch {
            branch: sibling_branch.0,
        },
        ..ReadViewSpec::default()
    };
    let (mut first_subscription, first_initial) = node
        .open_maintained_view_subscription_in_authorization_mode(
            &shape,
            &binding,
            AuthorId::SYSTEM,
            DurabilityTier::Edge,
            &first_view,
            None,
            QueryAuthorizationMode::TrustedServing,
        )
        .unwrap();
    let (mut sibling_subscription, sibling_initial) = node
        .open_maintained_view_subscription_in_authorization_mode(
            &shape,
            &binding,
            AuthorId::SYSTEM,
            DurabilityTier::Edge,
            &sibling_view,
            None,
            QueryAuthorizationMode::TrustedServing,
        )
        .unwrap();
    assert_eq!(first_initial.root_count, 1);
    assert_eq!(sibling_initial.root_count, 1);

    let first_write = node
        .commit_mergeable_on_branch(
            first_branch,
            MergeableCommit::new("issues", issue, 2_000).cells(BTreeMap::from([
                ("title".to_owned(), Value::String("first branch".to_owned())),
                ("state".to_owned(), Value::String("open".to_owned())),
                ("assignee".to_owned(), Value::Uuid(author(0xa1).0)),
                ("priority".to_owned(), Value::U64(7)),
            ])),
        )
        .unwrap();
    node.apply_fate_update(
        first_write,
        Fate::Accepted,
        None,
        Some(DurabilityTier::Edge),
    )
    .unwrap();
    let first_update = node
        .drain_local_maintained_view_subscription(&mut first_subscription, None)
        .unwrap()
        .expect("first branch must receive its own accepted overlay update");
    assert!(
        first_update
            .added
            .iter()
            .any(|(_, row)| row.row_uuid() == issue)
    );
    assert!(
        node.drain_local_maintained_view_subscription(&mut sibling_subscription, None)
            .unwrap()
            .is_none(),
        "a sibling branch subscription must not receive first branch deltas"
    );
}

#[test]
fn branch_program_maintained_view_settles_overlay_fates_at_every_tier() {
    for (tier, acceptance) in [
        (DurabilityTier::Local, (None, DurabilityTier::Edge)),
        (DurabilityTier::Edge, (None, DurabilityTier::Edge)),
        (
            DurabilityTier::Global,
            (Some(GlobalSeq(4)), DurabilityTier::Global),
        ),
    ] {
        let (_dir, mut node) = open_node();
        let branch_id = BranchId::from_bytes([tier as u8 + 0x50; 16]);
        let issue = row(7);
        let frozen_only_issue = row(8);
        commit_global_issue(&mut node, 7, "open", author(0xa1), 1);
        commit_global_issue(&mut node, 8, "open", author(0xa1), 2);
        node.create_branch(branch_id).unwrap();
        let initial_overlay = node
            .commit_mergeable_on_branch(
                branch_id,
                MergeableCommit::new("issues", issue, 2_500).cells(BTreeMap::from([
                    (
                        "title".to_owned(),
                        Value::String("initial overlay".to_owned()),
                    ),
                    ("state".to_owned(), Value::String("open".to_owned())),
                    ("assignee".to_owned(), Value::Uuid(author(0xa1).0)),
                    ("priority".to_owned(), Value::U64(7)),
                ])),
            )
            .unwrap();
        node.apply_fate_update(
            initial_overlay,
            Fate::Accepted,
            Some(GlobalSeq(3)),
            Some(DurabilityTier::Global),
        )
        .unwrap();

        let shape = Query::from("issues")
            .validate(&node.catalogue.schema)
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let read_view = ReadViewSpec {
            source: ReadViewSourceSpec::Branch {
                branch: branch_id.0,
            },
            ..ReadViewSpec::default()
        };
        let (mut subscription, initial) = node
            .open_maintained_view_subscription_in_authorization_mode(
                &shape,
                &binding,
                AuthorId::SYSTEM,
                tier,
                &read_view,
                None,
                QueryAuthorizationMode::TrustedServing,
            )
            .unwrap();
        assert_eq!(
            initial.root_count, 2,
            "{tier:?} subscription must include the frozen base"
        );

        let replacement = node
            .commit_mergeable_on_branch(
                branch_id,
                MergeableCommit::new("issues", issue, 3_000).cells(BTreeMap::from([
                    (
                        "title".to_owned(),
                        Value::String("overlay title".to_owned()),
                    ),
                    ("state".to_owned(), Value::String("open".to_owned())),
                    ("assignee".to_owned(), Value::Uuid(author(0xa1).0)),
                    ("priority".to_owned(), Value::U64(7)),
                ])),
            )
            .unwrap();
        if tier == DurabilityTier::Local {
            assert!(
                node.drain_local_maintained_view_subscription(&mut subscription, None)
                    .unwrap()
                    .is_some(),
                "Local subscriptions must see pending branch writes"
            );
        } else {
            assert!(
                node.drain_local_maintained_view_subscription(&mut subscription, None)
                    .unwrap()
                    .is_none(),
                "{tier:?} subscriptions must not expose pending branch writes"
            );
        }
        node.apply_fate_update(
            replacement,
            Fate::Accepted,
            acceptance.0,
            Some(acceptance.1),
        )
        .unwrap();
        if tier >= DurabilityTier::Edge {
            let update = node
                .drain_local_maintained_view_subscription(&mut subscription, None)
                .unwrap()
                .expect("accepted branch replacement must reach the requested tier");
            assert!(update.added.iter().any(|(_, row)| row.row_uuid() == issue));
        }

        let deletion = node
            .commit_mergeable_on_branch(
                branch_id,
                MergeableCommit::new("issues", frozen_only_issue, 4_000)
                    .deletion(DeletionEvent::Deleted),
            )
            .unwrap();
        let deletion_acceptance = match tier {
            DurabilityTier::Global => (Some(GlobalSeq(5)), DurabilityTier::Global),
            _ => (None, DurabilityTier::Edge),
        };
        if tier == DurabilityTier::Local {
            let pending_deletion = node
                .drain_local_maintained_view_subscription(&mut subscription, None)
                .unwrap()
                .expect("Local branch deletion must publish while pending");
            assert!(pending_deletion.removed.iter().any(|occurrence| {
                *occurrence
                    == crate::tools::OutputOccurrenceId::single_source(
                        crate::tools::ObjectId::from_uuid(frozen_only_issue.0),
                    )
            }));
        } else {
            assert!(
                node.drain_local_maintained_view_subscription(&mut subscription, None)
                    .unwrap()
                    .is_none(),
                "{tier:?} subscriptions must not expose pending branch deletion"
            );
        }
        node.apply_fate_update(
            deletion,
            Fate::Accepted,
            deletion_acceptance.0,
            Some(deletion_acceptance.1),
        )
        .unwrap();
        if tier >= DurabilityTier::Edge {
            let deletion_update = node
                .drain_local_maintained_view_subscription(&mut subscription, None)
                .unwrap()
                .expect("accepted branch deletion must reach the requested tier");
            assert!(
                deletion_update.removed.iter().any(|occurrence| {
                    *occurrence
                        == crate::tools::OutputOccurrenceId::single_source(
                            crate::tools::ObjectId::from_uuid(frozen_only_issue.0),
                        )
                }),
                "{tier:?} branch deletion must mask frozen-base membership"
            );
        }

        let restoration = node
            .commit_mergeable_on_branch(
                branch_id,
                MergeableCommit::new("issues", frozen_only_issue, 5_000)
                    .deletion(DeletionEvent::Restored),
            )
            .unwrap();
        let restoration_acceptance = match tier {
            DurabilityTier::Global => (Some(GlobalSeq(6)), DurabilityTier::Global),
            _ => (None, DurabilityTier::Edge),
        };
        if tier == DurabilityTier::Local {
            let pending_restoration = node
                .drain_local_maintained_view_subscription(&mut subscription, None)
                .unwrap()
                .expect("Local branch restore must publish while pending");
            assert!(
                pending_restoration
                    .added
                    .iter()
                    .any(|(_, row)| row.row_uuid() == frozen_only_issue)
            );
        } else {
            assert!(
                node.drain_local_maintained_view_subscription(&mut subscription, None)
                    .unwrap()
                    .is_none(),
                "{tier:?} subscriptions must not expose pending branch restore"
            );
        }
        node.apply_fate_update(
            restoration,
            Fate::Accepted,
            restoration_acceptance.0,
            Some(restoration_acceptance.1),
        )
        .unwrap();
        if tier >= DurabilityTier::Edge {
            let restoration_update = node
                .drain_local_maintained_view_subscription(&mut subscription, None)
                .unwrap()
                .expect("accepted branch restore must reach the requested tier");
            assert!(
                restoration_update
                    .added
                    .iter()
                    .any(|(_, row)| row.row_uuid() == frozen_only_issue),
                "{tier:?} branch restore must re-expose the frozen base"
            );
        }
    }
}

#[test]
fn branch_program_maintained_view_retracts_rejected_pending_overlay_versions() {
    for tier in [
        DurabilityTier::Local,
        DurabilityTier::Edge,
        DurabilityTier::Global,
    ] {
        let (_dir, mut node) = open_node();
        let branch_id = BranchId::from_bytes([tier as u8 + 0x60; 16]);
        let issue = row(7);
        let rejected_only = row(9);
        commit_global_issue(&mut node, 7, "open", author(0xa1), 1);
        node.create_branch(branch_id).unwrap();
        let accepted = node
            .commit_mergeable_on_branch(
                branch_id,
                MergeableCommit::new("issues", issue, 2_500).cells(BTreeMap::from([
                    (
                        "title".to_owned(),
                        Value::String("accepted overlay".to_owned()),
                    ),
                    ("state".to_owned(), Value::String("open".to_owned())),
                    ("assignee".to_owned(), Value::Uuid(author(0xa1).0)),
                    ("priority".to_owned(), Value::U64(7)),
                ])),
            )
            .unwrap();
        node.apply_fate_update(
            accepted,
            Fate::Accepted,
            Some(GlobalSeq(3)),
            Some(DurabilityTier::Global),
        )
        .unwrap();

        let shape = Query::from("issues")
            .validate(&node.catalogue.schema)
            .unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let read_view = ReadViewSpec {
            source: ReadViewSourceSpec::Branch {
                branch: branch_id.0,
            },
            ..ReadViewSpec::default()
        };
        let (mut subscription, initial) = node
            .open_maintained_view_subscription_in_authorization_mode(
                &shape,
                &binding,
                AuthorId::SYSTEM,
                tier,
                &read_view,
                None,
                QueryAuthorizationMode::TrustedServing,
            )
            .unwrap();
        assert!(initial.rows.iter().any(|current| {
            current.row_uuid() == issue
                && current.cell(node.table("issues").unwrap(), "title")
                    == Some(Value::String("accepted overlay".to_owned()))
        }));

        let rejected_replacement = node
            .commit_mergeable_on_branch(
                branch_id,
                MergeableCommit::new("issues", issue, 3_000).cells(BTreeMap::from([
                    (
                        "title".to_owned(),
                        Value::String("rejected replacement".to_owned()),
                    ),
                    ("state".to_owned(), Value::String("open".to_owned())),
                    ("assignee".to_owned(), Value::Uuid(author(0xa1).0)),
                    ("priority".to_owned(), Value::U64(8)),
                ])),
            )
            .unwrap();
        if tier == DurabilityTier::Local {
            let pending = node
                .drain_local_maintained_view_subscription(&mut subscription, None)
                .unwrap()
                .expect("Local must expose a pending replacement");
            assert!(pending.added.iter().any(|(_, current)| {
                current.row_uuid() == issue
                    && current.cell(node.table("issues").unwrap(), "title")
                        == Some(Value::String("rejected replacement".to_owned()))
            }));
        } else {
            assert!(
                node.drain_local_maintained_view_subscription(&mut subscription, None)
                    .unwrap()
                    .is_none(),
                "{tier:?} must not expose a pending replacement"
            );
        }
        node.apply_fate_update(
            rejected_replacement,
            Fate::Rejected(crate::tx::RejectionReason::AuthorizationDenied),
            None,
            None,
        )
        .unwrap();
        if tier == DurabilityTier::Local {
            let retracted = node
                .drain_local_maintained_view_subscription(&mut subscription, None)
                .unwrap()
                .expect("rejecting a pending replacement must restore the accepted winner");
            assert!(retracted.added.iter().any(|(_, current)| {
                current.row_uuid() == issue
                    && current.cell(node.table("issues").unwrap(), "title")
                        == Some(Value::String("accepted overlay".to_owned()))
            }));
        } else {
            assert!(
                node.drain_local_maintained_view_subscription(&mut subscription, None)
                    .unwrap()
                    .is_none(),
                "a rejected replacement must never perturb {tier:?}"
            );
        }

        let rejected_insert = node
            .commit_mergeable_on_branch(
                branch_id,
                MergeableCommit::new("issues", rejected_only, 4_000).cells(BTreeMap::from([
                    (
                        "title".to_owned(),
                        Value::String("rejected insert".to_owned()),
                    ),
                    ("state".to_owned(), Value::String("open".to_owned())),
                    ("assignee".to_owned(), Value::Uuid(author(0xa1).0)),
                    ("priority".to_owned(), Value::U64(9)),
                ])),
            )
            .unwrap();
        if tier == DurabilityTier::Local {
            let pending = node
                .drain_local_maintained_view_subscription(&mut subscription, None)
                .unwrap()
                .expect("Local must expose a pending insert");
            assert!(
                pending
                    .added
                    .iter()
                    .any(|(_, current)| current.row_uuid() == rejected_only)
            );
        } else {
            assert!(
                node.drain_local_maintained_view_subscription(&mut subscription, None)
                    .unwrap()
                    .is_none(),
                "{tier:?} must not expose a pending insert"
            );
        }
        node.apply_fate_update(
            rejected_insert,
            Fate::Rejected(crate::tx::RejectionReason::AuthorizationDenied),
            None,
            None,
        )
        .unwrap();
        if tier == DurabilityTier::Local {
            let retracted = node
                .drain_local_maintained_view_subscription(&mut subscription, None)
                .unwrap()
                .expect("rejecting a pending insert must retract it");
            assert!(retracted.removed.iter().any(|occurrence| {
                *occurrence
                    == crate::tools::OutputOccurrenceId::single_source(
                        crate::tools::ObjectId::from_uuid(rejected_only.0),
                    )
            }));
        } else {
            assert!(
                node.drain_local_maintained_view_subscription(&mut subscription, None)
                    .unwrap()
                    .is_none(),
                "a rejected insert must never perturb {tier:?}"
            );
        }
    }
}

#[test]
fn recursive_reachability_subscription_grants_and_revokes_incrementally() {
    let (_dir, mut core) = open_recursive_node();
    let schema = recursive_schema();
    let team1 = row(1);
    let team2 = row(2);
    let team3 = row(3);
    let team4 = row(4);
    let resource1 = row(101);
    let resource2 = row(102);
    commit_global_cells(
        &mut core,
        "resources",
        resource1,
        BTreeMap::from([("name".to_owned(), Value::String("r1".to_owned()))]),
        10,
        1,
    );
    commit_global_cells(
        &mut core,
        "resources",
        resource2,
        BTreeMap::from([("name".to_owned(), Value::String("r2".to_owned()))]),
        11,
        2,
    );
    commit_global_cells(
        &mut core,
        "resourceAccess",
        row(201),
        BTreeMap::from([
            ("resource".to_owned(), Value::Uuid(resource1.0)),
            ("team".to_owned(), Value::Uuid(team3.0)),
        ]),
        12,
        3,
    );
    commit_global_cells(
        &mut core,
        "resourceAccess",
        row(202),
        BTreeMap::from([
            ("resource".to_owned(), Value::Uuid(resource2.0)),
            ("team".to_owned(), Value::Uuid(team4.0)),
        ]),
        13,
        4,
    );
    for (idx, member, parent, seq) in [(301, team1, team2, 5), (302, team2, team3, 6)] {
        commit_global_cells(
            &mut core,
            "teamTeamMemberships",
            row(idx),
            BTreeMap::from([
                ("member".to_owned(), Value::Uuid(member.0)),
                ("parent".to_owned(), Value::Uuid(parent.0)),
                ("onlyAdmins".to_owned(), Value::Bool(false)),
            ]),
            10 + seq,
            seq,
        );
    }

    let shape = recursive_shape(&schema);
    let binding = shape
        .bind(BTreeMap::from([("team".to_owned(), Value::Uuid(team1.0))]))
        .unwrap();
    let mut peer = PeerState::new();
    let initial = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    assert!(matches!(
        initial,
        SyncMessage::ViewUpdate {
            result_member_adds,
            ..
        } if result_member_adds.iter().filter_map(crate::protocol::ResultMemberEntry::as_row).any(|(_, row_uuid, _)| row_uuid == resource1)
            && result_member_adds.iter().filter_map(crate::protocol::ResultMemberEntry::as_row).all(|(_, row_uuid, _)| row_uuid != resource2)
    ));

    commit_global_cells(
        &mut core,
        "teamTeamMemberships",
        row(303),
        BTreeMap::from([
            ("member".to_owned(), Value::Uuid(team3.0)),
            ("parent".to_owned(), Value::Uuid(team4.0)),
            ("onlyAdmins".to_owned(), Value::Bool(false)),
        ]),
        17,
        7,
    );
    let grant = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert!(matches!(
        grant,
        SyncMessage::ViewUpdate {
            result_member_adds,
            result_member_removes,
            ..
        } if result_member_adds.iter().filter_map(crate::protocol::ResultMemberEntry::as_row).any(|(_, row_uuid, _)| row_uuid == resource2)
            && result_member_removes.is_empty()
    ));

    delete_global(&mut core, "teamTeamMemberships", row(302), 18, 8);
    let revoke = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert!(matches!(
        revoke,
        SyncMessage::ViewUpdate {
            result_member_adds,
            result_member_removes,
            ..
        } if result_member_adds.is_empty()
            && result_member_removes.iter().filter_map(crate::protocol::ResultMemberEntry::as_row).any(|(_, row_uuid, _)| row_uuid == resource1)
            && result_member_removes.iter().filter_map(crate::protocol::ResultMemberEntry::as_row).any(|(_, row_uuid, _)| row_uuid == resource2)
    ));
}
