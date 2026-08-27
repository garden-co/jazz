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

    let initial_tx = commit_global_issue(&mut server, 0, "open", author(0), 1);
    let mut peer = PeerState::edge_client(AuthorSubject::SYSTEM);
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };
    let initial = peer
        .rehydrate_query_for_subscription_with_opts(
            &mut server,
            subscription,
            &shape,
            &binding,
            opts.clone(),
        )
        .expect("serve initial settled issues view")
        .expect("initial settled issues view is ready");
    client
        .apply_sync_message_settled(initial)
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
            AuthorSubject::SYSTEM,
            QueryAuthorizationMode::ClientLocal,
        )
        .expect("prepare client-local maintained issues query");
    let (mut local, initial_snapshot) = client
        .open_maintained_view_subscription_in_authorization_mode(
            &local_shape,
            &local_binding,
            AuthorSubject::SYSTEM,
            DurabilityTier::Local,
            &ReadViewSpec::default(),
            Some(local_plan),
            QueryAuthorizationMode::ClientLocal,
        )
        .expect("open client-local maintained issues query");
    assert_eq!(initial_snapshot.root_count, 1);
    client.seed_local_maintained_authoritative_generation(&mut local, binding_view);

    let updated_tx = client
        .commit_mergeable_settled(
            MergeableCommit::new("issues", issue, 2_000)
                .made_by(AuthorSubject::SYSTEM)
                .parents(vec![initial_tx])
                .cells(BTreeMap::from([
                    (
                        "title".to_owned(),
                        Value::String("updated title".to_owned()),
                    ),
                    ("state".to_owned(), Value::String("open".to_owned())),
                    ("assignee".to_owned(), Value::Uuid(uuid::Uuid::nil())),
                    ("priority".to_owned(), Value::U64(0)),
                ])),
        )
        .expect("commit ordinary local issue update");
    let _ = updated_tx;

    let update = client
        .drain_local_maintained_view_subscription(&mut local, Some(binding_view))
        .expect("drain client-local maintained update")
        .expect("ordinary content update produces a delta");
    let LocalMaintainedViewSubscriptionUpdate::Flat {
        authoritative_membership_changed,
        added,
        removed,
        ..
    } = update
    else {
        panic!("flat issue query produced a structured maintained update");
    };
    assert!(!authoritative_membership_changed);
    let issue_occurrence = OutputOccurrenceId::single_source(ObjectId::from_uuid(issue.0));
    assert!(added.iter().any(|(id, _)| id == &issue_occurrence));
    assert!(removed.iter().any(|id| id == &issue_occurrence));
    let updated = added
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
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            result_member_adds,
            ..
        }) if result_member_adds.iter().filter_map(crate::protocol::ResultMemberEntry::as_row).any(|(_, row_uuid, _)| row_uuid == resource1)
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
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            result_member_adds,
            result_member_removes,
            ..
        }) if result_member_adds.iter().filter_map(crate::protocol::ResultMemberEntry::as_row).any(|(_, row_uuid, _)| row_uuid == resource2)
            && result_member_removes.is_empty()
    ));

    delete_global(&mut core, "teamTeamMemberships", row(302), 18, 8);
    let revoke = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert!(matches!(
        revoke,
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            result_member_adds,
            result_member_removes,
            ..
        }) if result_member_adds.is_empty()
            && result_member_removes.iter().filter_map(crate::protocol::ResultMemberEntry::as_row).any(|(_, row_uuid, _)| row_uuid == resource1)
            && result_member_removes.iter().filter_map(crate::protocol::ResultMemberEntry::as_row).any(|(_, row_uuid, _)| row_uuid == resource2)
    ));
}
