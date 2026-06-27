use crate::query::{Include, JoinMode, OrderDirection};

#[test]
fn write_policy_rejection_cleans_up_client() {
    let schema = owner_policy_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let author = user(0xa1);
    let other = user(0xb2);
    let row_uuid = row(1);
    let (tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row_uuid, 10)
                .made_by(author)
                .cells(owner_cells(other, "wrong owner")),
        )
        .unwrap();

    let [fate] = core.apply_sync_message(unit).unwrap().try_into().unwrap();
    assert_eq!(
        fate,
        SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            global_seq: None,
            durability: None,
        }
    );
    writer.apply_sync_message(fate).unwrap();
    assert!(
        writer
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn session_owner_string_uuid_write_policy_accepts_matching_author() {
    let schema = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("owner_id", ColumnType::String),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::shape(
        Query::from("todos").filter(eq(col("owner_id"), claim("user_id"))),
    ))]);
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let author = user(0xa1);
    let row_uuid = row(0x51);
    let (tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row_uuid, 10)
                .made_by(author)
                .cells(BTreeMap::from([
                    ("title".to_owned(), Value::String("owned".to_owned())),
                    ("owner_id".to_owned(), Value::String(author.0.to_string())),
                ])),
        )
        .unwrap();

    let [fate] = core.apply_sync_message(unit).unwrap().try_into().unwrap();
    assert_eq!(
        fate,
        SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_seq: Some(GlobalSeq(1)),
            durability: Some(DurabilityTier::Global),
        }
    );
    assert_eq!(
        core.current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<Vec<_>>(),
        vec![(
            row_uuid,
            BTreeMap::from([
                ("title".to_owned(), Value::String("owned".to_owned())),
                ("owner_id".to_owned(), Value::String(author.0.to_string())),
            ]),
        )]
    );
}

#[test]
fn owner_only_delete_requires_current_owner() {
    let schema = owner_policy_schema();
    let (_owner_dir, mut owner_writer) = open_node_with_schema(node(1), schema.clone());
    let (_other_dir, mut other_writer) = open_node_with_schema(node(2), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let owner = user(0xa1);
    let other = user(0xb2);
    let row_uuid = row(1);
    let create = commit_owner_policy_global(
        &mut owner_writer,
        &mut core,
        row_uuid,
        owner,
        owner,
        "owned",
        10,
    );

    let (bad_delete, bad_unit) = other_writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row_uuid, 11)
                .made_by(other)
                .parents(vec![create])
                .deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    let [bad_fate] = core
        .apply_sync_message(bad_unit)
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(
        bad_fate,
        SyncMessage::FateUpdate {
            tx_id: bad_delete,
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            global_seq: None,
            durability: None,
        }
    );

    let (good_delete, good_unit) = owner_writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row_uuid, 12)
                .made_by(owner)
                .parents(vec![create])
                .deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    let [good_fate] = core
        .apply_sync_message(good_unit)
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(
        good_fate,
        SyncMessage::FateUpdate {
            tx_id: good_delete,
            fate: Fate::Accepted,
            global_seq: Some(GlobalSeq(2)),
            durability: Some(DurabilityTier::Global),
        }
    );
    assert!(
        core.current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .is_empty()
    );
}
#[test]
fn owner_only_read_narrows_view_updates_per_peer_identity() {
    let schema = owner_policy_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let (_reader_a_dir, mut reader_a) = open_node_with_schema(node(3), schema.clone());
    let (_reader_b_dir, mut reader_b) = open_node_with_schema(node(4), schema);
    let author_a = user(0xa1);
    let author_b = user(0xb2);
    let tx_a = commit_core_owner_fixture(&mut core, row(1), author_a, "a row", 10);
    let tx_b = commit_core_owner_fixture(&mut core, row(2), author_b, "b row", 11);
    let mut link_a = PeerState::for_author(author_a);
    let mut link_b = PeerState::for_author(author_b);

    let update_a = link_a.current_rows_update(&mut core, "todos").unwrap();
    assert_view_update_only_references_rows(&update_a, BTreeSet::from([row(1)]));
    reader_a.apply_sync_message(update_a).unwrap();
    let update_b = link_b.current_rows_update(&mut core, "todos").unwrap();
    assert_view_update_only_references_rows(&update_b, BTreeSet::from([row(2)]));
    reader_b.apply_sync_message(update_b).unwrap();
    let subscription = core.whole_table_subscription_key("todos").unwrap();

    assert_eq!(
        link_a.subscription_result_sets(subscription),
        Some(BTreeSet::from([tx_a]))
    );
    assert_eq!(
        link_b.subscription_result_sets(subscription),
        Some(BTreeSet::from([tx_b]))
    );
    assert_policy_subscription_rows(&mut reader_a, 42, author_a);
    assert_policy_subscription_rows(&mut reader_b, 43, author_b);
}
#[test]
fn owner_transfer_removes_settled_result_set_without_redacting_local_copy() {
    let schema = owner_policy_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let (_reader_a_dir, mut reader_a) = open_node_with_schema(node(3), schema.clone());
    let (_reader_b_dir, mut reader_b) = open_node_with_schema(node(4), schema);
    let author_a = user(0xa1);
    let author_b = user(0xb2);
    let row_uuid = row(7);
    let tx_a = commit_core_owner_fixture(&mut core, row_uuid, author_a, "owned by A", 10);
    let mut link_a = PeerState::for_author(author_a);

    let update = link_a.current_rows_update(&mut core, "todos").unwrap();
    assert_view_update_only_references_rows(&update, BTreeSet::from([row_uuid]));
    reader_a.apply_sync_message(update).unwrap();
    assert_eq!(
        reader_a
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap(),
        vec![(row_uuid, owner_cells(author_a, "owned by A"))]
    );

    let tx_b = commit_core_owner_fixture(&mut core, row_uuid, author_b, "owned by B", 11);
    let update = link_a.current_rows_update(&mut core, "todos").unwrap();
    let SyncMessage::ViewUpdate {
        version_bundles,
        peer_payload_inventory:
            crate::protocol::PeerPayloadInventory {
                complete_tx_payloads: complete_tx_payload_refs,
            },
        result_row_adds,
        result_row_removes,
        ..
    } = &update
    else {
        panic!("expected view update");
    };
    assert!(version_bundles.is_empty());
    assert!(complete_tx_payload_refs.is_empty());
    assert!(result_row_adds.is_empty());
    assert_eq!(
        result_row_removes,
        &vec![("todos".to_owned().into(), row_uuid, tx_a)]
    );
    reader_a.apply_sync_message(update).unwrap();
    assert!(
        reader_a
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        reader_a
            .subscription_current_rows("todos", DurabilityTier::Local)
            .unwrap(),
        vec![(row_uuid, owner_cells(author_a, "owned by A"))]
    );

    let mut link_b = PeerState::for_author(author_b);
    let update = link_b.current_rows_update(&mut core, "todos").unwrap();
    assert_view_update_only_references_rows(&update, BTreeSet::from([row_uuid]));
    reader_b.apply_sync_message(update).unwrap();
    let subscription = core.whole_table_subscription_key("todos").unwrap();
    assert_eq!(
        link_b.subscription_result_sets(subscription),
        Some(BTreeSet::from([tx_b]))
    );
    assert_eq!(
        reader_b
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap(),
        vec![(row_uuid, owner_cells(author_b, "owned by B"))]
    );
}
#[test]
fn join_policy_authorizes_writes_reads_and_next_emission_revocation() {
    let invited = user(0xa1);
    let uninvited = user(0xb2);
    let canvas_row = row(8);
    let invite_row = row(9);
    let canvas_policy = Policy::shape(Query::from("canvases").join_via(
        "canvasInvites",
        "canvas",
        [eq(col("userID"), claim("sub"))],
    ));
    let schema = JazzSchema::new([
        TableSchema::new("canvases", [ColumnSchema::new("title", ColumnType::String)])
            .with_read_policy(canvas_policy.clone())
            .with_write_policy(canvas_policy),
        TableSchema::new(
            "canvasInvites",
            [
                ColumnSchema::new("canvas", ColumnType::Uuid),
                ColumnSchema::new("userID", ColumnType::Uuid),
            ],
        )
        .with_reference("canvas", "canvases"),
    ]);
    let (_uninvited_writer_dir, mut uninvited_writer) =
        open_node_with_schema(node(1), schema.clone());
    let (_invited_writer_dir, mut invited_writer) = open_node_with_schema(node(2), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let (_invited_dir, mut invited_reader) = open_node_with_schema(node(3), schema.clone());
    let (_uninvited_dir, mut uninvited_reader) = open_node_with_schema(node(4), schema);

    let denied_tx = uninvited_writer
        .commit_mergeable_unit(
            MergeableCommit::new("canvases", canvas_row, 10)
                .made_by(uninvited)
                .cells(BTreeMap::from([(
                    "title".to_owned(),
                    Value::String("blocked".to_owned()),
                )])),
        )
        .unwrap();
    let [denied] = core
        .apply_sync_message(denied_tx.1)
        .unwrap()
        .try_into()
        .unwrap();
    assert!(matches!(
        denied,
        SyncMessage::FateUpdate {
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            ..
        }
    ));

    let invite_tx = core
        .commit_mergeable(MergeableCommit::new("canvasInvites", invite_row, 11).cells(
            BTreeMap::from([
                ("canvas".to_owned(), Value::Uuid(canvas_row.0)),
                ("userID".to_owned(), Value::Uuid(invited.0)),
            ]),
        ))
        .unwrap();
    core.apply_fate_update(
        invite_tx,
        Fate::Accepted,
        Some(GlobalSeq(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    let accepted_tx = invited_writer
        .commit_mergeable_unit(
            MergeableCommit::new("canvases", canvas_row, 12)
                .made_by(invited)
                .cells(BTreeMap::from([(
                    "title".to_owned(),
                    Value::String("allowed".to_owned()),
                )])),
        )
        .unwrap();
    let accepted_id = accepted_tx.0;
    let [accepted] = core
        .apply_sync_message(accepted_tx.1)
        .unwrap()
        .try_into()
        .unwrap();
    assert!(matches!(
        accepted,
        SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            ..
        }
    ));
    assert!(matches!(
        core.transaction_state(accepted_id),
        Some((Fate::Accepted, _, DurabilityTier::Global))
    ));

    let mut invited_link = PeerState::for_author(invited);
    let invited_update = invited_link
        .current_rows_update(&mut core, "canvases")
        .unwrap();
    invited_reader.apply_sync_message(invited_update).unwrap();
    assert_eq!(
        invited_reader
            .subscription_current_rows("canvases", DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(|row| {
                let table = &invited_reader.catalogue.schema.tables[0];
                (
                    row.row_uuid(),
                    BTreeMap::from([(
                        "title".to_owned(),
                        row.cell(table, "title").expect("title cell"),
                    )]),
                )
            })
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(
            canvas_row,
            BTreeMap::from([("title".to_owned(), Value::String("allowed".to_owned()))])
        )])
    );

    let mut uninvited_link = PeerState::for_author(uninvited);
    let uninvited_update = uninvited_link
        .current_rows_update(&mut core, "canvases")
        .unwrap();
    uninvited_reader
        .apply_sync_message(uninvited_update)
        .unwrap();
    assert!(
        uninvited_reader
            .subscription_current_rows("canvases", DurabilityTier::Global)
            .unwrap()
            .is_empty()
    );

    let revoke_tx = core
        .commit_mergeable(
            MergeableCommit::new("canvasInvites", invite_row, 13).deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    core.apply_fate_update(
        revoke_tx,
        Fate::Accepted,
        Some(GlobalSeq(3)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let revoked_update = invited_link
        .current_rows_update(&mut core, "canvases")
        .unwrap();
    let SyncMessage::ViewUpdate {
        result_row_removes, ..
    } = &revoked_update
    else {
        panic!("expected view update");
    };
    assert_eq!(
        result_row_removes,
        &vec![("canvases".to_owned().into(), canvas_row, accepted_id)]
    );
    invited_reader.apply_sync_message(revoked_update).unwrap();
    assert!(
        invited_reader
            .subscription_current_rows("canvases", DurabilityTier::Global)
            .unwrap()
            .is_empty()
    );
    // Closure-row policy revocation is still checked at emission; C2 composes
    // output-row policies into the subscription graph.
}
#[test]
fn composed_read_policy_grants_and_revokes_incrementally() {
    let invited = user(0xa1);
    let spy = user(0xb2);
    let canvas_row = row(8);
    let shape_row = row(10);
    let invite_row = row(9);
    let shape_policy = Policy::shape(Query::from("shapes").join_via_column(
        "canvasInvites",
        "canvas",
        "canvas",
        [eq(col("userID"), claim("sub"))],
    ));
    let schema = JazzSchema::new([
        TableSchema::new("canvases", [ColumnSchema::new("title", ColumnType::String)]),
        TableSchema::new(
            "shapes",
            [
                ColumnSchema::new("canvas", ColumnType::Uuid),
                ColumnSchema::new("title", ColumnType::String),
            ],
        )
        .with_reference("canvas", "canvases")
        .with_read_policy(shape_policy),
        TableSchema::new(
            "canvasInvites",
            [
                ColumnSchema::new("canvas", ColumnType::Uuid),
                ColumnSchema::new("userID", ColumnType::Uuid),
            ],
        )
        .with_reference("canvas", "canvases"),
    ]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let shape = Query::from("shapes")
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = crate::protocol::SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
    };

    let canvas_tx =
        core.commit_mergeable(MergeableCommit::new("canvases", canvas_row, 10).cells(
            BTreeMap::from([("title".to_owned(), Value::String("policy-row".to_owned()))]),
        ))
        .unwrap();
    core.apply_fate_update(
        canvas_tx,
        Fate::Accepted,
        Some(GlobalSeq(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let shape_tx = core
        .commit_mergeable(
            MergeableCommit::new("shapes", shape_row, 11).cells(BTreeMap::from([
                ("canvas".to_owned(), Value::Uuid(canvas_row.0)),
                ("title".to_owned(), Value::String("policy-row".to_owned())),
            ])),
        )
        .unwrap();
    core.apply_fate_update(
        shape_tx,
        Fate::Accepted,
        Some(GlobalSeq(2)),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    let mut invited_link = PeerState::for_author(invited);
    let mut spy_link = PeerState::for_author(spy);
    let invited_initial = invited_link
        .rehydrate_query(&mut core, &shape, &binding)
        .unwrap();
    let spy_initial = spy_link
        .rehydrate_query(&mut core, &shape, &binding)
        .unwrap();
    assert!(matches!(
        invited_initial,
        SyncMessage::ViewUpdate {
            result_row_adds: ref adds,
            ..
        } if adds.is_empty()
    ));
    assert!(matches!(
        spy_initial,
        SyncMessage::ViewUpdate {
            result_row_adds: ref adds,
            ..
        } if adds.is_empty()
    ));
    assert_eq!(
        core.query
            .query_shape_cache
            .keys()
            .filter(|(_, tier)| *tier == DurabilityTier::Global)
            .count(),
        1,
        "identities with the same shape and policy should share one prepared graph"
    );
    assert_eq!(
        invited_link
            .maintained_subscription_view_metrics()
            .full_recomputes_out,
        0
    );
    assert_eq!(
        spy_link
            .maintained_subscription_view_metrics()
            .full_recomputes_out,
        0
    );

    let invite_tx = core
        .commit_mergeable(MergeableCommit::new("canvasInvites", invite_row, 12).cells(
            BTreeMap::from([
                ("canvas".to_owned(), Value::Uuid(canvas_row.0)),
                ("userID".to_owned(), Value::Uuid(invited.0)),
            ]),
        ))
        .unwrap();
    core.apply_fate_update(
        invite_tx,
        Fate::Accepted,
        Some(GlobalSeq(3)),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    let grant_update = invited_link
        .query_update(&mut core, &shape, &binding)
        .unwrap();
    let SyncMessage::ViewUpdate {
        result_row_adds,
        result_row_removes,
        ..
    } = grant_update
    else {
        panic!("expected grant update");
    };
    assert_eq!(
        result_row_adds,
        vec![
            ("canvases".to_owned().into(), canvas_row, canvas_tx),
            ("shapes".to_owned().into(), shape_row, shape_tx),
        ]
    );
    assert!(result_row_removes.is_empty());
    assert_eq!(invited_link.metrics.view_updates_out, 2);
    assert_eq!(
        invited_link
            .maintained_subscription_view_metrics()
            .full_recomputes_out,
        0
    );

    let spy_update = spy_link.query_update(&mut core, &shape, &binding).unwrap();
    assert!(matches!(
        spy_update,
        SyncMessage::ViewUpdate {
            result_row_adds: ref adds,
            result_row_removes: ref removes,
            ..
        } if adds.is_empty() && removes.is_empty()
    ));
    assert_eq!(spy_link.metrics.result_adds_out, 0);
    assert_eq!(spy_link.metrics.version_bundles_out, 0);
    assert_eq!(
        spy_link
            .maintained_subscription_view_metrics()
            .full_recomputes_out,
        0
    );

    let revoke_tx = core
        .commit_mergeable(
            MergeableCommit::new("canvasInvites", invite_row, 13).deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    core.apply_fate_update(
        revoke_tx,
        Fate::Accepted,
        Some(GlobalSeq(4)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let revoke_update = invited_link
        .query_update(&mut core, &shape, &binding)
        .unwrap();
    let SyncMessage::ViewUpdate {
        result_row_adds,
        result_row_removes,
        ..
    } = revoke_update
    else {
        panic!("expected revoke update");
    };
    assert!(result_row_adds.is_empty());
    assert_eq!(
        result_row_removes,
        vec![
            ("canvases".to_owned().into(), canvas_row, canvas_tx),
            ("shapes".to_owned().into(), shape_row, shape_tx),
        ]
    );
    assert_eq!(invited_link.metrics.view_updates_out, 3);
    assert_eq!(
        invited_link.subscription_result_sets(subscription),
        Some(BTreeSet::new())
    );
    assert_eq!(
        invited_link
            .maintained_subscription_view_metrics()
            .full_recomputes_out,
        0
    );
    assert_eq!(
        spy_link
            .maintained_subscription_view_metrics()
            .full_recomputes_out,
        0
    );
}
#[test]
fn system_identity_read_policy_sees_everything() {
    let schema = owner_policy_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let (_reader_dir, mut reader) = open_node_with_schema(node(3), schema);
    commit_core_owner_fixture(&mut core, row(1), user(0xa1), "a row", 10);
    commit_core_owner_fixture(&mut core, row(2), user(0xb2), "b row", 11);
    let mut peer = PeerState::new();

    let update = peer.current_rows_update(&mut core, "todos").unwrap();
    assert_view_update_only_references_rows(&update, BTreeSet::from([row(1), row(2)]));
    reader.apply_sync_message(update).unwrap();

    assert_eq!(
        reader
            .subscription_current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(1), row(2)])
    );
}

#[test]
fn relay_and_edge_peer_identities_drive_policy_composed_reads() {
    let schema = owner_policy_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let owner = user(0xa1);
    let other = user(0xb2);
    commit_core_owner_fixture(&mut core, row(1), owner, "owned", 10);

    let mut relay = PeerState::relay();
    assert_eq!(relay.identity(), AuthorId::SYSTEM);
    assert_view_update_only_references_rows(
        &relay.current_rows_update(&mut core, "todos").unwrap(),
        BTreeSet::from([row(1)]),
    );

    let mut edge_owner = PeerState::edge_client(owner);
    assert_eq!(edge_owner.identity(), owner);
    assert_view_update_only_references_rows(
        &edge_owner.current_rows_update(&mut core, "todos").unwrap(),
        BTreeSet::from([row(1)]),
    );

    let mut edge_other = PeerState::edge_client(other);
    assert_eq!(edge_other.identity(), other);
    assert_view_update_only_references_rows(
        &edge_other.current_rows_update(&mut core, "todos").unwrap(),
        BTreeSet::new(),
    );
}

#[test]
fn deletion_read_policy_requires_visible_global_content_winner() {
    let schema = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::owner_only("todos", "owner"))]);
    let (_dir, mut core) = open_node_with_schema(node(9), schema);
    let owner = user(0xa1);
    let other = user(0xb2);
    let row_uuid = row(0x81);
    let content = core
        .commit_mergeable(
            MergeableCommit::new("todos", row_uuid, 10).cells(owner_cells(owner, "visible")),
        )
        .unwrap();
    core.apply_fate_update(
        content,
        Fate::Accepted,
        Some(GlobalSeq(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let deletion = core
        .commit_mergeable(
            MergeableCommit::new("todos", row_uuid, 11).deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    core.apply_fate_update(
        deletion,
        Fate::Accepted,
        Some(GlobalSeq(2)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let table = core.table("todos").unwrap().clone();
    let deletion_version = core
        .query_global_layer_winner("todos", row_uuid, VersionLayer::Deletion)
        .unwrap()
        .unwrap();

    assert!(
        core.read_policy_allows_deletion_version(&table, &deletion_version, owner)
            .unwrap()
    );
    assert!(
        !core
            .read_policy_allows_deletion_version(&table, &deletion_version, other)
            .unwrap()
    );

    let orphan_row = row(0x82);
    let orphan_deletion = core
        .commit_mergeable(
            MergeableCommit::new("todos", orphan_row, 12).deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    core.apply_fate_update(
        orphan_deletion,
        Fate::Accepted,
        Some(GlobalSeq(3)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let orphan_version = core
        .query_global_layer_winner("todos", orphan_row, VersionLayer::Deletion)
        .unwrap()
        .unwrap();
    assert!(
        !core
            .read_policy_allows_deletion_version(&table, &orphan_version, owner)
            .unwrap()
    );
}

fn required_include_rls_schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new(
            "roots",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("target", ColumnType::Uuid),
            ],
        )
        .with_reference("target", "targets"),
        TableSchema::new(
            "targets",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("owner", ColumnType::Uuid),
            ],
        )
        .with_read_policy(Policy::owner_only("targets", "owner")),
    ])
}

fn required_include_shape(core: &NodeState<RocksDbStorage>, include: Include) -> ValidatedQuery {
    Query::from("roots")
        .include_with(include)
        .validate(&core.catalogue.schema)
        .unwrap()
}

fn required_include_rows(
    core: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    identity: AuthorId,
) -> Vec<CurrentRow> {
    let binding = shape.bind(BTreeMap::new()).unwrap();
    core.query_rows_for_link(shape, &binding, DurabilityTier::Global, identity)
        .unwrap()
}

fn seed_required_include_fixture(core: &mut NodeState<RocksDbStorage>, readable_owner: AuthorId) {
    let unreadable_owner = user(0xb2);
    let target_tx = core
        .commit_mergeable_many(vec![
            MergeableCommit::new("targets", row(0xc1), 10)
                .cells(owner_cells(unreadable_owner, "hidden target")),
            MergeableCommit::new("targets", row(0xc2), 10)
                .cells(owner_cells(readable_owner, "visible target")),
        ])
        .unwrap();
    core.apply_fate_update(
        target_tx,
        Fate::Accepted,
        Some(core.clock.next_global_seq),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    let root_tx = core
        .commit_mergeable_many(vec![
            MergeableCommit::new("roots", row(0xd1), 20).cells(BTreeMap::from([
                ("title".to_owned(), v("references hidden")),
                ("target".to_owned(), Value::Uuid(row(0xc1).0)),
            ])),
            MergeableCommit::new("roots", row(0xd2), 20).cells(BTreeMap::from([
                ("title".to_owned(), v("references visible")),
                ("target".to_owned(), Value::Uuid(row(0xc2).0)),
            ])),
        ])
        .unwrap();
    core.apply_fate_update(
        root_tx,
        Fate::Accepted,
        Some(core.clock.next_global_seq),
        Some(DurabilityTier::Global),
    )
    .unwrap();
}

fn seed_missing_required_include_fixture(core: &mut NodeState<RocksDbStorage>) {
    let root_tx = core
        .commit_mergeable_many(vec![
            MergeableCommit::new("roots", row(0xd1), 20).cells(BTreeMap::from([
                ("title".to_owned(), v("references missing")),
                ("target".to_owned(), Value::Uuid(row(0xcf).0)),
            ])),
            MergeableCommit::new("roots", row(0xd2), 20).cells(BTreeMap::from([
                ("title".to_owned(), v("references existing")),
                ("target".to_owned(), Value::Uuid(row(0xc2).0)),
            ])),
            MergeableCommit::new("targets", row(0xc2), 10)
                .cells(owner_cells(AuthorId::SYSTEM, "existing target")),
        ])
        .unwrap();
    core.apply_fate_update(
        root_tx,
        Fate::Accepted,
        Some(core.clock.next_global_seq),
        Some(DurabilityTier::Global),
    )
    .unwrap();
}

fn seed_null_required_include_fixture(core: &mut NodeState<RocksDbStorage>) {
    let root_tx = core
        .commit_mergeable_many(vec![
            MergeableCommit::new("roots", row(0xd1), 20)
                .cells(BTreeMap::from([("title".to_owned(), v("references null"))])),
            MergeableCommit::new("roots", row(0xd2), 20).cells(BTreeMap::from([
                ("title".to_owned(), v("references existing")),
                ("target".to_owned(), Value::Uuid(row(0xc2).0)),
            ])),
            MergeableCommit::new("targets", row(0xc2), 10)
                .cells(owner_cells(AuthorId::SYSTEM, "existing target")),
        ])
        .unwrap();
    core.apply_fate_update(
        root_tx,
        Fate::Accepted,
        Some(core.clock.next_global_seq),
        Some(DurabilityTier::Global),
    )
    .unwrap();
}

fn multi_segment_required_include_rls_schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new(
            "roots",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("project", ColumnType::Uuid),
            ],
        )
        .with_reference("project", "projects"),
        TableSchema::new(
            "projects",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("org", ColumnType::Uuid),
            ],
        )
        .with_reference("org", "orgs"),
        TableSchema::new(
            "orgs",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("owner", ColumnType::Uuid),
            ],
        )
        .with_read_policy(Policy::owner_only("orgs", "owner")),
    ])
}

fn seed_multi_segment_include_fixture(
    core: &mut NodeState<RocksDbStorage>,
    readable_owner: AuthorId,
) {
    let unreadable_owner = user(0xb2);
    let tx = core
        .commit_mergeable_many(vec![
            MergeableCommit::new("orgs", row(0xe1), 10)
                .cells(owner_cells(unreadable_owner, "hidden org")),
            MergeableCommit::new("orgs", row(0xe2), 10)
                .cells(owner_cells(readable_owner, "visible org")),
            MergeableCommit::new("projects", row(0xc1), 20).cells(BTreeMap::from([
                ("title".to_owned(), v("project hidden")),
                ("org".to_owned(), Value::Uuid(row(0xe1).0)),
            ])),
            MergeableCommit::new("projects", row(0xc2), 20).cells(BTreeMap::from([
                ("title".to_owned(), v("project visible")),
                ("org".to_owned(), Value::Uuid(row(0xe2).0)),
            ])),
            MergeableCommit::new("projects", row(0xc3), 20).cells(BTreeMap::from([
                ("title".to_owned(), v("project missing")),
                ("org".to_owned(), Value::Uuid(row(0xef).0)),
            ])),
            MergeableCommit::new("roots", row(0xd1), 30).cells(BTreeMap::from([
                ("title".to_owned(), v("references hidden org")),
                ("project".to_owned(), Value::Uuid(row(0xc1).0)),
            ])),
            MergeableCommit::new("roots", row(0xd2), 30).cells(BTreeMap::from([
                ("title".to_owned(), v("references visible org")),
                ("project".to_owned(), Value::Uuid(row(0xc2).0)),
            ])),
            MergeableCommit::new("roots", row(0xd3), 30).cells(BTreeMap::from([
                ("title".to_owned(), v("references missing org")),
                ("project".to_owned(), Value::Uuid(row(0xc3).0)),
            ])),
        ])
        .unwrap();
    core.apply_fate_update(
        tx,
        Fate::Accepted,
        Some(core.clock.next_global_seq),
        Some(DurabilityTier::Global),
    )
    .unwrap();
}

fn canonical_view_update_rows(update: &SyncMessage) -> (Vec<ResultRowEntry>, Vec<ResultRowEntry>) {
    let SyncMessage::ViewUpdate {
        result_row_adds,
        result_row_removes,
        ..
    } = update
    else {
        panic!("expected view update");
    };
    let mut adds = result_row_adds.clone();
    let mut removes = result_row_removes.clone();
    adds.sort();
    removes.sort();
    (adds, removes)
}

fn canonical_view_update_payload(
    update: &SyncMessage,
) -> (
    Vec<String>,
    Vec<TxId>,
    Vec<ResultRowEntry>,
    Vec<ResultRowEntry>,
) {
    let SyncMessage::ViewUpdate {
        version_bundles,
        peer_payload_inventory:
            crate::protocol::PeerPayloadInventory {
                complete_tx_payloads: complete_tx_payload_refs,
            },
        ..
    } = update
    else {
        panic!("expected view update");
    };
    let mut version_bundles = version_bundles
        .iter()
        .map(|bundle| format!("{bundle:?}"))
        .collect::<Vec<_>>();
    version_bundles.sort();
    let mut complete_tx_payload_refs = complete_tx_payload_refs.clone();
    complete_tx_payload_refs.sort();
    let (adds, removes) = canonical_view_update_rows(update);
    (version_bundles, complete_tx_payload_refs, adds, removes)
}

#[test]
fn required_include_unreadable_target_drops_parent() {
    let schema = required_include_rls_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let reader = user(0xa1);
    seed_required_include_fixture(&mut core, reader);
    let shape = required_include_shape(&core, Include::new("target"));

    let rows = required_include_rows(&mut core, &shape, reader);
    assert_eq!(
        rows.into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd2)])
    );
}

#[test]
fn required_include_uses_identity_sensitive_graph_path_without_shared_plan_cache() {
    let schema = required_include_rls_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let reader = user(0xa1);
    seed_required_include_fixture(&mut core, reader);
    let shape = required_include_shape(&core, Include::new("target").require_includes());

    core.clear_prepared_query_plan_cache_for_test();
    let rows = required_include_rows(&mut core, &shape, reader);
    assert_eq!(
        rows.into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd2)])
    );
    assert!(
        core.prepared_query_plan_cache_is_empty_for_test(),
        "identity-sensitive include membership lowering must not enter the shared prepared-plan cache"
    );
}

#[test]
fn inner_multi_segment_include_missing_or_unreadable_second_hop_drops_parent() {
    let schema = multi_segment_required_include_rls_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let reader = user(0xa1);
    seed_multi_segment_include_fixture(&mut core, reader);
    let shape = required_include_shape(&core, Include::new("project.org"));

    let rows = required_include_rows(&mut core, &shape, reader);
    assert_eq!(
        rows.into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd2)])
    );
}

#[test]
fn maintained_subscription_view_multi_segment_inner_include_matches_full_recompute() {
    let schema = multi_segment_required_include_rls_schema();
    let (_full_recompute_dir, mut full_recompute_core) =
        open_node_with_schema(node(9), schema.clone());
    let (_maintained_dir, mut maintained_core) = open_node_with_schema(node(9), schema);
    let reader = user(0xa1);
    seed_multi_segment_include_fixture(&mut full_recompute_core, reader);
    seed_multi_segment_include_fixture(&mut maintained_core, reader);
    let shape = required_include_shape(&maintained_core, Include::new("project.org"));
    let binding = shape.bind(BTreeMap::new()).unwrap();
    assert!(
        maintained_core.supported_maintained_view(&shape, &binding, reader),
        "multi-segment inner include should be accepted by maintained subscription view support"
    );

    let mut maintained_peer = PeerState::for_author(reader);

    let full_recompute_rows = required_include_rows(&mut full_recompute_core, &shape, reader);
    assert_eq!(
        full_recompute_rows
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd2)])
    );

    let maintained = maintained_peer
        .rehydrate_query(&mut maintained_core, &shape, &binding)
        .unwrap();
    assert_eq!(
        maintained_peer
            .maintained_subscription_view_metrics()
            .full_recomputes_out,
        0
    );
    assert_eq!(
        maintained_peer
            .maintained_subscription_view_metrics()
            .hits_out,
        1
    );

    let (result_adds, result_removes) = canonical_view_update_rows(&maintained);
    assert!(result_removes.is_empty());
    assert_eq!(
        result_adds
            .iter()
            .filter(|entry| entry.0.as_str() == "roots")
            .map(|entry| entry.1)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd2)])
    );
    assert_view_update_only_references_rows(
        &maintained,
        BTreeSet::from([row(0xd2), row(0xc2), row(0xe2)]),
    );
}

#[test]
fn prepared_subscription_multi_segment_forward_include_keeps_root_delta() {
    let schema = multi_segment_required_include_rls_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let reader = user(0xa1);
    seed_multi_segment_include_fixture(&mut core, reader);
    let shape = required_include_shape(&core, Include::new("project.org"));
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut peer = PeerState::for_author(reader);
    peer.rehydrate_query(&mut core, &shape, &binding).unwrap();

    let update_tx = core
        .commit_mergeable(
            MergeableCommit::new("roots", row(0xd2), 40)
                .parents(vec![TxId::new(TxTime(10), node(9))])
                .cells(BTreeMap::from([
                    ("title".to_owned(), v("updated visible root")),
                    ("project".to_owned(), Value::Uuid(row(0xc2).0)),
                ])),
        )
        .unwrap();
    core.apply_fate_update(
        update_tx,
        Fate::Accepted,
        Some(core.clock.next_global_seq),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    let update = peer.query_update(&mut core, &shape, &binding).unwrap();
    let SyncMessage::ViewUpdate {
        result_row_adds, ..
    } = update
    else {
        panic!("expected view update");
    };
    assert_eq!(
        result_row_adds.into_iter().collect::<BTreeSet<_>>(),
        BTreeSet::from([("roots".to_owned().into(), row(0xd2), update_tx)])
    );
}

#[test]
fn full_recompute_and_maintained_inner_multi_segment_include_payloads_match() {
    let schema = multi_segment_required_include_rls_schema();
    let (_full_recompute_dir, mut full_recompute_core) =
        open_node_with_schema(node(9), schema.clone());
    let (_maintained_dir, mut maintained_core) = open_node_with_schema(node(9), schema);
    let reader = user(0xa1);
    seed_multi_segment_include_fixture(&mut full_recompute_core, reader);
    seed_multi_segment_include_fixture(&mut maintained_core, reader);
    let shape = required_include_shape(&maintained_core, Include::new("project.org"));
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let mut full_recompute_peer = PeerState::for_author(reader);
    full_recompute_peer.force_full_recompute_path_for_test(true);
    let mut maintained_peer = PeerState::for_author(reader);

    let full_recompute = full_recompute_peer
        .rehydrate_query(&mut full_recompute_core, &shape, &binding)
        .unwrap();
    let maintained = maintained_peer
        .rehydrate_query(&mut maintained_core, &shape, &binding)
        .unwrap();

    assert_eq!(
        canonical_view_update_payload(&maintained),
        canonical_view_update_payload(&full_recompute)
    );
}

#[test]
fn holes_multi_segment_include_keeps_parent_and_withholds_unreadable_second_hop() {
    let schema = multi_segment_required_include_rls_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let reader = user(0xa1);
    seed_multi_segment_include_fixture(&mut core, reader);
    let shape = required_include_shape(
        &core,
        Include::new("project.org").join_mode(JoinMode::Holes),
    );

    let rows = required_include_rows(&mut core, &shape, reader);
    assert_eq!(
        rows.iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd1), row(0xd2), row(0xd3)])
    );

    let binding = shape.bind(BTreeMap::new()).unwrap();
    let update = core
        .view_update_for_query_binding_with_peer_payload_inventory(
            &shape,
            &binding,
            SubscriptionKey {
                shape_id: shape.shape_id(),
                binding_id: binding.binding_id(),
            },
            [],
            [],
            [],
            reader,
        )
        .unwrap();
    assert_view_update_only_references_rows(
        &update,
        BTreeSet::from([
            row(0xd1),
            row(0xd2),
            row(0xd3),
            row(0xc1),
            row(0xc2),
            row(0xc3),
            row(0xe2),
        ]),
    );
}

#[test]
fn maintained_subscription_view_multi_segment_holes_include_matches_full_recompute() {
    let schema = multi_segment_required_include_rls_schema();
    let (_full_recompute_dir, mut full_recompute_core) =
        open_node_with_schema(node(9), schema.clone());
    let (_maintained_dir, mut maintained_core) = open_node_with_schema(node(9), schema);
    let reader = user(0xa1);
    seed_multi_segment_include_fixture(&mut full_recompute_core, reader);
    seed_multi_segment_include_fixture(&mut maintained_core, reader);
    let shape = required_include_shape(
        &maintained_core,
        Include::new("project.org").join_mode(JoinMode::Holes),
    );
    let binding = shape.bind(BTreeMap::new()).unwrap();
    assert!(
        maintained_core.supported_maintained_view(&shape, &binding, reader),
        "multi-segment holes include should be accepted by maintained subscription view support"
    );

    let mut full_recompute_peer = PeerState::for_author(reader);
    full_recompute_peer.force_full_recompute_path_for_test(true);
    let mut maintained_peer = PeerState::for_author(reader);

    let full_recompute = full_recompute_peer
        .rehydrate_query(&mut full_recompute_core, &shape, &binding)
        .unwrap();
    let maintained = maintained_peer
        .rehydrate_query(&mut maintained_core, &shape, &binding)
        .unwrap();
    assert_eq!(
        canonical_view_update_rows(&maintained),
        canonical_view_update_rows(&full_recompute)
    );
    assert_eq!(
        maintained_peer
            .maintained_subscription_view_metrics()
            .full_recomputes_out,
        0
    );
    assert_eq!(
        maintained_peer
            .maintained_subscription_view_metrics()
            .hits_out,
        1
    );

    assert_view_update_only_references_rows(
        &maintained,
        BTreeSet::from([
            row(0xd1),
            row(0xd2),
            row(0xd3),
            row(0xc1),
            row(0xc2),
            row(0xc3),
            row(0xe2),
        ]),
    );
}

#[test]
fn inner_include_missing_target_drops_parent() {
    let schema = required_include_rls_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    seed_missing_required_include_fixture(&mut core);
    let shape = required_include_shape(&core, Include::new("target"));

    let rows = required_include_rows(&mut core, &shape, AuthorId::SYSTEM);
    assert_eq!(
        rows.into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd2)])
    );
}

#[test]
fn inner_include_null_target_drops_parent() {
    let schema = required_include_rls_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    seed_null_required_include_fixture(&mut core);
    let shape = required_include_shape(&core, Include::new("target"));

    let rows = required_include_rows(&mut core, &shape, AuthorId::SYSTEM);
    assert_eq!(
        rows.into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd2)])
    );
}

#[test]
fn holes_include_missing_target_keeps_parent() {
    let schema = required_include_rls_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    seed_missing_required_include_fixture(&mut core);
    let shape = required_include_shape(&core, Include::new("target").join_mode(JoinMode::Holes));

    let rows = required_include_rows(&mut core, &shape, AuthorId::SYSTEM);
    assert_eq!(
        rows.into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd1), row(0xd2)])
    );
}

#[test]
fn holes_include_uses_shared_plan_path_without_root_membership_lowering() {
    let schema = required_include_rls_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    seed_missing_required_include_fixture(&mut core);
    let shape = required_include_shape(&core, Include::new("target").join_mode(JoinMode::Holes));

    core.clear_prepared_query_plan_cache_for_test();
    let rows = required_include_rows(&mut core, &shape, AuthorId::SYSTEM);
    assert_eq!(
        rows.into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd1), row(0xd2)])
    );
    assert!(
        !core.prepared_query_plan_cache_is_empty_for_test(),
        "hole-only includes should stay on the shared plan path because they do not filter root membership"
    );
}

#[test]
fn holes_include_unreadable_target_keeps_parent_and_withholds_target() {
    let schema = required_include_rls_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let reader = user(0xa1);
    seed_required_include_fixture(&mut core, reader);
    let shape = required_include_shape(&core, Include::new("target").join_mode(JoinMode::Holes));

    let rows = required_include_rows(&mut core, &shape, reader);
    assert_eq!(
        rows.iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd1), row(0xd2)])
    );

    let binding = shape.bind(BTreeMap::new()).unwrap();
    let update = core
        .view_update_for_query_binding_with_peer_payload_inventory(
            &shape,
            &binding,
            SubscriptionKey {
                shape_id: shape.shape_id(),
                binding_id: binding.binding_id(),
            },
            [],
            [],
            [],
            reader,
        )
        .unwrap();
    assert_view_update_only_references_rows(
        &update,
        BTreeSet::from([row(0xd1), row(0xd2), row(0xc2)]),
    );
}

#[test]
fn system_identity_required_include_uses_existence_only_resolvability() {
    let schema = required_include_rls_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    seed_required_include_fixture(&mut core, user(0xa1));
    let shape = required_include_shape(&core, Include::new("target").require_includes());

    let rows = required_include_rows(&mut core, &shape, AuthorId::SYSTEM);
    assert_eq!(
        rows.into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd1), row(0xd2)])
    );
}

#[test]
fn maintained_view_graph_streams_match_policy_result_and_filter_bundle_members() {
    let schema = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::owner_only("todos", "owner"))]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let author_a = user(0xa1);
    let author_b = user(0xb2);

    let sibling_tx = core
        .commit_mergeable_many(vec![
            MergeableCommit::new("todos", row(0x90), 10).cells(owner_cells(author_a, "include")),
            MergeableCommit::new("todos", row(0x91), 10).cells(owner_cells(author_b, "include")),
            MergeableCommit::new("todos", row(0x92), 10).cells(owner_cells(author_a, "skip")),
        ])
        .unwrap();
    core.apply_fate_update(
        sibling_tx,
        Fate::Accepted,
        Some(core.clock.next_global_seq),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    let deleted_readable_content = accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0x93), 20).cells(owner_cells(author_a, "delete me")),
    );
    let deleted_readable = accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0x93), 21)
            .parents(vec![deleted_readable_content])
            .deletion(DeletionEvent::Deleted),
    );
    let deleted_unreadable_content = accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0x94), 22).cells(owner_cells(author_b, "hidden delete")),
    );
    let deleted_unreadable = accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0x94), 23)
            .parents(vec![deleted_unreadable_content])
            .deletion(DeletionEvent::Deleted),
    );

    let shape = Query::from("todos")
        .filter(eq(col("title"), lit("include")))
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let result_current = core
        .maintained_view_result_current(&shape, &binding, author_a)
        .unwrap();
    let result_current_values = result_current.to_values().unwrap();
    assert_eq!(
        maintained_view_result_keys(&result_current),
        policy_result_keys(&mut core, &shape, &binding, author_a)
    );
    assert_eq!(
        maintained_view_result_keys(&result_current),
        BTreeSet::from([(
            row(0x90),
            sibling_tx.time.0,
            core.node_aliases[&sibling_tx.node].0
        )])
    );
    assert!(result_current_values.iter().all(|(values, weight)| {
        *weight == 1
            && matches!(
                values[result_current
                    .descriptor
                    .field_index("schema_version")
                    .unwrap()],
                Value::U64(_)
            )
            && matches!(
                values[result_current.descriptor.field_index("parents").unwrap()],
                Value::Array(_)
            )
    }));

    let policy_versions = core
        .maintained_view_policy_readable_versions(&shape, &binding, author_a)
        .unwrap();
    let version_keys = maintained_view_version_keys(&policy_versions, "content");
    assert!(version_keys.contains(&(
        row(0x90),
        sibling_tx.time.0,
        core.node_aliases[&sibling_tx.node].0
    )));
    assert!(!version_keys.contains(&(
        row(0x91),
        sibling_tx.time.0,
        core.node_aliases[&sibling_tx.node].0
    )));
    let deletion_keys = maintained_view_version_keys(&policy_versions, "deletion");
    assert!(deletion_keys.contains(&(
        row(0x93),
        deleted_readable.time.0,
        core.node_aliases[&deleted_readable.node].0
    )));
    assert!(!deletion_keys.contains(&(
        row(0x94),
        deleted_unreadable.time.0,
        core.node_aliases[&deleted_unreadable.node].0
    )));
}

#[test]
fn maintained_view_tagged_terminal_matches_one_shot_streams_and_reconstructs_versions() {
    let schema = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::owner_only("todos", "owner"))]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let author_a = user(0xa1);
    let author_b = user(0xb2);

    let sibling_tx = core
        .commit_mergeable_many(vec![
            MergeableCommit::new("todos", row(0x90), 10).cells(owner_cells(author_a, "include")),
            MergeableCommit::new("todos", row(0x91), 10).cells(owner_cells(author_b, "include")),
            MergeableCommit::new("todos", row(0x92), 10).cells(owner_cells(author_a, "skip")),
        ])
        .unwrap();
    core.apply_fate_update(
        sibling_tx,
        Fate::Accepted,
        Some(core.clock.next_global_seq),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    let deleted_readable_content = accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0x93), 20).cells(owner_cells(author_a, "delete me")),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0x93), 21)
            .parents(vec![deleted_readable_content])
            .deletion(DeletionEvent::Deleted),
    );
    let deleted_unreadable_content = accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0x94), 22).cells(owner_cells(author_b, "hidden delete")),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0x94), 23)
            .parents(vec![deleted_unreadable_content])
            .deletion(DeletionEvent::Deleted),
    );

    let shape = Query::from("todos")
        .filter(eq(col("title"), lit("include")))
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    assert_maintained_view_tagged_terminal_matches_one_shot_streams(
        &mut core,
        &shape,
        &binding,
        AuthorId::SYSTEM,
    );
    assert_maintained_view_tagged_terminal_matches_one_shot_streams(
        &mut core, &shape, &binding, author_a,
    );
}

#[test]
fn maintained_view_cold_snapshot_seeds_maintained_indexes_equal_one_shot() {
    let schema = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::owner_only("todos", "owner"))]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let author_a = user(0xa1);
    let author_b = user(0xb2);

    let sibling_tx = core
        .commit_mergeable_many(vec![
            MergeableCommit::new("todos", row(0x90), 10).cells(owner_cells(author_a, "include")),
            MergeableCommit::new("todos", row(0x91), 10).cells(owner_cells(author_b, "include")),
            MergeableCommit::new("todos", row(0x92), 10).cells(owner_cells(author_a, "skip")),
        ])
        .unwrap();
    core.apply_fate_update(
        sibling_tx,
        Fate::Accepted,
        Some(core.clock.next_global_seq),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    let deleted_readable_content = accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0x93), 20).cells(owner_cells(author_a, "delete me")),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0x93), 21)
            .parents(vec![deleted_readable_content])
            .deletion(DeletionEvent::Deleted),
    );
    let deleted_unreadable_content = accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0x94), 22).cells(owner_cells(author_b, "hidden delete")),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0x94), 23)
            .parents(vec![deleted_unreadable_content])
            .deletion(DeletionEvent::Deleted),
    );

    let shape = Query::from("todos")
        .filter(eq(col("title"), lit("include")))
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    assert_maintained_view_cold_snapshot_seed_matches_one_shot(
        &mut core,
        &shape,
        &binding,
        AuthorId::SYSTEM,
    );
    assert_maintained_view_cold_snapshot_seed_matches_one_shot(
        &mut core, &shape, &binding, author_a,
    );
}

#[test]
fn maintained_view_system_identity_bypasses_root_read_policy() {
    let schema = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::owner_only("todos", "owner"))]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let author_a = user(0xa1);
    let author_b = user(0xb2);
    let tx_a = accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0xa0), 10).cells(owner_cells(author_a, "a")),
    );
    let tx_b = accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0xa1), 11).cells(owner_cells(author_b, "b")),
    );
    let deleted_content = accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0xa2), 12).cells(owner_cells(author_b, "deleted")),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0xa2), 13)
            .parents(vec![deleted_content])
            .deletion(DeletionEvent::Deleted),
    );

    let shape = Query::from("todos")
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let result_current = core
        .maintained_view_result_current(&shape, &binding, AuthorId::SYSTEM)
        .unwrap();
    assert_eq!(
        maintained_view_result_keys(&result_current),
        BTreeSet::from([
            (row(0xa0), tx_a.time.0, core.node_aliases[&tx_a.node].0),
            (row(0xa1), tx_b.time.0, core.node_aliases[&tx_b.node].0),
        ])
    );
}

#[test]
fn maintained_view_allows_join_policy_slice() {
    let schema = JazzSchema::new([
        TableSchema::new(
            "todos",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("owner", ColumnType::Uuid),
            ],
        )
        .with_read_policy(Policy::shape(Query::from("todos").join_via(
            "members",
            "owner",
            [eq(col("user"), claim("sub"))],
        ))),
        TableSchema::new(
            "members",
            [
                ColumnSchema::new("owner", ColumnType::Uuid),
                ColumnSchema::new("user", ColumnType::Uuid),
            ],
        )
        .with_reference("owner", "todos"),
    ]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let shape = Query::from("todos")
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    assert!(
        core.supported_maintained_view(&shape, &binding, user(0xa1)),
        "non-recursive join policy should be supported"
    );
    core.maintained_view_result_current(&shape, &binding, user(0xa1))
        .unwrap();
}

#[test]
fn maintained_subscription_view_shared_todo_member_include_emits_relation_deltas_without_full_recompute()
 {
    let schema = JazzSchema::new([
        TableSchema::new(
            "sharedTodos",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("owner", ColumnType::Uuid),
            ],
        )
        .with_reference("owner", "members"),
        TableSchema::new(
            "members",
            [
                ColumnSchema::new("name", ColumnType::String),
                ColumnSchema::new("userID", ColumnType::Uuid),
            ],
        )
        .with_read_policy(Policy::shape(
            Query::from("members").filter(eq(col("userID"), claim("sub"))),
        )),
    ]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let reader = user(0xa1);
    let other = user(0xb2);
    let member_row = row(0x71);
    let todo_row = row(0x72);

    let hidden_member_tx = accept_global(
        &mut core,
        MergeableCommit::new("members", member_row, 10).cells(BTreeMap::from([
            ("name".to_owned(), Value::String("hidden owner".to_owned())),
            ("userID".to_owned(), Value::Uuid(other.0)),
        ])),
    );
    let todo_tx = accept_global(
        &mut core,
        MergeableCommit::new("sharedTodos", todo_row, 11).cells(BTreeMap::from([
            ("title".to_owned(), Value::String("shared slice".to_owned())),
            ("owner".to_owned(), Value::Uuid(member_row.0)),
        ])),
    );

    let shape = Query::from("sharedTodos")
        .include_with(Include::new("owner").require_includes())
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    assert!(
        core.supported_maintained_view(&shape, &binding, reader),
        "shared-todo owner/member include should be supported by maintained subscription views"
    );

    let mut peer = PeerState::for_author(reader);
    let initial = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    assert_eq!(
        canonical_view_update_rows(&initial),
        (Vec::new(), Vec::new())
    );
    assert_eq!(
        peer.maintained_subscription_view_metrics()
            .full_recomputes_out,
        0
    );
    assert_eq!(peer.maintained_subscription_view_metrics().hits_out, 1);

    let visible_member_tx = accept_global(
        &mut core,
        MergeableCommit::new("members", member_row, 12)
            .parents(vec![hidden_member_tx])
            .cells(BTreeMap::from([
                ("name".to_owned(), Value::String("visible owner".to_owned())),
                ("userID".to_owned(), Value::Uuid(reader.0)),
            ])),
    );
    let grant = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_eq!(
        canonical_view_update_rows(&grant),
        (
            vec![
                ("members".to_owned().into(), member_row, visible_member_tx),
                ("sharedTodos".to_owned().into(), todo_row, todo_tx),
            ],
            Vec::new(),
        )
    );
    assert_view_update_only_references_rows(&grant, BTreeSet::from([member_row, todo_row]));
    assert_eq!(
        peer.maintained_subscription_view_metrics()
            .full_recomputes_out,
        0
    );
    assert_eq!(peer.maintained_subscription_view_metrics().hits_out, 2);

    let hidden_again_tx = accept_global(
        &mut core,
        MergeableCommit::new("members", member_row, 13)
            .parents(vec![visible_member_tx])
            .cells(BTreeMap::from([
                ("name".to_owned(), Value::String("hidden again".to_owned())),
                ("userID".to_owned(), Value::Uuid(other.0)),
            ])),
    );
    let revoke = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_eq!(
        canonical_view_update_rows(&revoke),
        (
            Vec::new(),
            vec![
                ("members".to_owned().into(), member_row, visible_member_tx),
                ("sharedTodos".to_owned().into(), todo_row, todo_tx),
            ],
        )
    );
    assert_retraction_without_replacement_leak(
        &revoke,
        member_row,
        visible_member_tx,
        hidden_again_tx,
    );
    assert_eq!(
        peer.maintained_subscription_view_metrics()
            .full_recomputes_out,
        0
    );
    assert_eq!(peer.maintained_subscription_view_metrics().hits_out, 3);
}

#[test]
fn maintained_subscription_view_ordered_offset_limit_boundary_churn_stays_incremental() {
    let (_core_dir, mut core) = open_node_with_schema(node(9), priority_schema());
    let first = row(0x11);
    let second = row(0x22);
    let third = row(0x33);
    let fourth = row(0x44);
    let first_tx = accept_global(
        &mut core,
        MergeableCommit::new("todos", first, 10).cells(priority_cells("first", 10)),
    );
    let second_tx = accept_global(
        &mut core,
        MergeableCommit::new("todos", second, 11).cells(priority_cells("second", 20)),
    );
    let third_tx = accept_global(
        &mut core,
        MergeableCommit::new("todos", third, 12).cells(priority_cells("third", 30)),
    );
    let fourth_tx = accept_global(
        &mut core,
        MergeableCommit::new("todos", fourth, 13).cells(priority_cells("fourth", 40)),
    );
    let shape = Query::from("todos")
        .order_by("priority", OrderDirection::Asc)
        .offset(1)
        .limit(2)
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    assert!(
        core.supported_maintained_view(&shape, &binding, AuthorId::SYSTEM),
        "ordered offset/limit windows should be maintained, not silently downgraded"
    );

    let mut peer = PeerState::new();
    let initial = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    assert_view_update_rows(
        initial,
        [("todos", second, second_tx), ("todos", third, third_tx)],
        [],
    );
    assert_eq!(
        peer.maintained_subscription_view_metrics()
            .full_recomputes_out,
        0
    );

    let zeroth = row(0x05);
    let zeroth_tx = accept_global(
        &mut core,
        MergeableCommit::new("todos", zeroth, 14).cells(priority_cells("zeroth", 5)),
    );
    let shifted_down = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_view_update_rows(
        shifted_down,
        [("todos", first, first_tx)],
        [("todos", third, third_tx)],
    );

    accept_global(
        &mut core,
        MergeableCommit::new("todos", zeroth, 15)
            .parents(vec![zeroth_tx])
            .deletion(DeletionEvent::Deleted),
    );
    let shifted_back = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_view_update_rows(
        shifted_back,
        [("todos", third, third_tx)],
        [("todos", first, first_tx)],
    );

    accept_global(
        &mut core,
        MergeableCommit::new("todos", second, 16)
            .parents(vec![second_tx])
            .deletion(DeletionEvent::Deleted),
    );
    let fill_from_tail = peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_view_update_rows(
        fill_from_tail,
        [("todos", fourth, fourth_tx)],
        [("todos", second, second_tx)],
    );

    let metrics = peer.maintained_subscription_view_metrics();
    assert_eq!(metrics.full_recomputes_out, 0);
    assert_eq!(metrics.unsupported_skips_out, 0);
    assert_eq!(metrics.hits_out, 4);
}

#[test]
fn supported_maintained_view_allows_reference_bearing_root_table() {
    // The maintained subscription view footprint is table-aware and now ships
    // reference-closure rows from the fast path.
    let ref_schema = JazzSchema::new([
        TableSchema::new(
            "todos",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("author", ColumnType::Uuid),
            ],
        )
        .with_reference("author", "authors"),
        TableSchema::new("authors", [ColumnSchema::new("name", ColumnType::String)]),
    ]);
    let (_ref_dir, ref_core) = open_node_with_schema(node(9), ref_schema);
    let shape = Query::from("todos")
        .validate(&ref_core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    assert!(
        ref_core.supported_maintained_view(&shape, &binding, user(0xa1)),
        "reference-bearing root table must be reported as supported"
    );

    // Control: the same query on a table with no references is supported.
    let plain_schema = JazzSchema::new([TableSchema::new(
        "todos",
        [ColumnSchema::new("title", ColumnType::String)],
    )]);
    let (_plain_dir, plain_core) = open_node_with_schema(node(9), plain_schema);
    let plain_shape = Query::from("todos")
        .validate(&plain_core.catalogue.schema)
        .unwrap();
    let plain_binding = plain_shape.bind(BTreeMap::new()).unwrap();
    assert!(
        plain_core.supported_maintained_view(&plain_shape, &plain_binding, user(0xa1)),
        "no-reference root table must be supported"
    );
}

#[test]
fn reachable_closure_helper_yields_seed_reachable_team_set() {
    let (_core_dir, mut core) = open_node_with_schema(node(9), recursive_reachable_schema());
    let shape = recursive_reachable_shape(&core);
    let reachable = &shape.query().reachable[0];
    let param_types = BTreeMap::from([("team".to_owned(), ColumnType::Uuid)]);
    let graphs = core
        .lower_reachable_graph_parts(
            &shape,
            &param_types,
            reachable,
            &core.table("teamAccess").unwrap().clone(),
            &core.table("teamEdges").unwrap().clone(),
            DurabilityTier::Global,
            &BTreeMap::new(),
        )
        .unwrap();
    let subscription = subscribe_reachable_test_graph(
        &mut core,
        &shape,
        graphs.closure.project(["team", "reachable_team"]),
        ["team"],
        team(1),
    );
    seed_recursive_reachable_fixture(&mut core);
    let rows = drain_reachable_test_rows(&subscription);
    assert_eq!(uuid_field_set(&rows, "reachable_team"), team_set([1, 2, 3]));
}

#[test]
fn reachable_edge_constituent_current_graph_yields_closure_edges_with_versions() {
    let (_core_dir, mut core) = open_node_with_schema(node(9), recursive_reachable_schema());
    let shape = recursive_reachable_shape(&core);
    let graph = core
        .reachable_edge_constituent_current_graph(&shape, &shape.query().reachable[0])
        .unwrap();
    let graph = reachable_constituent_test_graph_with_team(
        &core,
        &shape,
        graph,
        &shape.query().reachable[0],
        "teamEdges",
        "member",
    );
    let subscription = subscribe_reachable_test_graph(&mut core, &shape, graph, ["team"], team(1));
    let fixture = seed_recursive_reachable_fixture(&mut core);
    let initial = drain_reachable_test_rows(&subscription);
    accept_global(
        &mut core,
        edge_commit(0xe2, 2, 3, 30).parents(vec![fixture.edge_2_to_3]),
    );
    let rows = initial.apply(drain_reachable_test_rows(&subscription));
    assert_eq!(uuid_field_set(&rows, "row_uuid"), row_set([0xe1, 0xe2]));
    assert_version_bearing(&rows);
}

#[test]
fn reachable_access_constituent_current_graph_yields_closure_access_rows_with_versions() {
    let (_core_dir, mut core) = open_node_with_schema(node(9), recursive_reachable_schema());
    let shape = recursive_reachable_shape(&core);
    let graph = core
        .reachable_access_constituent_current_graph(&shape, &shape.query().reachable[0])
        .unwrap();
    let graph = reachable_constituent_test_graph_with_team(
        &core,
        &shape,
        graph,
        &shape.query().reachable[0],
        "teamAccess",
        "team",
    );
    let subscription = subscribe_reachable_test_graph(&mut core, &shape, graph, ["team"], team(1));
    seed_recursive_reachable_fixture(&mut core);
    let rows = drain_reachable_test_rows(&subscription);
    assert_eq!(
        uuid_field_set(&rows, "row_uuid"),
        row_set([0xa1, 0xa2, 0xa3])
    );
    assert_version_bearing(&rows);
}

#[test]
fn reachable_constituents_retract_when_edge_removed_and_closure_shrinks() {
    let (_core_dir, mut core) = open_node_with_schema(node(9), recursive_reachable_schema());
    let shape = recursive_reachable_shape(&core);
    let before_graph = core
        .reachable_access_constituent_current_graph(&shape, &shape.query().reachable[0])
        .unwrap();
    let before_graph = reachable_constituent_test_graph_with_team(
        &core,
        &shape,
        before_graph,
        &shape.query().reachable[0],
        "teamAccess",
        "team",
    );
    let subscription =
        subscribe_reachable_test_graph(&mut core, &shape, before_graph, ["team"], team(1));
    let edge_to_remove = seed_recursive_reachable_fixture(&mut core).edge_2_to_3;
    let before = drain_reachable_test_rows(&subscription);
    assert_eq!(
        uuid_field_set(&before, "row_uuid"),
        row_set([0xa1, 0xa2, 0xa3])
    );

    accept_global(
        &mut core,
        MergeableCommit::new("teamEdges", row(0xe2), 20)
            .parents(vec![edge_to_remove])
            .deletion(DeletionEvent::Deleted),
    );

    let after = before.apply(drain_reachable_test_rows(&subscription));
    assert_eq!(uuid_field_set(&after, "row_uuid"), row_set([0xa1, 0xa2]));
}

fn accept_global(core: &mut NodeState<RocksDbStorage>, commit: MergeableCommit) -> TxId {
    let tx_id = core.commit_mergeable(commit).unwrap();
    core.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(core.clock.next_global_seq),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    tx_id
}

fn priority_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("priority", ColumnType::U64),
        ],
    )])
}

fn priority_cells(title: impl Into<String>, priority: u64) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.into())),
        ("priority".to_owned(), Value::U64(priority)),
    ])
}

fn assert_view_update_rows<const A: usize, const R: usize>(
    update: SyncMessage,
    expected_adds: [(&str, RowUuid, TxId); A],
    expected_removes: [(&str, RowUuid, TxId); R],
) {
    let SyncMessage::ViewUpdate {
        result_row_adds,
        result_row_removes,
        ..
    } = update
    else {
        panic!("expected view update");
    };
    let mut result_row_adds = result_row_adds;
    let mut result_row_removes = result_row_removes;
    result_row_adds.sort();
    result_row_removes.sort();
    let mut expected_adds = expected_adds
        .into_iter()
        .map(|(table, row_uuid, tx_id)| (table.to_owned().into(), row_uuid, tx_id))
        .collect::<Vec<_>>();
    let mut expected_removes = expected_removes
        .into_iter()
        .map(|(table, row_uuid, tx_id)| (table.to_owned().into(), row_uuid, tx_id))
        .collect::<Vec<_>>();
    expected_adds.sort();
    expected_removes.sort();
    assert_eq!(result_row_adds, expected_adds);
    assert_eq!(result_row_removes, expected_removes);
}

struct RecursiveReachableFixture {
    edge_2_to_3: TxId,
}

fn recursive_reachable_schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new("docs", [ColumnSchema::new("title", ColumnType::String)]),
        TableSchema::new("teams", [ColumnSchema::new("name", ColumnType::String)]),
        TableSchema::new(
            "teamEdges",
            [
                ColumnSchema::new("member", ColumnType::Uuid),
                ColumnSchema::new("parent", ColumnType::Uuid),
            ],
        )
        .with_reference("member", "teams")
        .with_reference("parent", "teams"),
        TableSchema::new(
            "teamAccess",
            [
                ColumnSchema::new("doc", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
            ],
        )
        .with_reference("doc", "docs")
        .with_reference("team", "teams"),
    ])
}

fn recursive_reachable_shape(core: &NodeState<RocksDbStorage>) -> ValidatedQuery {
    Query::from("docs")
        .reachable_via(
            "teamAccess",
            "doc",
            "team",
            param("team"),
            "teamEdges",
            "member",
            "parent",
            [],
        )
        .validate(&core.catalogue.schema)
        .unwrap()
}

fn seed_recursive_reachable_fixture(
    core: &mut NodeState<RocksDbStorage>,
) -> RecursiveReachableFixture {
    for id in 1..=5 {
        accept_global(
            core,
            MergeableCommit::new("teams", row(id), id as u64).cells(BTreeMap::from([(
                "name".to_owned(),
                v(format!("team {id}")),
            )])),
        );
    }
    for (row_id, title) in [
        (0xd1, "one"),
        (0xd2, "two"),
        (0xd3, "three"),
        (0xd4, "four"),
    ] {
        accept_global(
            core,
            MergeableCommit::new("docs", row(row_id), row_id as u64)
                .cells(BTreeMap::from([("title".to_owned(), v(title))])),
        );
    }
    accept_global(core, edge_commit(0xe1, 1, 2, 10));
    let edge_2_to_3 = accept_global(core, edge_commit(0xe2, 2, 3, 11));
    accept_global(core, edge_commit(0xe3, 4, 5, 12));
    for (row_id, doc_id, team_id) in [
        (0xa1, 0xd1, 1),
        (0xa2, 0xd2, 2),
        (0xa3, 0xd3, 3),
        (0xa4, 0xd4, 5),
    ] {
        accept_global(
            core,
            MergeableCommit::new("teamAccess", row(row_id), row_id as u64).cells(BTreeMap::from([
                ("doc".to_owned(), Value::Uuid(row(doc_id).0)),
                ("team".to_owned(), Value::Uuid(team(team_id))),
            ])),
        );
    }
    RecursiveReachableFixture { edge_2_to_3 }
}

fn edge_commit(row_id: u8, member: u8, parent: u8, time: u64) -> MergeableCommit {
    MergeableCommit::new("teamEdges", row(row_id), time).cells(BTreeMap::from([
        ("member".to_owned(), Value::Uuid(team(member))),
        ("parent".to_owned(), Value::Uuid(team(parent))),
    ]))
}

fn team(id: u8) -> uuid::Uuid {
    uuid::Uuid::from_bytes([id; 16])
}

fn reachable_constituent_test_graph_with_team(
    core: &NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    graph: GraphBuilder,
    reachable: &crate::query::ReachableVia,
    table_name: &str,
    team_column: &str,
) -> GraphBuilder {
    let param_types = BTreeMap::from([("team".to_owned(), ColumnType::Uuid)]);
    let closure = core
        .lower_reachable_graph_parts(
            shape,
            &param_types,
            reachable,
            &core.table("teamAccess").unwrap().clone(),
            &core.table("teamEdges").unwrap().clone(),
            DurabilityTier::Global,
            &BTreeMap::new(),
        )
        .unwrap()
        .closure
        .project(["team", "reachable_team"]);
    let table = core.table(table_name).unwrap();
    GraphBuilder::join(
        graph.unwrap_nullable(format!("user_{team_column}")),
        closure,
        [format!("user_{team_column}")],
        ["reachable_team".to_owned()],
    )
    .project_fields(
        test_maintained_view_version_fields(table)
            .into_iter()
            .map(|field| ProjectField::renamed(format!("left.{field}"), field))
            .chain([ProjectField::renamed("right.team", "team")]),
    )
}

fn test_maintained_view_version_fields(table: &TableSchema) -> Vec<String> {
    let mut fields = vec!["row_uuid".to_owned()];
    fields.extend(
        table
            .columns
            .iter()
            .map(|column| format!("user_{}", column.name)),
    );
    fields.extend([
        "tx_time".to_owned(),
        "tx_node_id".to_owned(),
        "schema_version".to_owned(),
        "parents".to_owned(),
    ]);
    fields
}

fn subscribe_reachable_test_graph(
    core: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    graph: GraphBuilder,
    output_key_fields: impl IntoIterator<Item = impl Into<String>>,
    seed_team: uuid::Uuid,
) -> groove::ivm::Subscription {
    let binding_descriptor = groove::records::RecordDescriptor::new([(
        "team".to_owned(),
        groove::records::ValueType::Uuid,
    )]);
    let prepared = core
        .database
        .prepare(
            graph,
            format!("jazz-query:{}", shape.shape_id().0),
            binding_descriptor,
            output_key_fields,
        )
        .unwrap();
    core.database
        .bind_shape(prepared.id(), &[Value::Uuid(seed_team)])
        .unwrap()
}

fn drain_reachable_test_rows(subscription: &groove::ivm::Subscription) -> ReachableTestRows {
    let mut descriptor = None;
    let mut values = Vec::new();
    let mut empty_polls_after_values = 0;
    for _ in 0..100 {
        match subscription.try_recv() {
            Ok(deltas) => {
                if descriptor.is_none() {
                    descriptor = Some(deltas.descriptor);
                }
                values.extend(deltas.to_values().unwrap());
                empty_polls_after_values = 0;
            }
            Err(std::sync::mpsc::TryRecvError::Empty) => {
                if descriptor.is_some() {
                    empty_polls_after_values += 1;
                    if empty_polls_after_values >= 50 {
                        break;
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
            Err(std::sync::mpsc::TryRecvError::Disconnected) => {
                panic!("reachable test subscription disconnected");
            }
        }
    }
    ReachableTestRows {
        descriptor: descriptor.expect("reachable test graph produced no descriptor"),
        values,
    }
}

struct ReachableTestRows {
    descriptor: groove::records::RecordDescriptor,
    values: Vec<(Vec<Value>, i64)>,
}

impl ReachableTestRows {
    fn apply(mut self, deltas: ReachableTestRows) -> Self {
        self.values.extend(deltas.values);
        self
    }
}

fn uuid_field_set(rows: &ReachableTestRows, field: &str) -> BTreeSet<uuid::Uuid> {
    let idx = rows.descriptor.field_index(field).unwrap();
    let mut weights = BTreeMap::<uuid::Uuid, i64>::new();
    for (values, weight) in &rows.values {
        let Value::Uuid(uuid) = &values[idx] else {
            panic!("{field} must be uuid");
        };
        *weights.entry(*uuid).or_default() += *weight;
    }
    weights
        .into_iter()
        .filter_map(|(uuid, weight)| (weight > 0).then_some(uuid))
        .collect()
}

fn row_set<const N: usize>(ids: [u8; N]) -> BTreeSet<uuid::Uuid> {
    ids.into_iter().map(|id| row(id).0).collect()
}

fn team_set<const N: usize>(ids: [u8; N]) -> BTreeSet<uuid::Uuid> {
    ids.into_iter().map(team).collect()
}

fn assert_version_bearing(rows: &ReachableTestRows) {
    for field in ["tx_time", "tx_node_id", "schema_version", "parents"] {
        assert!(
            rows.descriptor.field_index(field).is_some(),
            "missing version field {field}"
        );
    }
}

fn policy_result_keys(
    core: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    binding: &Binding,
    identity: AuthorId,
) -> BTreeSet<(RowUuid, u64, u64)> {
    core.query_rows_for_link(shape, binding, DurabilityTier::Global, identity)
        .unwrap()
        .into_iter()
        .map(|row| {
            let (tx_time, tx_node_alias) = row.projected_tx_alias().unwrap();
            (row.row_uuid(), tx_time.0, tx_node_alias.0)
        })
        .collect()
}

fn maintained_view_result_keys(rows: &groove::ivm::RecordDeltas) -> BTreeSet<(RowUuid, u64, u64)> {
    let row_idx = rows.descriptor.field_index("row_uuid").unwrap();
    let time_idx = rows.descriptor.field_index("content_tx_time").unwrap();
    let node_idx = rows.descriptor.field_index("content_tx_node_id").unwrap();
    rows.to_values()
        .unwrap()
        .into_iter()
        .filter(|(_, weight)| *weight > 0)
        .map(|(values, _)| {
            let Value::Uuid(row_uuid) = values[row_idx] else {
                panic!("row_uuid must be uuid");
            };
            let Value::U64(tx_time) = values[time_idx] else {
                panic!("content_tx_time must be u64");
            };
            let Value::U64(tx_node_id) = values[node_idx] else {
                panic!("content_tx_node_id must be u64");
            };
            (RowUuid(row_uuid), tx_time, tx_node_id)
        })
        .collect()
}

fn maintained_view_version_keys(
    rows: &groove::ivm::RecordDeltas,
    event_kind: &str,
) -> BTreeSet<(RowUuid, u64, u64)> {
    let kind_idx = rows.descriptor.field_index("event_kind").unwrap();
    let row_idx = rows.descriptor.field_index("version_row_uuid").unwrap();
    let time_idx = rows.descriptor.field_index("version_tx_time").unwrap();
    let node_idx = rows.descriptor.field_index("version_tx_node_id").unwrap();
    rows.to_values()
        .unwrap()
        .into_iter()
        .filter(|(values, weight)| {
            *weight > 0 && values[kind_idx] == Value::String(event_kind.to_owned())
        })
        .map(|(values, _)| {
            let Value::Uuid(row_uuid) = values[row_idx] else {
                panic!("version_row_uuid must be uuid");
            };
            let Value::U64(tx_time) = values[time_idx] else {
                panic!("version_tx_time must be u64");
            };
            let Value::U64(tx_node_id) = values[node_idx] else {
                panic!("version_tx_node_id must be u64");
            };
            (RowUuid(row_uuid), tx_time, tx_node_id)
        })
        .collect()
}

fn assert_maintained_view_tagged_terminal_matches_one_shot_streams(
    core: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    binding: &Binding,
    identity: AuthorId,
) {
    let table = core.table("todos").unwrap().clone();
    let tagged = core
        .maintained_view_tagged_terminal(shape, binding, identity)
        .unwrap();
    assert_eq!(
        tagged_event_kinds(&tagged),
        BTreeSet::from([
            "replacement_content".to_owned(),
            "replacement_deletion".to_owned(),
            "result_current".to_owned(),
            "version_content".to_owned(),
            "version_deletion".to_owned(),
        ])
    );

    let result_current = core
        .maintained_view_result_current(shape, binding, identity)
        .unwrap();
    assert_eq!(
        tagged_result_current_rows(&tagged, &table),
        result_current_rows(
            &result_current,
            &table,
            "version_tx_time",
            "version_tx_node_id"
        )
    );

    let expected_versions = core
        .maintained_view_policy_readable_version_rows_by_tx(shape, identity)
        .unwrap();
    assert_eq!(
        tagged_versions_by_tx(
            core,
            &tagged,
            &table,
            ["version_content", "version_deletion"]
        ),
        version_rows_by_tx_key(expected_versions)
    );

    let expected_replacements = core
        .maintained_view_replacement_for_remove_by_row(shape, identity)
        .unwrap();
    assert_eq!(
        tagged_replacements_by_row(&tagged, &table),
        replacement_for_remove_key(expected_replacements)
    );
}

fn assert_maintained_view_cold_snapshot_seed_matches_one_shot(
    core: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    binding: &Binding,
    identity: AuthorId,
) {
    let maintained = core
        .maintained_view_seed_from_cold_snapshot(shape, binding, identity)
        .unwrap();

    let expected_result_current = core
        .maintained_view_result_current(shape, binding, identity)
        .unwrap();
    assert_eq!(
        maintained.active_result_entries(),
        result_current_entries(core, &expected_result_current)
    );

    let expected_versions = core
        .maintained_view_policy_readable_version_rows_by_tx(shape, identity)
        .unwrap();
    let txs = expected_versions.keys().copied().collect::<Vec<_>>();
    assert_eq!(
        maintained_versions_by_tx_key(&maintained, txs),
        version_rows_by_tx_key(expected_versions)
    );

    let expected_replacements = core
        .maintained_view_replacement_for_remove_by_row(shape, identity)
        .unwrap();
    let rows = expected_replacements.keys().copied().collect::<Vec<_>>();
    assert_eq!(
        maintained_replacements_by_row_key(&maintained, rows),
        replacement_for_remove_key(expected_replacements)
    );
}

fn result_current_entries(
    core: &NodeState<RocksDbStorage>,
    rows: &groove::ivm::RecordDeltas,
) -> BTreeSet<ResultRowEntry> {
    let row_idx = rows.descriptor.field_index("row_uuid").unwrap();
    let time_idx = rows.descriptor.field_index("content_tx_time").unwrap();
    let node_idx = rows.descriptor.field_index("content_tx_node_id").unwrap();
    rows.to_values()
        .unwrap()
        .into_iter()
        .filter(|(_, weight)| *weight > 0)
        .map(|(values, _)| {
            let Value::Uuid(row_uuid) = values[row_idx] else {
                panic!("row_uuid must be uuid");
            };
            let Value::U64(tx_time) = values[time_idx] else {
                panic!("content_tx_time must be u64");
            };
            let Value::U64(tx_node_id) = values[node_idx] else {
                panic!("content_tx_node_id must be u64");
            };
            let tx_node = core
                .node_aliases
                .iter()
                .find_map(|(node, alias)| (alias.0 == tx_node_id).then_some(*node))
                .unwrap();
            (
                groove::Intern::new(core.table("todos").unwrap().name.clone()),
                RowUuid(row_uuid),
                TxId::new(TxTime(tx_time), tx_node),
            )
        })
        .collect()
}

fn maintained_versions_by_tx_key(
    maintained: &crate::node::maintained_subscription_view::MaintainedSubscriptionView,
    txs: impl IntoIterator<Item = TxId>,
) -> BTreeMap<TxId, Vec<VersionRowKey>> {
    txs.into_iter()
        .map(|tx_id| {
            let mut versions = maintained
                .versions_by_tx(tx_id)
                .into_iter()
                .map(version_row_key)
                .collect::<Vec<_>>();
            versions.sort();
            (tx_id, versions)
        })
        .collect()
}

fn maintained_replacements_by_row_key(
    maintained: &crate::node::maintained_subscription_view::MaintainedSubscriptionView,
    rows: impl IntoIterator<Item = RowUuid>,
) -> BTreeMap<RowUuid, ReplacementKey> {
    rows.into_iter()
        .map(|row_uuid| {
            let (content_winner, deletion_winner) = maintained.replacement_for("todos", row_uuid);
            (
                row_uuid,
                ReplacementKey {
                    content_winner: content_winner.map(version_row_key),
                    deletion_winner: deletion_winner.map(version_row_key),
                },
            )
        })
        .collect()
}

fn tagged_event_kinds(rows: &groove::ivm::RecordDeltas) -> BTreeSet<String> {
    let kind_idx = rows.descriptor.field_index("event_kind").unwrap();
    rows.to_values()
        .unwrap()
        .into_iter()
        .filter(|(_, weight)| *weight > 0)
        .map(|(values, _)| {
            let Value::String(kind) = &values[kind_idx] else {
                panic!("event_kind must be string");
            };
            kind.clone()
        })
        .collect()
}

fn tagged_result_current_rows(
    rows: &groove::ivm::RecordDeltas,
    table: &TableSchema,
) -> BTreeSet<Vec<String>> {
    result_rows_matching(rows, table, "result_current", "tx_time", "tx_node_id")
}

fn result_current_rows(
    rows: &groove::ivm::RecordDeltas,
    table: &TableSchema,
    version_time_field: &str,
    version_node_field: &str,
) -> BTreeSet<Vec<String>> {
    result_rows_matching(
        rows,
        table,
        "result_content",
        version_time_field,
        version_node_field,
    )
}

fn result_rows_matching(
    rows: &groove::ivm::RecordDeltas,
    table: &TableSchema,
    event_kind: &str,
    version_time_field: &str,
    version_node_field: &str,
) -> BTreeSet<Vec<String>> {
    let kind_idx = rows.descriptor.field_index("event_kind").unwrap();
    let field_indices = [
        "row_uuid",
        "content_tx_time",
        "content_tx_node_id",
        version_time_field,
        version_node_field,
        "schema_version",
        "parents",
        "_deletion",
    ]
    .into_iter()
    .map(|field| rows.descriptor.field_index(field).unwrap())
    .chain(table.columns.iter().map(|column| {
        regular_or_tagged_field_index(&rows.descriptor, table, &format!("user_{}", column.name))
            .unwrap()
    }))
    .collect::<Vec<_>>();
    rows.to_values()
        .unwrap()
        .into_iter()
        .filter(|(values, weight)| {
            *weight > 0 && values[kind_idx] == Value::String(event_kind.to_owned())
        })
        .map(|(values, _)| {
            field_indices
                .iter()
                .map(|idx| format!("{:?}", values[*idx]))
                .collect()
        })
        .collect()
}

fn tagged_versions_by_tx(
    core: &NodeState<RocksDbStorage>,
    rows: &groove::ivm::RecordDeltas,
    table: &TableSchema,
    event_kinds: impl IntoIterator<Item = &'static str>,
) -> BTreeMap<TxId, Vec<VersionRowKey>> {
    let selected = event_kinds
        .into_iter()
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();
    let kind_idx = rows.descriptor.field_index("event_kind").unwrap();
    let mut by_tx = BTreeMap::<TxId, Vec<VersionRowKey>>::new();
    for (values, weight) in rows.to_values().unwrap() {
        if weight <= 0 {
            continue;
        }
        let Value::String(kind) = &values[kind_idx] else {
            panic!("event_kind must be string");
        };
        if !selected.contains(kind) {
            continue;
        }
        let version = reconstruct_version_row_from_tagged_values(table, &rows.descriptor, &values);
        let tx_id = core.version_tx_id(&version).unwrap();
        by_tx
            .entry(tx_id)
            .or_default()
            .push(version_row_key(version));
    }
    for versions in by_tx.values_mut() {
        versions.sort();
    }
    by_tx
}

fn tagged_replacements_by_row(
    rows: &groove::ivm::RecordDeltas,
    table: &TableSchema,
) -> BTreeMap<RowUuid, ReplacementKey> {
    let kind_idx = rows.descriptor.field_index("event_kind").unwrap();
    let mut replacements = BTreeMap::<RowUuid, ReplacementKey>::new();
    for (values, weight) in rows.to_values().unwrap() {
        if weight <= 0 {
            continue;
        }
        let Value::String(kind) = &values[kind_idx] else {
            panic!("event_kind must be string");
        };
        if kind != "replacement_content" && kind != "replacement_deletion" {
            continue;
        }
        let version = reconstruct_version_row_from_tagged_values(table, &rows.descriptor, &values);
        let row_uuid = version.row_uuid();
        let key = version_row_key(version);
        let replacement = replacements.entry(row_uuid).or_default();
        if kind == "replacement_content" {
            replacement.content_winner = Some(key);
        } else {
            replacement.deletion_winner = Some(key);
        }
    }
    replacements
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
struct VersionRowKey {
    table: String,
    row_uuid: RowUuid,
    layer: VersionLayer,
    raw_record: Vec<u8>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct ReplacementKey {
    content_winner: Option<VersionRowKey>,
    deletion_winner: Option<VersionRowKey>,
}

fn version_rows_by_tx_key(
    versions_by_tx: BTreeMap<TxId, Vec<VersionRow>>,
) -> BTreeMap<TxId, Vec<VersionRowKey>> {
    versions_by_tx
        .into_iter()
        .map(|(tx_id, versions)| {
            let mut versions = versions
                .into_iter()
                .map(version_row_key)
                .collect::<Vec<_>>();
            versions.sort();
            (tx_id, versions)
        })
        .collect()
}

fn replacement_for_remove_key(
    replacements: BTreeMap<RowUuid, MaintainedViewReplacementForRemove>,
) -> BTreeMap<RowUuid, ReplacementKey> {
    replacements
        .into_iter()
        .map(|(row_uuid, replacement)| {
            (
                row_uuid,
                ReplacementKey {
                    content_winner: replacement.content_winner.map(version_row_key),
                    deletion_winner: replacement.deletion_winner.map(version_row_key),
                },
            )
        })
        .collect()
}

fn version_row_key(version: VersionRow) -> VersionRowKey {
    VersionRowKey {
        table: version.table().to_owned(),
        row_uuid: version.row_uuid(),
        layer: version.layer(),
        raw_record: version.record.raw().to_vec(),
    }
}

fn reconstruct_version_row_from_tagged_values(
    table: &TableSchema,
    tagged_descriptor: &groove::records::RecordDescriptor,
    tagged_values: &[Value],
) -> VersionRow {
    let deletion_idx = tagged_descriptor.field_index("_deletion").unwrap();
    let is_deletion = !matches!(tagged_values[deletion_idx], Value::Nullable(None));
    let storage_descriptor = if is_deletion {
        table.register_storage_table().record_schema()
    } else {
        table.history_storage_table().record_schema()
    };
    let storage_values = storage_descriptor
        .fields()
        .iter()
        .map(|field| {
            let field_name = field.name.as_ref().unwrap();
            let idx = regular_or_tagged_field_index(tagged_descriptor, table, field_name).unwrap();
            if is_deletion && field_name == "_deletion" {
                match &tagged_values[idx] {
                    Value::Nullable(Some(value)) => match value.as_ref() {
                        Value::U8(discriminant) | Value::Enum(discriminant) => {
                            Value::Enum(*discriminant)
                        }
                        value => panic!("unexpected deletion discriminant value: {value:?}"),
                    },
                    value => panic!("deletion row must carry deletion discriminant: {value:?}"),
                }
            } else {
                tagged_values[idx].clone()
            }
        })
        .collect::<Vec<_>>();
    let raw = storage_descriptor.create(&storage_values).unwrap();
    VersionRow {
        table: groove::Intern::new(table.name.clone()),
        record: OwnedRecord::new(raw, storage_descriptor),
    }
}

fn regular_or_tagged_field_index(
    descriptor: &groove::records::RecordDescriptor,
    table: &TableSchema,
    field_name: &str,
) -> Option<usize> {
    descriptor.field_index(field_name).or_else(|| {
        field_name.strip_prefix("user_").and_then(|column| {
            descriptor.field_index(&crate::node::query_eval::maintained_view_tagged_user_field(
                &table.name,
                column,
            ))
        })
    })
}

#[test]
fn unsupported_policy_predicates_deny_instead_of_allowing() {
    let schema = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    core.catalogue.schema.tables[0].read_policy =
        Some(Query::from("todos").filter(gt(col("title"), lit("a"))));
    let tx = core
        .commit_mergeable(
            MergeableCommit::new("todos", row(0x83), 10).cells(owner_cells(user(0xa1), "z")),
        )
        .unwrap();
    core.apply_fate_update(
        tx,
        Fate::Accepted,
        Some(GlobalSeq(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let table = core.table("todos").unwrap().clone();
    let version = core.query_versions_for_tx(tx).unwrap().remove(0);
    assert!(
        !core
            .read_policy_allows_version(&table, &version, user(0xa1))
            .unwrap()
    );
}

#[test]
fn unresolved_policy_operands_deny_instead_of_allowing() {
    let schema = JazzSchema::new([TableSchema::new(
        "todos",
        [ColumnSchema::new("title", ColumnType::String)],
    )]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    core.catalogue.schema.tables[0].read_policy =
        Some(Query::from("todos").filter(eq(col("title"), claim("missing"))));
    let tx = core
        .commit_mergeable(MergeableCommit::new("todos", row(0x84), 10).cells(title_cells("z")))
        .unwrap();
    core.apply_fate_update(
        tx,
        Fate::Accepted,
        Some(GlobalSeq(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let table = core.table("todos").unwrap().clone();
    let version = core.query_versions_for_tx(tx).unwrap().remove(0);
    assert!(
        !core
            .read_policy_allows_version(&table, &version, user(0xa1))
            .unwrap()
    );
}

#[test]
fn unbound_team_claim_in_composed_read_policy_denies_without_binding_error() {
    let schema = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("team", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::shape(
        Query::from("todos").filter(eq(col("team"), claim("team"))),
    ))]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let tx = core
        .commit_mergeable(
            MergeableCommit::new("todos", row(0x87), 10).cells(BTreeMap::from([
                ("title".to_owned(), v("team-owned")),
                ("team".to_owned(), Value::Uuid(user(0xa1).0)),
            ])),
        )
        .unwrap();
    core.apply_fate_update(
        tx,
        Fate::Accepted,
        Some(GlobalSeq(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let mut edge = PeerState::edge_client(user(0xa1));

    assert_view_update_only_references_rows(
        &edge.current_rows_update(&mut core, "todos").unwrap(),
        BTreeSet::new(),
    );
}

#[test]
fn registered_team_claim_in_composed_read_policy_allows_matching_rows() {
    let schema = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("team", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::shape(
        Query::from("todos").filter(eq(col("team"), claim("team"))),
    ))]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let team_a = user(0xa1);
    let team_b = user(0xb2);
    let tx_a = core
        .commit_mergeable(
            MergeableCommit::new("todos", row(0x87), 10).cells(BTreeMap::from([
                ("title".to_owned(), v("team-a")),
                ("team".to_owned(), Value::Uuid(team_a.0)),
            ])),
        )
        .unwrap();
    core.apply_fate_update(
        tx_a,
        Fate::Accepted,
        Some(GlobalSeq(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let tx_b = core
        .commit_mergeable(
            MergeableCommit::new("todos", row(0x88), 11).cells(BTreeMap::from([
                ("title".to_owned(), v("team-b")),
                ("team".to_owned(), Value::Uuid(team_b.0)),
            ])),
        )
        .unwrap();
    core.apply_fate_update(
        tx_b,
        Fate::Accepted,
        Some(GlobalSeq(2)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    core.set_session_claims(
        team_a,
        BTreeMap::from([("team".to_owned(), Value::Uuid(team_a.0))]),
    );
    let mut edge = PeerState::edge_client(team_a);

    assert_view_update_only_references_rows(
        &edge.current_rows_update(&mut core, "todos").unwrap(),
        BTreeSet::from([row(0x87)]),
    );
}

fn recursive_doc_write_policy_schema() -> JazzSchema {
    let policy = Policy::shape(Query::from("docs").reachable_via(
        "doc_access",
        "doc",
        "team",
        claim("sub"),
        "team_edges",
        "member",
        "parent",
        [],
    ));

    JazzSchema::new([
        TableSchema::new(
            "docs",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("kind", ColumnType::String),
            ],
        )
        .with_read_policy(Policy::public())
        .with_write_policy(policy),
        TableSchema::new("teams", [ColumnSchema::new("name", ColumnType::String)])
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
        TableSchema::new(
            "doc_access",
            [
                ColumnSchema::new("doc", ColumnType::Uuid),
                ColumnSchema::new("team", ColumnType::Uuid),
            ],
        )
        .with_reference("doc", "docs")
        .with_reference("team", "teams")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "team_edges",
            [
                ColumnSchema::new("member", ColumnType::Uuid),
                ColumnSchema::new("parent", ColumnType::Uuid),
            ],
        )
        .with_reference("member", "teams")
        .with_reference("parent", "teams")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
    ])
}

fn recursive_doc_cells(title: &str, kind: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("kind".to_owned(), Value::String(kind.to_owned())),
    ])
}

#[test]
fn recursive_reachable_write_policy_allows_direct_and_closure_docs() {
    let schema = recursive_doc_write_policy_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let reader = user(0xb2);
    let direct_doc = RowUuid(uuid::uuid!("10000000-0000-0000-0000-000000000001"));
    let closure_doc = RowUuid(uuid::uuid!("10000000-0000-0000-0000-000000000002"));
    let hidden_doc = RowUuid(uuid::uuid!("10000000-0000-0000-0000-000000000003"));
    let parent_team = RowUuid(uuid::uuid!("20000000-0000-0000-0000-000000000002"));
    let hidden_team = RowUuid(uuid::uuid!("20000000-0000-0000-0000-000000000003"));

    for (team, name) in [
        (RowUuid(reader.0), "reader"),
        (parent_team, "parent"),
        (hidden_team, "hidden"),
    ] {
        accept_global(
            &mut core,
            MergeableCommit::new("teams", team, 10).cells(BTreeMap::from([(
                "name".to_owned(),
                Value::String(name.to_owned()),
            )])),
        );
    }
    for (doc, title, kind) in [
        (direct_doc, "direct", "visible"),
        (closure_doc, "closure", "visible"),
        (hidden_doc, "hidden", "hidden"),
    ] {
        accept_global(
            &mut core,
            MergeableCommit::new("docs", doc, 20).cells(recursive_doc_cells(title, kind)),
        );
    }
    for (idx, doc, team) in [
        (0xa1, direct_doc, RowUuid(reader.0)),
        (0xa2, closure_doc, parent_team),
        (0xa3, hidden_doc, hidden_team),
    ] {
        accept_global(
            &mut core,
            MergeableCommit::new("doc_access", row(idx), 30).cells(BTreeMap::from([
                ("doc".to_owned(), Value::Uuid(doc.0)),
                ("team".to_owned(), Value::Uuid(team.0)),
            ])),
        );
    }
    accept_global(
        &mut core,
        MergeableCommit::new("team_edges", row(0xe1), 40).cells(BTreeMap::from([
            ("member".to_owned(), Value::Uuid(reader.0)),
            ("parent".to_owned(), Value::Uuid(parent_team.0)),
        ])),
    );

    assert!(core.dry_run_write_current_allows("docs", direct_doc, reader).unwrap());
    assert!(core.dry_run_write_current_allows("docs", closure_doc, reader).unwrap());
    assert!(!core.dry_run_write_current_allows("docs", hidden_doc, reader).unwrap());
}

#[test]
fn unbound_is_admin_claim_in_read_policy_denies_as_false() {
    let schema = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("requiresAdmin", ColumnType::Bool),
        ],
    )
    .with_read_policy(Policy::shape(
        Query::from("todos").filter(eq(col("requiresAdmin"), claim("isAdmin"))),
    ))]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let tx = core
        .commit_mergeable(
            MergeableCommit::new("todos", row(0x88), 10).cells(BTreeMap::from([
                ("title".to_owned(), v("admin")),
                ("requiresAdmin".to_owned(), Value::Bool(true)),
            ])),
        )
        .unwrap();
    core.apply_fate_update(
        tx,
        Fate::Accepted,
        Some(GlobalSeq(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let table = core.table("todos").unwrap().clone();
    let version = core.query_versions_for_tx(tx).unwrap().remove(0);

    assert!(
        !core
            .read_policy_allows_version(&table, &version, user(0xa1))
            .unwrap()
    );

    let mut edge = PeerState::edge_client(user(0xa1));
    assert_view_update_only_references_rows(
        &edge.current_rows_update(&mut core, "todos").unwrap(),
        BTreeSet::new(),
    );
}

#[test]
fn missing_read_or_write_policy_is_public_for_that_operation() {
    let schema = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )]);
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let (_tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0x85), 10)
                .made_by(user(0xa1))
                .cells(owner_cells(user(0xb2), "public write")),
        )
        .unwrap();
    let [fate] = core.apply_sync_message(unit).unwrap().try_into().unwrap();
    assert!(matches!(
        fate,
        SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            ..
        }
    ));

    let mut edge = PeerState::edge_client(user(0xcc));
    assert_view_update_only_references_rows(
        &edge.current_rows_update(&mut core, "todos").unwrap(),
        BTreeSet::from([row(0x85)]),
    );
}

#[test]
fn content_extent_visibility_requires_referencing_readable_version_row() {
    let schema = JazzSchema::new([TableSchema::new(
        "docs",
        [
            crate::schema::ColumnSchema::text("body"),
            crate::schema::ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::owner_only("docs", "owner"))]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let owner = user(0xa1);
    let other = user(0xb2);
    let row_uuid = row(0x86);
    let tx = core
        .commit_mergeable(
            MergeableCommit::new("docs", row_uuid, 10)
                .made_by(owner)
                .cells(BTreeMap::from([
                    ("body".to_owned(), Value::Bytes(b"secret".to_vec())),
                    ("owner".to_owned(), Value::Uuid(owner.0)),
                ])),
        )
        .unwrap();
    core.apply_fate_update(
        tx,
        Fate::Accepted,
        Some(GlobalSeq(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    let table = core.table("docs").unwrap().clone();
    let version = core.query_versions_for_tx(tx).unwrap().remove(0);
    let Value::Bytes(payload) = version.cell(&table, "body").unwrap().unwrap() else {
        panic!("body must be stored as text oplog bytes");
    };
    let extent = text_oplog::decode(&payload)
        .unwrap()
        .into_iter()
        .find_map(|op| match op {
            TextOp::Insert {
                content: TextContent::Ref(extent),
                ..
            } => Some(extent),
            _ => None,
        })
        .expect("large-value commit must reference a content extent");
    let mut owner_peer = PeerState::edge_client(owner);
    assert!(matches!(
        owner_peer
            .serve_content_extents(&mut core, row_uuid, [extent.clone()])
            .unwrap(),
        SyncMessage::ContentExtents { extents } if extents.len() == 1 && extents[0].bytes == b"secret"
    ));

    let mut other_peer = PeerState::edge_client(other);
    assert!(matches!(
        other_peer.serve_content_extents(&mut core, row_uuid, [extent.clone()]),
        Err(Error::UnsupportedSyncMessage(
            "content extent is not visible for row"
        ))
    ));

    let unreferenced = core
        .content_store()
        .append(owner, row_uuid, "body", b"not referenced")
        .unwrap();
    assert!(matches!(
        owner_peer.serve_content_extents(&mut core, row_uuid, [unreferenced]),
        Err(Error::UnsupportedSyncMessage(
            "content extent is not visible for row"
        ))
    ));
}
