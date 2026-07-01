use crate::query::{Include, JoinMode, OrderDirection, PolicyBranch, Predicate, any_of};

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
fn maintained_public_query_bundle_filters_private_rows_from_same_tx() {
    let schema = JazzSchema::new([
        TableSchema::new(
            "announcements",
            [ColumnSchema::new("title", ColumnType::String)],
        )
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "messages",
            [
                ColumnSchema::new("body", ColumnType::String),
                ColumnSchema::new("owner_id", ColumnType::String),
            ],
        )
        .with_read_policy(Policy::shape(
            Query::from("messages").filter(eq(col("owner_id"), claim("user_id"))),
        ))
        .with_write_policy(Policy::public()),
    ]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let (_bob_dir, mut bob_node) = open_node_with_schema(node(4), schema.clone());
    let alice = user(0xa1);
    let bob = user(0xb2);
    let announcement_row = row(0x11);
    let private_message_row = row(0x12);
    let tx_id = core
        .commit_mergeable_many(vec![
            MergeableCommit::new("announcements", announcement_row, 10)
                .made_by(alice)
                .cells(BTreeMap::from([("title".to_owned(), v("public"))])),
            MergeableCommit::new("messages", private_message_row, 10)
                .made_by(alice)
                .cells(BTreeMap::from([
                    ("body".to_owned(), v("alice private")),
                    ("owner_id".to_owned(), Value::String(alice.0.to_string())),
                ])),
        ])
        .unwrap();
    core.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalSeq(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let shape = Query::from("announcements").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut bob_peer = PeerState::for_author(bob);

    let update = bob_peer
        .rehydrate_query(&mut core, &shape, &binding)
        .unwrap();
    let SyncMessage::ViewUpdate {
        version_bundles,
        peer_payload_inventory:
            crate::protocol::PeerPayloadInventory {
                complete_tx_payloads,
            },
        result_member_adds,
        ..
    } = &update
    else {
        panic!("expected view update");
    };
    assert_eq!(result_member_adds, &vec![(
        groove::Intern::new("announcements".to_owned()),
        announcement_row,
        tx_id
    )]);
    assert!(complete_tx_payloads.is_empty());
    assert!(!bob_peer.shipped_complete_tx_payloads().contains(&tx_id));
    let shipped_rows = version_bundles
        .iter()
        .flat_map(|bundle| bundle.versions.iter().map(|version| version.row_uuid()))
        .collect::<BTreeSet<_>>();
    assert_eq!(shipped_rows, BTreeSet::from([announcement_row]));

    bob_node.apply_sync_message(update).unwrap();
    assert_eq!(
        bob_node
            .current_rows("announcements", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<Vec<_>>(),
        vec![(
            announcement_row,
            BTreeMap::from([("title".to_owned(), v("public"))])
        )]
    );
    assert!(
        bob_node
            .current_rows("messages", DurabilityTier::Local)
            .unwrap()
            .is_empty()
    );
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
        result_member_adds,
        result_member_removes,
        ..
    } = &update
    else {
        panic!("expected view update");
    };
    assert!(version_bundles.is_empty());
    assert!(complete_tx_payload_refs.is_empty());
    assert!(result_member_adds.is_empty());
    assert_eq!(
        result_member_removes,
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
        result_member_removes, ..
    } = &revoked_update
    else {
        panic!("expected view update");
    };
    assert_eq!(
        result_member_removes,
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
fn write_policy_branch_or_join_allows_either_literal_branch_or_membership_join() {
    let invited = user(0xa1);
    let uninvited = user(0xb2);
    let public_canvas = row(8);
    let private_canvas = row(9);
    let blocked_canvas = row(11);
    let invite_row = row(10);
    let policy = Policy::shape(
        Query::from("canvases")
            .filter(eq(col("isPublic"), lit(true)))
            .policy_branch(PolicyBranch::from_query(Query::from("canvases").join_via(
                "canvasInvites",
                "canvas",
                [eq(col("userID"), claim("sub"))],
            ))),
    );
    let schema = JazzSchema::new([
        TableSchema::new(
            "canvases",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("isPublic", ColumnType::Bool),
            ],
        )
        .with_write_policy(policy),
        TableSchema::new(
            "canvasInvites",
            [
                ColumnSchema::new("canvas", ColumnType::Uuid),
                ColumnSchema::new("userID", ColumnType::Uuid),
            ],
        )
        .with_reference("canvas", "canvases"),
    ]);
    let (_invited_dir, mut invited_writer) = open_node_with_schema(node(1), schema.clone());
    let (_uninvited_dir, mut uninvited_writer) = open_node_with_schema(node(2), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);

    let invite_tx = core
        .commit_mergeable(MergeableCommit::new("canvasInvites", invite_row, 3).cells(
            BTreeMap::from([
                ("canvas".to_owned(), Value::Uuid(private_canvas.0)),
                ("userID".to_owned(), Value::Uuid(invited.0)),
            ]),
        ))
        .unwrap();
    core.apply_fate_update(
        invite_tx,
        Fate::Accepted,
        Some(GlobalSeq(0)),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    let public_tx = uninvited_writer
        .commit_mergeable_unit(
            MergeableCommit::new("canvases", public_canvas, 14)
                .made_by(uninvited)
                .cells(BTreeMap::from([
                    ("title".to_owned(), Value::String("public".to_owned())),
                    ("isPublic".to_owned(), Value::Bool(true)),
                ])),
        )
        .unwrap();
    let [public_fate] = core.apply_sync_message(public_tx.1).unwrap().try_into().unwrap();
    assert!(matches!(
        public_fate,
        SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            ..
        }
    ));

    let private_tx = invited_writer
        .commit_mergeable_unit(
            MergeableCommit::new("canvases", private_canvas, 15)
                .made_by(invited)
                .cells(BTreeMap::from([
                    ("title".to_owned(), Value::String("private".to_owned())),
                    ("isPublic".to_owned(), Value::Bool(false)),
                ])),
        )
        .unwrap();
    let [private_fate] = core
        .apply_sync_message(private_tx.1)
        .unwrap()
        .try_into()
        .unwrap();
    assert!(matches!(
        private_fate,
        SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            ..
        }
    ));

    let blocked_tx = uninvited_writer
        .commit_mergeable_unit(
            MergeableCommit::new("canvases", blocked_canvas, 16)
                .made_by(uninvited)
                .cells(BTreeMap::from([
                    ("title".to_owned(), Value::String("blocked".to_owned())),
                    ("isPublic".to_owned(), Value::Bool(false)),
                ])),
        )
        .unwrap();
    let [blocked_fate] = core
        .apply_sync_message(blocked_tx.1)
        .unwrap()
        .try_into()
        .unwrap();
    assert!(matches!(
        blocked_fate,
        SyncMessage::FateUpdate {
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            ..
        }
    ));
}

#[test]
fn read_policy_branch_or_join_allows_public_or_membership_reads() {
    let member = user(0xa1);
    let other = user(0xb2);
    let public_chat = row(0x18);
    let private_chat = row(0x19);
    let membership = row(0x1a);
    let policy = Policy::shape(
        Query::from("chats")
            .filter(eq(col("isPublic"), lit(true)))
            .policy_branch(PolicyBranch::from_query(Query::from("chats").join_via(
                "chatMembers",
                "chatId",
                [eq(col("userId"), claim("user_id"))],
            ))),
    );
    let schema = JazzSchema::new([
        TableSchema::new(
            "chats",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("isPublic", ColumnType::Bool),
                ColumnSchema::new("createdBy", ColumnType::Uuid),
            ],
        )
        .with_read_policy(policy)
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "chatMembers",
            [
                ColumnSchema::new("chatId", ColumnType::Uuid),
                ColumnSchema::new("userId", ColumnType::String),
            ],
        )
        .with_reference("chatId", "chats")
        .with_write_policy(Policy::public()),
    ]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let (_member_dir, _member_reader) = open_node_with_schema(node(3), schema.clone());
    let (_other_dir, _other_reader) = open_node_with_schema(node(4), schema);

    accept_global(
        &mut core,
        MergeableCommit::new("chats", public_chat, 10)
            .made_by(member)
            .cells(BTreeMap::from([
                ("title".to_owned(), Value::String("public".to_owned())),
                ("isPublic".to_owned(), Value::Bool(true)),
                ("createdBy".to_owned(), Value::Uuid(member.0)),
            ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("chats", private_chat, 11)
            .made_by(member)
            .cells(BTreeMap::from([
                ("title".to_owned(), Value::String("private".to_owned())),
                ("isPublic".to_owned(), Value::Bool(false)),
                ("createdBy".to_owned(), Value::Uuid(member.0)),
            ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("chatMembers", membership, 12).cells(BTreeMap::from([
            ("chatId".to_owned(), Value::Uuid(private_chat.0)),
            ("userId".to_owned(), Value::String(member.0.to_string())),
        ])),
    );
    let shape = Query::from("chats").validate(&core.catalogue.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    assert_eq!(
        core.query_rows_for_link(&shape, &binding, DurabilityTier::Global, member)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([public_chat, private_chat])
    );
    assert_eq!(
        core.query_rows_for_link(&shape, &binding, DurabilityTier::Global, other)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([public_chat])
    );
}

#[test]
fn message_read_policy_allows_public_chat_or_membership_join() {
    let member = user(0xa1);
    let other = user(0xb2);
    let public_chat = row(0x18);
    let private_chat = row(0x19);
    let public_message = row(0x28);
    let private_message = row(0x29);
    let membership = row(0x1a);
    let policy = Policy::shape(
        Query::from("messages")
            .join_via_row_id("chats", "chat_id", [eq(col("visibility"), lit("public"))])
            .policy_branch(PolicyBranch::from_query(Query::from("messages").join_via_column(
                "chat_members",
                "chat_id",
                "chat_id",
                [eq(col("user_id"), claim("user_id"))],
            ))),
    );
    let schema = JazzSchema::new([
        TableSchema::new(
            "chats",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("visibility", ColumnType::String),
            ],
        )
        .with_read_policy(Policy::shape(
            Query::from("chats")
                .filter(eq(col("visibility"), lit("public")))
                .policy_branch(PolicyBranch::from_query(Query::from("chats").join_via_column(
                    "chat_members",
                    "chat_id",
                    "id",
                    [eq(col("user_id"), claim("user_id"))],
                ))),
        ))
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "chat_members",
            [
                ColumnSchema::new("chat_id", ColumnType::Uuid),
                ColumnSchema::new("user_id", ColumnType::String),
            ],
        )
        .with_reference("chat_id", "chats")
        .with_read_policy(Policy::shape(
            Query::from("chat_members")
                .filter(Predicate::Any(Vec::new()))
                .policy_branch(PolicyBranch::from_query(
                    Query::from("chat_members").filter(eq(col("user_id"), claim("user_id"))),
                ))
                .policy_branch(PolicyBranch::from_query(Query::from("chat_members").join_via_column(
                    "chat_members",
                    "chat_id",
                    "chat_id",
                    [eq(col("user_id"), claim("user_id"))],
                ))),
        ))
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "messages",
            [
                ColumnSchema::new("chat_id", ColumnType::Uuid),
                ColumnSchema::new("text", ColumnType::String),
            ],
        )
        .with_reference("chat_id", "chats")
        .with_read_policy(policy)
        .with_write_policy(Policy::public()),
    ]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);

    accept_global(
        &mut core,
        MergeableCommit::new("chats", public_chat, 10).cells(BTreeMap::from([
            ("title".to_owned(), Value::String("public".to_owned())),
            ("visibility".to_owned(), Value::String("public".to_owned())),
        ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("chats", private_chat, 11).cells(BTreeMap::from([
            ("title".to_owned(), Value::String("private".to_owned())),
            ("visibility".to_owned(), Value::String("private".to_owned())),
        ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("messages", public_message, 12).cells(BTreeMap::from([
            ("chat_id".to_owned(), Value::Uuid(public_chat.0)),
            ("text".to_owned(), Value::String("public message".to_owned())),
        ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("messages", private_message, 13).cells(BTreeMap::from([
            ("chat_id".to_owned(), Value::Uuid(private_chat.0)),
            ("text".to_owned(), Value::String("private message".to_owned())),
        ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("chat_members", membership, 14).cells(BTreeMap::from([
            ("chat_id".to_owned(), Value::Uuid(private_chat.0)),
            ("user_id".to_owned(), Value::String(member.0.to_string())),
        ])),
    );

    let public_shape = Query::from("messages")
        .join_via_row_id("chats", "chat_id", [eq(col("visibility"), lit("public"))])
        .validate(&core.catalogue.schema)
        .unwrap();
    let public_binding = public_shape.bind(BTreeMap::new()).unwrap();
    assert_eq!(
        core.query_rows(&public_shape, &public_binding, DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([public_message])
    );
    assert_eq!(
        core.query_rows(&public_shape, &public_binding, DurabilityTier::Edge)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([public_message])
    );

    let shape = Query::from("messages")
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    assert_eq!(
        core.query_rows_for_link(&shape, &binding, DurabilityTier::Global, member)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([public_message, private_message])
    );
    assert_eq!(
        core.query_rows_for_link(&shape, &binding, DurabilityTier::Edge, member)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([public_message, private_message])
    );
    assert_eq!(
        core.query_rows_for_link(&shape, &binding, DurabilityTier::Global, other)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([public_message])
    );
}

#[test]
fn camel_case_message_read_policy_incrementally_adds_member_message() {
    let alice = user(0xa1);
    let bob = user(0xb2);
    let chat = row(0x18);
    let alice_profile = row(0x38);
    let bob_profile = row(0x39);
    let alice_message = row(0x28);
    let bob_message = row(0x29);
    let alice_membership = row(0x1a);
    let bob_membership = row(0x1b);
    let policy = Policy::shape(
        Query::from("messages")
            .filter(Predicate::Any(Vec::new()))
            .policy_branch(PolicyBranch::from_query(Query::from("messages").join_via_row_id(
                "chats",
                "chatId",
                [eq(col("isPublic"), lit(true))],
            )))
            .policy_branch(PolicyBranch::from_query(Query::from("messages").join_via_column(
                "chatMembers",
                "chatId",
                "chatId",
                [eq(col("userId"), claim("user_id"))],
            ))),
    );
    let schema = JazzSchema::new([
        TableSchema::new(
            "chats",
            [
                ColumnSchema::new("isPublic", ColumnType::Bool),
                ColumnSchema::new("createdBy", ColumnType::String),
            ],
        )
        .with_read_policy(Policy::shape(
            Query::from("chats")
                .filter(Predicate::Any(Vec::new()))
                .policy_branch(PolicyBranch::from_query(
                    Query::from("chats").filter(eq(col("isPublic"), lit(true))),
                ))
                .policy_branch(PolicyBranch::from_query(Query::from("chats").join_via_column(
                    "chatMembers",
                    "chatId",
                    "id",
                    [eq(col("userId"), claim("user_id"))],
                ))),
        ))
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "chatMembers",
            [
                ColumnSchema::new("chatId", ColumnType::Uuid),
                ColumnSchema::new("userId", ColumnType::String),
            ],
        )
        .with_reference("chatId", "chats")
        .with_read_policy(Policy::shape(
            Query::from("chatMembers")
                .filter(Predicate::Any(Vec::new()))
                .policy_branch(PolicyBranch::from_query(
                    Query::from("chatMembers").filter(eq(col("userId"), claim("user_id"))),
                ))
                .policy_branch(PolicyBranch::from_query(Query::from("chatMembers").join_via_column(
                    "chatMembers",
                    "chatId",
                    "chatId",
                    [eq(col("userId"), claim("user_id"))],
                ))),
        ))
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "messages",
            [
                ColumnSchema::new("chatId", ColumnType::Uuid),
                ColumnSchema::new("text", ColumnType::String),
                ColumnSchema::new("senderId", ColumnType::Uuid),
                ColumnSchema::new("createdAt", ColumnType::U64),
            ],
        )
        .with_reference("chatId", "chats")
        .with_reference("senderId", "profiles")
        .with_read_policy(policy)
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "profiles",
            [
                ColumnSchema::new("userId", ColumnType::String),
                ColumnSchema::new("name", ColumnType::String),
            ],
        )
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
    ]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);

    accept_global(
        &mut core,
        MergeableCommit::new("chats", chat, 10).cells(BTreeMap::from([
            ("isPublic".to_owned(), Value::Bool(true)),
            ("createdBy".to_owned(), Value::String(alice.0.to_string())),
        ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("chatMembers", alice_membership, 11).cells(BTreeMap::from([
            ("chatId".to_owned(), Value::Uuid(chat.0)),
            ("userId".to_owned(), Value::String(alice.0.to_string())),
        ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("messages", alice_message, 12).cells(BTreeMap::from([
            ("chatId".to_owned(), Value::Uuid(chat.0)),
            ("text".to_owned(), Value::String("hello".to_owned())),
            ("senderId".to_owned(), Value::Uuid(alice_profile.0)),
            ("createdAt".to_owned(), Value::U64(12)),
        ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("profiles", alice_profile, 15).cells(BTreeMap::from([
            ("userId".to_owned(), Value::String(alice.0.to_string())),
            ("name".to_owned(), Value::String("Alice".to_owned())),
        ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("profiles", bob_profile, 16).cells(BTreeMap::from([
            ("userId".to_owned(), Value::String(bob.0.to_string())),
            ("name".to_owned(), Value::String("Bob".to_owned())),
        ])),
    );

    let shape = Query::from("messages")
        .include("senderId")
        .order_by("createdAt", OrderDirection::Desc)
        .limit(21)
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut alice_peer = PeerState::for_author(alice);
    alice_peer
        .rehydrate_query(&mut core, &shape, &binding)
        .unwrap();

    let bob_membership_tx = accept_global(
        &mut core,
        MergeableCommit::new("chatMembers", bob_membership, 13).cells(BTreeMap::from([
            ("chatId".to_owned(), Value::Uuid(chat.0)),
            ("userId".to_owned(), Value::String(bob.0.to_string())),
        ])),
    );
    let bob_message_tx = accept_global(
        &mut core,
        MergeableCommit::new("messages", bob_message, 14).cells(BTreeMap::from([
            ("chatId".to_owned(), Value::Uuid(chat.0)),
            ("text".to_owned(), Value::String("from bob".to_owned())),
            ("senderId".to_owned(), Value::Uuid(bob_profile.0)),
            ("createdAt".to_owned(), Value::U64(14)),
        ])),
    );

    let update = alice_peer.query_update(&mut core, &shape, &binding).unwrap();
    assert_view_update_only_references_rows(&update, BTreeSet::from([bob_message, bob_profile]));
    assert_view_update_only_ships_rows(&update, BTreeSet::from([bob_message, bob_profile]));
    assert!(matches!(
        update,
            SyncMessage::ViewUpdate {
                result_member_adds: ref adds,
                ..
        } if adds.iter().any(|entry| entry == &("messages".to_owned().into(), bob_message, bob_message_tx))
    ));
    let _ = bob_membership_tx;
}

#[test]
fn edge_read_policy_joins_use_edge_visible_dependency_rows() {
    let member = user(0xa1);
    let other = user(0xb2);
    let bob = user(0xc3);
    let public_chat = row(0x18);
    let private_chat = row(0x19);
    let public_message = row(0x28);
    let private_message = row(0x29);
    let bob_private_message = row(0x2a);
    let membership = row(0x1a);
    let bob_membership = row(0x1b);
    let policy = Policy::shape(
        Query::from("messages")
            .join_via_row_id("chats", "chat_id", [eq(col("visibility"), lit("public"))])
            .policy_branch(PolicyBranch::from_query(Query::from("messages").join_via_column(
                "chat_members",
                "chat_id",
                "chat_id",
                [eq(col("user_id"), claim("user_id"))],
            ))),
    );
    let schema = JazzSchema::new([
        TableSchema::new(
            "chat_members",
            [
                ColumnSchema::new("chat_id", ColumnType::Uuid),
                ColumnSchema::new("user_id", ColumnType::String),
            ],
        )
        .with_reference("chat_id", "chats")
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "chats",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("visibility", ColumnType::String),
            ],
        )
        .with_read_policy(Policy::shape(
            Query::from("chats")
                .filter(eq(col("visibility"), lit("public")))
                .policy_branch(PolicyBranch::from_query(Query::from("chats").join_via_column(
                    "chat_members",
                    "chat_id",
                    "id",
                    [eq(col("user_id"), claim("user_id"))],
                ))),
        ))
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "messages",
            [
                ColumnSchema::new("chat_id", ColumnType::Uuid),
                ColumnSchema::new("text", ColumnType::String),
            ],
        )
        .with_reference("chat_id", "chats")
        .with_read_policy(policy)
        .with_write_policy(Policy::public()),
    ]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    for commit in [
        MergeableCommit::new("chats", public_chat, 10).cells(BTreeMap::from([
            ("title".to_owned(), Value::String("public".to_owned())),
            ("visibility".to_owned(), Value::String("public".to_owned())),
        ])),
        MergeableCommit::new("chats", private_chat, 11).cells(BTreeMap::from([
            ("title".to_owned(), Value::String("private".to_owned())),
            ("visibility".to_owned(), Value::String("private".to_owned())),
        ])),
        MergeableCommit::new("messages", public_message, 12).cells(BTreeMap::from([
            ("chat_id".to_owned(), Value::Uuid(public_chat.0)),
            ("text".to_owned(), Value::String("public message".to_owned())),
        ])),
        MergeableCommit::new("messages", private_message, 13).cells(BTreeMap::from([
            ("chat_id".to_owned(), Value::Uuid(private_chat.0)),
            ("text".to_owned(), Value::String("private message".to_owned())),
        ])),
        MergeableCommit::new("chat_members", membership, 14).cells(BTreeMap::from([
            ("chat_id".to_owned(), Value::Uuid(private_chat.0)),
            ("user_id".to_owned(), Value::String(member.0.to_string())),
        ])),
        MergeableCommit::new("chat_members", bob_membership, 15).cells(BTreeMap::from([
            ("chat_id".to_owned(), Value::Uuid(private_chat.0)),
            ("user_id".to_owned(), Value::String(bob.0.to_string())),
        ])),
        MergeableCommit::new("messages", bob_private_message, 16).cells(BTreeMap::from([
            ("chat_id".to_owned(), Value::Uuid(private_chat.0)),
            ("text".to_owned(), Value::String("bob private message".to_owned())),
        ])),
    ] {
        let tx_id = core.commit_mergeable_many(vec![commit]).unwrap();
        core.apply_fate_update(tx_id, Fate::Accepted, None, Some(DurabilityTier::Edge))
            .unwrap();
    }

    let shape = Query::from("messages")
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    core.query.settled_result_sets.insert(
        crate::protocol::BindingViewKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
        read_view: Default::default(),
},
        BTreeSet::new(),
    );
    assert!(
        core.query_rows_for_link(&shape, &binding, DurabilityTier::Global, member)
            .unwrap()
            .is_empty(),
        "global policy reads must not be authorized by edge-only dependency rows",
    );
    assert_eq!(
        core.query_rows_for_link(&shape, &binding, DurabilityTier::Edge, member)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([public_message, private_message, bob_private_message])
    );
    assert_eq!(
        core.query_rows_for_link(&shape, &binding, DurabilityTier::Edge, other)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([public_message])
    );

    let mut other_peer = PeerState::edge_client(other);
    let update = other_peer
        .rehydrate_query_with_opts(
            &mut core,
            &shape,
            &binding,
            RegisterShapeOptions {
                tier: DurabilityTier::Edge,
                ..RegisterShapeOptions::default()
            },
        )
        .unwrap();
    assert_view_update_only_references_rows(&update, BTreeSet::from([public_chat, public_message]));
    assert_view_update_only_ships_rows(&update, BTreeSet::from([public_chat, public_message]));

    let mut member_peer = PeerState::edge_client(member);
    let update = member_peer
        .rehydrate_query_with_opts(
            &mut core,
            &shape,
            &binding,
            RegisterShapeOptions {
                tier: DurabilityTier::Edge,
                ..RegisterShapeOptions::default()
            },
        )
        .unwrap();
    assert_view_update_only_references_rows(
        &update,
        BTreeSet::from([
            public_chat,
            private_chat,
            public_message,
            private_message,
            bob_private_message,
        ]),
    );
    assert_view_update_only_ships_rows(
        &update,
        BTreeSet::from([
            public_chat,
            private_chat,
            public_message,
            private_message,
            bob_private_message,
        ]),
    );
}

#[test]
fn edge_membership_insert_updates_previously_empty_private_message_query() {
    let alice = user(0xa1);
    let bob = user(0xb2);
    let chat = row(0x18);
    let seed_message = row(0x28);
    let alice_membership = row(0x1a);
    let bob_membership = row(0x1b);
    let policy = Policy::shape(
        Query::from("messages")
            .filter(Predicate::Any(Vec::new()))
            .policy_branch(PolicyBranch::from_query(Query::from("messages").join_via_column(
                "chatMembers",
                "chatId",
                "chatId",
                [eq(col("userId"), claim("user_id"))],
            ))),
    );
    let schema = JazzSchema::new([
        TableSchema::new(
            "chatMembers",
            [
                ColumnSchema::new("chatId", ColumnType::Uuid),
                ColumnSchema::new("userId", ColumnType::String),
            ],
        )
        .with_reference("chatId", "chats")
        .with_read_policy(Policy::shape(
            Query::from("chatMembers")
                .filter(Predicate::Any(Vec::new()))
                .policy_branch(PolicyBranch::from_query(
                    Query::from("chatMembers").filter(eq(col("userId"), claim("user_id"))),
                ))
                .policy_branch(PolicyBranch::from_query(Query::from("chatMembers").join_via_column(
                    "chatMembers",
                    "chatId",
                    "chatId",
                    [eq(col("userId"), claim("user_id"))],
                ))),
        ))
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "chats",
            [
                ColumnSchema::new("isPublic", ColumnType::Bool),
                ColumnSchema::new("createdBy", ColumnType::String),
            ],
        )
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "messages",
            [
                ColumnSchema::new("chatId", ColumnType::Uuid),
                ColumnSchema::new("text", ColumnType::String),
                ColumnSchema::new("createdAt", ColumnType::U64),
            ],
        )
        .with_reference("chatId", "chats")
        .with_read_policy(policy)
        .with_write_policy(Policy::public()),
    ]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    for commit in [
        MergeableCommit::new("chats", chat, 10).cells(BTreeMap::from([
            ("isPublic".to_owned(), Value::Bool(false)),
            ("createdBy".to_owned(), Value::String(alice.0.to_string())),
        ])),
        MergeableCommit::new("chatMembers", alice_membership, 11).cells(BTreeMap::from([
            ("chatId".to_owned(), Value::Uuid(chat.0)),
            ("userId".to_owned(), Value::String(alice.0.to_string())),
        ])),
    ] {
        let tx_id = core.commit_mergeable_many(vec![commit]).unwrap();
        core.apply_fate_update(tx_id, Fate::Accepted, None, Some(DurabilityTier::Edge))
            .unwrap();
    }
    let seed_tx = core
        .commit_mergeable_many(vec![
            MergeableCommit::new("messages", seed_message, 12).cells(BTreeMap::from([
                ("chatId".to_owned(), Value::Uuid(chat.0)),
                ("text".to_owned(), Value::String("invite-only seed".to_owned())),
                ("createdAt".to_owned(), Value::U64(12)),
            ])),
        ])
        .unwrap();
    core.apply_fate_update(seed_tx, Fate::Accepted, None, Some(DurabilityTier::Edge))
        .unwrap();

    let shape = Query::from("messages")
        .filter(eq(col("chatId"), param("chatId")))
        .order_by("createdAt", OrderDirection::Asc)
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([("chatId".to_owned(), Value::Uuid(chat.0))]))
        .unwrap();
    let opts = RegisterShapeOptions {
        tier: DurabilityTier::Edge,
        ..RegisterShapeOptions::default()
    };
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: opts.read_view_key(),
    };
    let mut bob_peer = PeerState::edge_client(bob);
    let initial = bob_peer
        .rehydrate_query_with_opts(&mut core, &shape, &binding, opts)
        .unwrap();
    assert_view_update_only_references_rows(&initial, BTreeSet::new());

    let bob_membership_tx = core
        .commit_mergeable_many(vec![
            MergeableCommit::new("chatMembers", bob_membership, 13).cells(BTreeMap::from([
                ("chatId".to_owned(), Value::Uuid(chat.0)),
                ("userId".to_owned(), Value::String(bob.0.to_string())),
            ])),
        ])
        .unwrap();
    core.apply_fate_update(
        bob_membership_tx,
        Fate::Accepted,
        None,
        Some(DurabilityTier::Edge),
    )
    .unwrap();

    assert_eq!(
        core.query_rows_for_link(&shape, &binding, DurabilityTier::Edge, bob)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([seed_message])
    );

    let update = bob_peer
        .query_update_for_subscription(&mut core, subscription, &shape, &binding)
        .unwrap();
    assert!(matches!(
        update,
            SyncMessage::ViewUpdate {
                result_member_adds: ref adds,
                ..
        } if adds.iter().any(|entry| entry == &("messages".to_owned().into(), seed_message, seed_tx))
    ));
}

#[test]
fn edge_rehydrate_refreshes_previously_covered_private_message_query() {
    let alice = user(0xa1);
    let bob = user(0xb2);
    let chat = row(0x18);
    let seed_message = row(0x28);
    let bob_message = row(0x29);
    let alice_membership = row(0x1a);
    let bob_membership = row(0x1b);
    let policy = Policy::shape(
        Query::from("messages")
            .filter(Predicate::Any(Vec::new()))
            .policy_branch(PolicyBranch::from_query(Query::from("messages").join_via_column(
                "chatMembers",
                "chatId",
                "chatId",
                [eq(col("userId"), claim("user_id"))],
            ))),
    );
    let schema = JazzSchema::new([
        TableSchema::new(
            "chatMembers",
            [
                ColumnSchema::new("chatId", ColumnType::Uuid),
                ColumnSchema::new("userId", ColumnType::String),
            ],
        )
        .with_reference("chatId", "chats")
        .with_read_policy(Policy::shape(
            Query::from("chatMembers")
                .filter(Predicate::Any(Vec::new()))
                .policy_branch(PolicyBranch::from_query(
                    Query::from("chatMembers").filter(eq(col("userId"), claim("user_id"))),
                ))
                .policy_branch(PolicyBranch::from_query(Query::from("chatMembers").join_via_column(
                    "chatMembers",
                    "chatId",
                    "chatId",
                    [eq(col("userId"), claim("user_id"))],
                ))),
        ))
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "chats",
            [
                ColumnSchema::new("isPublic", ColumnType::Bool),
                ColumnSchema::new("createdBy", ColumnType::String),
            ],
        )
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "messages",
            [
                ColumnSchema::new("chatId", ColumnType::Uuid),
                ColumnSchema::new("text", ColumnType::String),
                ColumnSchema::new("createdAt", ColumnType::U64),
            ],
        )
        .with_reference("chatId", "chats")
        .with_read_policy(policy)
        .with_write_policy(Policy::public()),
    ]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    for commit in [
        MergeableCommit::new("chats", chat, 10).cells(BTreeMap::from([
            ("isPublic".to_owned(), Value::Bool(false)),
            ("createdBy".to_owned(), Value::String(alice.0.to_string())),
        ])),
        MergeableCommit::new("chatMembers", alice_membership, 11).cells(BTreeMap::from([
            ("chatId".to_owned(), Value::Uuid(chat.0)),
            ("userId".to_owned(), Value::String(alice.0.to_string())),
        ])),
    ] {
        let tx_id = core.commit_mergeable_many(vec![commit]).unwrap();
        core.apply_fate_update(tx_id, Fate::Accepted, None, Some(DurabilityTier::Edge))
            .unwrap();
    }
    let seed_tx = core
        .commit_mergeable_many(vec![
            MergeableCommit::new("messages", seed_message, 12).cells(BTreeMap::from([
                ("chatId".to_owned(), Value::Uuid(chat.0)),
                ("text".to_owned(), Value::String("invite-only seed".to_owned())),
                ("createdAt".to_owned(), Value::U64(12)),
            ])),
        ])
        .unwrap();
    core.apply_fate_update(seed_tx, Fate::Accepted, None, Some(DurabilityTier::Edge))
        .unwrap();

    let shape = Query::from("messages")
        .filter(eq(col("chatId"), param("chatId")))
        .order_by("createdAt", OrderDirection::Desc)
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([("chatId".to_owned(), Value::Uuid(chat.0))]))
        .unwrap();
    let opts = RegisterShapeOptions {
        tier: DurabilityTier::Edge,
        ..RegisterShapeOptions::default()
    };
    let mut alice_peer = PeerState::edge_client(alice);
    let initial = alice_peer
        .rehydrate_query_with_opts(&mut core, &shape, &binding, opts.clone())
        .unwrap();
    assert!(matches!(
        initial,
            SyncMessage::ViewUpdate {
                result_member_adds: ref adds,
                reset_result_set: true,
                ..
        } if adds.iter().any(|entry| entry == &("messages".to_owned().into(), seed_message, seed_tx))
    ));

    let bob_membership_tx = core
        .commit_mergeable_many(vec![
            MergeableCommit::new("chatMembers", bob_membership, 13).cells(BTreeMap::from([
                ("chatId".to_owned(), Value::Uuid(chat.0)),
                ("userId".to_owned(), Value::String(bob.0.to_string())),
            ])),
        ])
        .unwrap();
    core.apply_fate_update(
        bob_membership_tx,
        Fate::Accepted,
        None,
        Some(DurabilityTier::Edge),
    )
    .unwrap();
    let bob_message_tx = core
        .commit_mergeable_many(vec![
            MergeableCommit::new("messages", bob_message, 14).cells(BTreeMap::from([
                ("chatId".to_owned(), Value::Uuid(chat.0)),
                ("text".to_owned(), Value::String("bob accepted invite".to_owned())),
                ("createdAt".to_owned(), Value::U64(14)),
            ])),
        ])
        .unwrap();
    core.apply_fate_update(bob_message_tx, Fate::Accepted, None, Some(DurabilityTier::Edge))
        .unwrap();

    let rehydrated = alice_peer
        .rehydrate_query_with_opts(&mut core, &shape, &binding, opts)
        .unwrap();
    let SyncMessage::ViewUpdate {
        result_member_adds,
        reset_result_set,
        ..
    } = rehydrated
    else {
        panic!("expected rehydrate view update");
    };
    assert!(reset_result_set);
    assert_eq!(
        result_member_adds
            .into_iter()
            .filter_map(crate::protocol::ResultMemberEntry::into_row)
            .filter(|(table, _, _)| table.as_str() == "messages")
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([
            ("messages".to_owned().into(), seed_message, seed_tx),
            ("messages".to_owned().into(), bob_message, bob_message_tx),
        ])
    );
}

#[test]
fn edge_public_or_owner_claim_policy_rehydrates_empty_result_set() {
    let alice = user(0xa1);
    let bob = user(0xb2);
    let private_chat = row(0x18);
    let public_chat = row(0x19);
    let schema = JazzSchema::new([TableSchema::new(
        "chats",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("visibility", ColumnType::String),
            ColumnSchema::new("owner_id", ColumnType::String),
        ],
    )
    .with_read_policy(Policy::shape(Query::from("chats").filter(any_of([
        eq(col("visibility"), lit("public")),
        eq(col("owner_id"), claim("user_id")),
    ]))))
    .with_write_policy(Policy::public())]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    for commit in [
        MergeableCommit::new("chats", private_chat, 10).cells(BTreeMap::from([
            ("title".to_owned(), Value::String("private".to_owned())),
            ("visibility".to_owned(), Value::String("private".to_owned())),
            ("owner_id".to_owned(), Value::String(alice.0.to_string())),
        ])),
        MergeableCommit::new("chats", public_chat, 11).cells(BTreeMap::from([
            ("title".to_owned(), Value::String("public".to_owned())),
            ("visibility".to_owned(), Value::String("public".to_owned())),
            ("owner_id".to_owned(), Value::String(alice.0.to_string())),
        ])),
    ] {
        let tx_id = core.commit_mergeable_many(vec![commit]).unwrap();
        core.apply_fate_update(tx_id, Fate::Accepted, None, Some(DurabilityTier::Edge))
            .unwrap();
    }

    let shape = Query::from("chats")
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut bob_peer = PeerState::edge_client(bob);
    let update = bob_peer
        .rehydrate_query_with_opts(
            &mut core,
            &shape,
            &binding,
            RegisterShapeOptions {
                tier: DurabilityTier::Edge,
                ..RegisterShapeOptions::default()
            },
        )
        .unwrap();
    assert_view_update_only_references_rows(&update, BTreeSet::from([public_chat]));
    assert_view_update_only_ships_rows(&update, BTreeSet::from([public_chat]));
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
    read_view: Default::default(),
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
            result_member_adds: ref adds,
            ..
        } if adds.is_empty()
    ));
    assert!(matches!(
        spy_initial,
        SyncMessage::ViewUpdate {
            result_member_adds: ref adds,
            ..
        } if adds.is_empty()
    ));
    assert_eq!(
        core.query
            .query_shape_cache
            .keys()
            .filter(|(_, tier, _)| *tier == DurabilityTier::Global)
            .count(),
        1,
        "identities with the same shape and policy should share one prepared graph"
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
        result_member_adds,
        result_member_removes,
        ..
    } = grant_update
    else {
        panic!("expected grant update");
    };
    assert_eq!(
        result_member_adds,
        vec![
            ("canvases".to_owned().into(), canvas_row, canvas_tx),
            ("shapes".to_owned().into(), shape_row, shape_tx),
        ]
    );
    assert!(result_member_removes.is_empty());
    assert_eq!(invited_link.metrics.view_updates_out, 2);

    let spy_update = spy_link.query_update(&mut core, &shape, &binding).unwrap();
    assert!(matches!(
        spy_update,
        SyncMessage::ViewUpdate {
            result_member_adds: ref adds,
            result_member_removes: ref removes,
            ..
        } if adds.is_empty() && removes.is_empty()
    ));
    assert_eq!(spy_link.metrics.result_adds_out, 0);
    assert_eq!(spy_link.metrics.version_bundles_out, 0);

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
        result_member_adds,
        result_member_removes,
        ..
    } = revoke_update
    else {
        panic!("expected revoke update");
    };
    assert!(result_member_adds.is_empty());
    assert_eq!(
        result_member_removes,
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
fn edge_query_rehydrate_applies_session_user_id_read_policy() {
    let schema = JazzSchema::new([
        TableSchema::new(
            "chats",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("visibility", ColumnType::String),
                ColumnSchema::new("owner_id", ColumnType::String),
            ],
        )
        .with_read_policy(Policy::shape(
            Query::from("chats")
                .filter(eq(col("visibility"), lit("public")))
                .policy_branch(PolicyBranch::from_query(
                    Query::from("chats").filter(eq(col("owner_id"), claim("user_id"))),
                )),
        ))
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "messages",
            [
                ColumnSchema::new("chat_id", ColumnType::Uuid),
                ColumnSchema::new("body", ColumnType::String),
                ColumnSchema::new("author_id", ColumnType::String),
                ColumnSchema::new("owner_id", ColumnType::String),
            ],
        )
        .with_read_policy(Policy::shape(
            Query::from("messages").filter(eq(col("owner_id"), claim("user_id"))),
        ))
        .with_write_policy(Policy::public()),
    ]);
    let (_alice_dir, mut alice) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let alice_id = user(0xa1);
    let bob_id = user(0xb2);
    let alice_user_id = alice_id.0.to_string();
    let bob_user_id = bob_id.0.to_string();
    let alice_private_chat_tx = commit_mergeable_global(
        &mut alice,
        &mut core,
        MergeableCommit::new("chats", row(0x10), 10)
            .made_by(alice_id)
            .cells(BTreeMap::from([
                ("title".to_owned(), v("alice private")),
                ("visibility".to_owned(), v("private")),
                ("owner_id".to_owned(), v(alice_user_id.clone())),
            ])),
    );
    core.apply_fate_update(
        alice_private_chat_tx,
        Fate::Accepted,
        None,
        Some(DurabilityTier::Edge),
    )
    .unwrap();
    let public_chat_tx = commit_mergeable_global(
        &mut alice,
        &mut core,
        MergeableCommit::new("chats", row(0x11), 11)
            .made_by(alice_id)
            .cells(BTreeMap::from([
                ("title".to_owned(), v("public")),
                ("visibility".to_owned(), v("public")),
                ("owner_id".to_owned(), v(alice_user_id.clone())),
            ])),
    );
    core.apply_fate_update(public_chat_tx, Fate::Accepted, None, Some(DurabilityTier::Edge))
        .unwrap();
    let alice_private_message_tx = commit_mergeable_global(
        &mut alice,
        &mut core,
        MergeableCommit::new("messages", row(0x20), 12)
            .made_by(alice_id)
            .cells(BTreeMap::from([
                ("chat_id".to_owned(), Value::Uuid(row(0x10).0)),
                ("body".to_owned(), v("alice private message")),
                ("author_id".to_owned(), v(alice_user_id)),
                ("owner_id".to_owned(), v(alice_id.0.to_string())),
            ])),
    );
    core.apply_fate_update(
        alice_private_message_tx,
        Fate::Accepted,
        None,
        Some(DurabilityTier::Edge),
    )
    .unwrap();
    let bob_message_tx = commit_mergeable_global(
        &mut alice,
        &mut core,
        MergeableCommit::new("messages", row(0x21), 13)
            .made_by(alice_id)
            .cells(BTreeMap::from([
                ("chat_id".to_owned(), Value::Uuid(row(0x11).0)),
                ("body".to_owned(), v("bob message")),
                ("author_id".to_owned(), v(bob_user_id.clone())),
                ("owner_id".to_owned(), v(bob_user_id)),
            ])),
    );
    core.apply_fate_update(bob_message_tx, Fate::Accepted, None, Some(DurabilityTier::Edge))
        .unwrap();

    let mut bob = PeerState::edge_client(bob_id);
    let chat_shape = Query::from("chats")
        .validate(&core.catalogue.schema)
        .unwrap();
    let chat_binding = chat_shape.bind(BTreeMap::new()).unwrap();
    let message_shape = Query::from("messages")
        .validate(&core.catalogue.schema)
        .unwrap();
    let message_binding = message_shape.bind(BTreeMap::new()).unwrap();

    let chat_update = bob
        .rehydrate_query_with_opts(
            &mut core,
            &chat_shape,
            &chat_binding,
            RegisterShapeOptions {
                tier: DurabilityTier::Edge,
                ..RegisterShapeOptions::default()
            },
        )
        .unwrap();
    assert_view_update_only_references_rows(&chat_update, BTreeSet::from([row(0x11)]));
    assert_view_update_only_ships_rows(&chat_update, BTreeSet::from([row(0x11)]));

    let message_update = bob
        .rehydrate_query_with_opts(
            &mut core,
            &message_shape,
            &message_binding,
            RegisterShapeOptions {
                tier: DurabilityTier::Edge,
                ..RegisterShapeOptions::default()
            },
        )
        .unwrap();
    assert_view_update_only_references_rows(&message_update, BTreeSet::from([row(0x21)]));
    assert_view_update_only_ships_rows(&message_update, BTreeSet::from([row(0x21)]));
}

#[test]
fn edge_query_rehydrate_ships_public_chat_from_chat_policy_schema() {
    let schema = JazzSchema::new([
        TableSchema::new(
            "chats",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("visibility", ColumnType::String),
            ],
        )
        .with_read_policy(Policy::shape(
            Query::from("chats")
                .filter(eq(lit(true), lit(false)))
                .policy_branch(PolicyBranch::from_query(
                    Query::from("chats").filter(eq(col("visibility"), lit("public"))),
                ))
                .policy_branch(PolicyBranch::from_query(Query::from("chats").join_via_column(
                    "chat_members",
                    "chat_id",
                    "id",
                    [eq(col("user_id"), claim("user_id"))],
                ))),
        ))
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "chat_members",
            [
                ColumnSchema::new("chat_id", ColumnType::Uuid),
                ColumnSchema::new("user_id", ColumnType::String),
            ],
        )
        .with_reference("chat_id", "chats")
        .with_write_policy(Policy::public()),
    ]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let alice = user(0xa1);
    let bob = user(0xb2);
    let public_chat = row(0x11);
    let chat_tx = core
        .commit_mergeable(
            MergeableCommit::new("chats", public_chat, 10)
                .made_by(alice)
                .cells(BTreeMap::from([
                    ("title".to_owned(), v("public")),
                    ("visibility".to_owned(), v("public")),
                ])),
        )
        .unwrap();
    core.apply_fate_update(chat_tx, Fate::Accepted, None, Some(DurabilityTier::Edge))
        .unwrap();

    let shape = Query::from("chats")
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut bob_peer = PeerState::edge_client(bob);

    let update = bob_peer
        .rehydrate_query_with_opts(
            &mut core,
            &shape,
            &binding,
            RegisterShapeOptions {
                tier: DurabilityTier::Edge,
                ..RegisterShapeOptions::default()
            },
        )
        .unwrap();

    assert_view_update_only_references_rows(&update, BTreeSet::from([public_chat]));
    assert_view_update_only_ships_rows(&update, BTreeSet::from([public_chat]));
}

#[test]
fn nullable_join_code_claim_branch_allows_edge_chat_read() {
    let schema = JazzSchema::new([TableSchema::new(
        "chats",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("joinCode", ColumnType::String.nullable()),
        ],
    )
    .with_read_policy(Policy::shape(
        Query::from("chats")
            .filter(eq(lit(true), lit(false)))
            .policy_branch(PolicyBranch::from_query(Query::from("chats").filter(any_of([
                eq(col("joinCode"), claim("join_code")),
            ])))),
    ))
    .with_write_policy(Policy::public())]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let alice = user(0xa1);
    let reader = user(0xb2);
    let chat = row(0x31);
    let join_code = "jazz-join-123";
    let tx = core
        .commit_mergeable(
            MergeableCommit::new("chats", chat, 10)
                .made_by(alice)
                .cells(BTreeMap::from([
                    ("title".to_owned(), v("private by join code")),
                    (
                        "joinCode".to_owned(),
                        Value::Nullable(Some(Box::new(v(join_code)))),
                    ),
                ])),
        )
        .unwrap();
    core.apply_fate_update(tx, Fate::Accepted, None, Some(DurabilityTier::Edge))
        .unwrap();
    core.set_session_claims(
        reader,
        BTreeMap::from([("join_code".to_owned(), v(join_code))]),
    );

    let shape = Query::from("chats")
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    assert_eq!(
        core.query_rows_for_link(&shape, &binding, DurabilityTier::Edge, reader)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([chat])
    );

    let mut reader_peer = PeerState::edge_client(reader);
    let update = reader_peer
        .rehydrate_query_with_opts(
            &mut core,
            &shape,
            &binding,
            RegisterShapeOptions {
                tier: DurabilityTier::Edge,
                ..RegisterShapeOptions::default()
            },
        )
        .unwrap();

    assert_view_update_only_references_rows(&update, BTreeSet::from([chat]));
    assert_view_update_only_ships_rows(&update, BTreeSet::from([chat]));
}

#[test]
fn edge_query_rehydrate_resets_empty_result_for_denied_private_chat() {
    let schema = JazzSchema::new([
        TableSchema::new(
            "chats",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("visibility", ColumnType::String),
                ColumnSchema::new("owner_id", ColumnType::String),
            ],
        )
        .with_read_policy(Policy::shape(
            Query::from("chats")
                .filter(eq(col("visibility"), lit("public")))
                .policy_branch(PolicyBranch::from_query(
                    Query::from("chats").filter(eq(col("owner_id"), claim("user_id"))),
                )),
        ))
        .with_write_policy(Policy::public()),
    ]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let alice = user(0xa1);
    let bob = user(0xb2);
    let private_chat = row(0x12);
    let tx = core
        .commit_mergeable(
            MergeableCommit::new("chats", private_chat, 10)
                .made_by(alice)
                .cells(BTreeMap::from([
                    ("title".to_owned(), v("private")),
                    ("visibility".to_owned(), v("private")),
                    ("owner_id".to_owned(), v(alice.0.to_string())),
                ])),
        )
        .unwrap();
    core.apply_fate_update(tx, Fate::Accepted, None, Some(DurabilityTier::Edge))
        .unwrap();

    let shape = Query::from("chats")
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut bob_peer = PeerState::edge_client(bob);

    let update = bob_peer
        .rehydrate_query_with_opts(
            &mut core,
            &shape,
            &binding,
            RegisterShapeOptions {
                tier: DurabilityTier::Edge,
                ..RegisterShapeOptions::default()
            },
        )
        .unwrap();

    let SyncMessage::ViewUpdate {
        reset_result_set,
        result_member_adds,
        version_bundles,
        ..
    } = update
    else {
        panic!("expected view update");
    };
    assert!(reset_result_set);
    assert!(result_member_adds.is_empty());
    assert!(version_bundles.is_empty());
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
        result_member_adds,
        result_member_removes,
        ..
    } = update
    else {
        panic!("expected view update");
    };
    let mut adds = result_member_adds
        .iter()
        .filter_map(crate::protocol::ResultMemberEntry::as_row)
        .collect::<Vec<_>>();
    let mut removes = result_member_removes
        .iter()
        .filter_map(crate::protocol::ResultMemberEntry::as_row)
        .collect::<Vec<_>>();
    adds.sort();
    removes.sort();
    (adds, removes)
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
fn maintained_subscription_view_multi_segment_inner_include_payload_references_visible_path() {
    let schema = multi_segment_required_include_rls_schema();
    let (_full_recompute_dir, mut full_recompute_core) =
        open_node_with_schema(node(9), schema.clone());
    let (_maintained_dir, mut maintained_core) = open_node_with_schema(node(9), schema);
    let reader = user(0xa1);
    seed_multi_segment_include_fixture(&mut full_recompute_core, reader);
    seed_multi_segment_include_fixture(&mut maintained_core, reader);
    let shape = required_include_shape(&maintained_core, Include::new("project.org"));
    let binding = shape.bind(BTreeMap::new()).unwrap();

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
        result_member_adds, ..
    } = update
    else {
        panic!("expected view update");
    };
    assert_eq!(
        result_member_adds
            .into_iter()
            .filter_map(crate::protocol::ResultMemberEntry::into_row)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([("roots".to_owned().into(), row(0xd2), update_tx)])
    );
}

#[test]
fn maintained_inner_multi_segment_include_payload_references_visible_path_only() {
    let schema = multi_segment_required_include_rls_schema();
    let (_maintained_dir, mut maintained_core) = open_node_with_schema(node(9), schema);
    let reader = user(0xa1);
    seed_multi_segment_include_fixture(&mut maintained_core, reader);
    let shape = required_include_shape(&maintained_core, Include::new("project.org"));
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let mut maintained_peer = PeerState::for_author(reader);

    let maintained = maintained_peer
        .rehydrate_query(&mut maintained_core, &shape, &binding)
        .unwrap();

    let (adds, removes) = canonical_view_update_rows(&maintained);
    assert!(removes.is_empty());
    assert_eq!(
        adds.into_iter()
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
            read_view: Default::default(),
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
fn maintained_subscription_view_multi_segment_holes_include_payload_references_visible_paths() {
    let schema = multi_segment_required_include_rls_schema();
    let (_maintained_dir, mut maintained_core) = open_node_with_schema(node(9), schema);
    let reader = user(0xa1);
    seed_multi_segment_include_fixture(&mut maintained_core, reader);
    let shape = required_include_shape(
        &maintained_core,
        Include::new("project.org").join_mode(JoinMode::Holes),
    );
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let mut maintained_peer = PeerState::for_author(reader);

    let maintained = maintained_peer
        .rehydrate_query(&mut maintained_core, &shape, &binding)
        .unwrap();
    let (adds, removes) = canonical_view_update_rows(&maintained);
    assert!(removes.is_empty());
    assert_eq!(
        adds.into_iter()
            .filter(|entry| entry.0.as_str() == "roots")
            .map(|entry| entry.1)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd1), row(0xd2), row(0xd3)])
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
fn holes_include_keeps_parent_without_root_membership_filtering() {
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
            read_view: Default::default(),
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
    // TODO(query-engine): keep this as an explicit raw-fact oracle until
    // maintained query-engine output exposes policy-readable version streams
    // through a public or debug fact surface.
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
fn maintained_view_seeded_query_engine_snapshot_matches_rows_and_witnesses() {
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
    let deleted_readable_delete = accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0x93), 21)
            .parents(vec![deleted_readable_content])
            .deletion(DeletionEvent::Deleted),
    );
    let deleted_unreadable_content = accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0x94), 22).cells(owner_cells(author_b, "hidden delete")),
    );
    let deleted_unreadable_delete = accept_global(
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

    assert_query_engine_maintained_seed_matches_public_rows_and_witnesses(
        &mut core,
        &shape,
        &binding,
        AuthorId::SYSTEM,
        [
            (sibling_tx, row(0x90), VersionLayer::Content),
            (sibling_tx, row(0x91), VersionLayer::Content),
            (deleted_readable_delete, row(0x93), VersionLayer::Deletion),
            (deleted_unreadable_delete, row(0x94), VersionLayer::Deletion),
        ],
        [
            (row(0x93), VersionLayer::Content, false),
            (row(0x93), VersionLayer::Deletion, true),
            (row(0x94), VersionLayer::Content, false),
            (row(0x94), VersionLayer::Deletion, true),
        ],
    );
    assert_query_engine_maintained_seed_matches_public_rows_and_witnesses(
        &mut core,
        &shape,
        &binding,
        author_a,
        [
            (sibling_tx, row(0x90), VersionLayer::Content),
            (deleted_readable_delete, row(0x93), VersionLayer::Deletion),
        ],
        [
            (row(0x93), VersionLayer::Content, false),
            (row(0x93), VersionLayer::Deletion, true),
            (row(0x94), VersionLayer::Content, false),
            (row(0x94), VersionLayer::Deletion, true),
        ],
    );
}

#[test]
fn maintained_view_query_engine_seed_clean_owner_policy_claim_params_match_one_shot() {
    let schema = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::owner_only("todos", "owner"))]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let author = user(0xa1);
    let other = user(0xb2);

    accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0xa0), 10).cells(owner_cells(author, "owned")),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0xb0), 11).cells(owner_cells(other, "hidden")),
    );

    let shape = Query::from("todos")
        .filter(eq(col("title"), param("title")))
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([(
            "title".to_owned(),
            Value::String("owned".to_owned()),
        )]))
        .unwrap();
    let mut peer = PeerState::for_author(author);
    let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    let (adds, removes) = canonical_view_update_rows(&update);
    assert_eq!(
        adds.into_iter()
            .map(|(_table, row_uuid, _tx_id)| row_uuid)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xa0)]),
        "query-engine maintained rows should route by retained query and policy claim params"
    );
    assert!(removes.is_empty());
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
    let mut peer = PeerState::new();
    let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    let (adds, removes) = canonical_view_update_rows(&update);
    assert_eq!(
        adds.into_iter()
            .map(|(_table, row_uuid, tx_id)| (row_uuid, tx_id))
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([(row(0xa0), tx_a), (row(0xa1), tx_b)])
    );
    assert!(removes.is_empty());
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
    let mut peer = PeerState::for_author(user(0xa1));
    peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
}

#[test]
fn maintained_view_retained_claim_param_equality_matches_literal_recompute() {
    let schema = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::owner_only("todos", "owner"))]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let author = user(0xa1);
    let other = user(0xb2);

    accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0xa0), 10).cells(owner_cells(author, "owned")),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0xb0), 11).cells(owner_cells(other, "other")),
    );

    let retained_shape = Query::from("todos")
        .validate(&core.catalogue.schema)
        .unwrap();
    let retained_binding = retained_shape.bind(BTreeMap::new()).unwrap();
    let expected_rows = BTreeSet::from([row(0xa0)]);

    let (prepared_shape, prepared_binding, prepared_plan) = core
        .prepare_query_binding_for_link(
            &retained_shape,
            &retained_binding,
            DurabilityTier::Global,
            author,
        )
        .unwrap();
    let prepared_rows = core
        .query_rows_with_prepared_plan_for_identity(
            &prepared_shape,
            &prepared_binding,
            DurabilityTier::Global,
            Some(&prepared_plan),
            author,
        )
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();
    assert_eq!(prepared_rows, expected_rows);

    let mut peer = PeerState::for_author(author);
    let update = peer
        .rehydrate_query(&mut core, &retained_shape, &retained_binding)
        .unwrap();
    let (adds, removes) = canonical_view_update_rows(&update);
    assert_eq!(
        adds.into_iter()
            .map(|(_table, row_uuid, _tx_id)| row_uuid)
            .collect::<BTreeSet<_>>(),
        expected_rows
    );
    assert!(removes.is_empty());
}

#[test]
fn maintained_view_join_policy_retained_claim_param_matches_query_engine_result() {
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
    let author = user(0xa1);
    let other = user(0xb2);

    accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0xa0), 10).cells(owner_cells(author, "owned")),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("todos", row(0xb0), 11).cells(owner_cells(other, "other")),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("members", row(0xa1), 12).cells(BTreeMap::from([
            ("owner".to_owned(), Value::Uuid(row(0xa0).0)),
            ("user".to_owned(), Value::Uuid(author.0)),
        ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("members", row(0xb1), 13).cells(BTreeMap::from([
            ("owner".to_owned(), Value::Uuid(row(0xb0).0)),
            ("user".to_owned(), Value::Uuid(other.0)),
        ])),
    );

    let shape = Query::from("todos")
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let full_recompute_rows = core
        .query_rows_for_link(&shape, &binding, DurabilityTier::Global, author)
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();
    assert_eq!(full_recompute_rows, BTreeSet::from([row(0xa0)]));

    let mut peer = PeerState::for_author(author);
    let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    let (adds, removes) = canonical_view_update_rows(&update);
    assert_eq!(
        adds.into_iter()
            .map(|(_table, row_uuid, _tx_id)| row_uuid)
            .collect::<BTreeSet<_>>(),
        full_recompute_rows
    );
    assert!(removes.is_empty());
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

    let mut peer = PeerState::for_author(reader);
    let initial = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    assert_eq!(
        canonical_view_update_rows(&initial),
        (Vec::new(), Vec::new())
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

    let mut peer = PeerState::new();
    let initial = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    assert_view_update_rows(
        initial,
        [("todos", second, second_tx), ("todos", third, third_tx)],
        [],
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
    assert_eq!(metrics.unsupported_skips_out, 0);
    assert_eq!(metrics.hits_out, 4);
}

#[test]
fn maintained_subscription_view_rehydrates_reference_bearing_root_table() {
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
    let (_ref_dir, mut ref_core) = open_node_with_schema(node(9), ref_schema);
    let shape = Query::from("todos")
        .validate(&ref_core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut ref_peer = PeerState::for_author(user(0xa1));
    ref_peer
        .rehydrate_query(&mut ref_core, &shape, &binding)
        .unwrap();
    let ref_metrics = ref_peer.maintained_subscription_view_metrics();
    assert_eq!(ref_metrics.unsupported_skips_out, 0);
    assert_eq!(ref_metrics.hits_out, 1);

    // Control: the same query on a table with no references is supported.
    let plain_schema = JazzSchema::new([TableSchema::new(
        "todos",
        [ColumnSchema::new("title", ColumnType::String)],
    )]);
    let (_plain_dir, mut plain_core) = open_node_with_schema(node(9), plain_schema);
    let plain_shape = Query::from("todos")
        .validate(&plain_core.catalogue.schema)
        .unwrap();
    let plain_binding = plain_shape.bind(BTreeMap::new()).unwrap();
    let mut plain_peer = PeerState::for_author(user(0xa1));
    plain_peer
        .rehydrate_query(&mut plain_core, &plain_shape, &plain_binding)
        .unwrap();
    let plain_metrics = plain_peer.maintained_subscription_view_metrics();
    assert_eq!(plain_metrics.unsupported_skips_out, 0);
    assert_eq!(plain_metrics.hits_out, 1);
}

#[test]
fn maintained_subscription_view_explicit_include_suppresses_other_implicit_references() {
    let schema = JazzSchema::new([
        TableSchema::new(
            "roots",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("primary", ColumnType::Uuid),
                ColumnSchema::new("secondary", ColumnType::Uuid),
            ],
        )
        .with_reference("primary", "targets")
        .with_reference("secondary", "targets"),
        TableSchema::new("targets", [ColumnSchema::new("name", ColumnType::String)]),
    ]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let included = row(0x11);
    let excluded = row(0x22);
    let root = row(0x33);
    accept_global(
        &mut core,
        MergeableCommit::new("targets", included, 10)
            .cells(BTreeMap::from([("name".to_owned(), v("included"))])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("targets", excluded, 11)
            .cells(BTreeMap::from([("name".to_owned(), v("excluded"))])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("roots", root, 12).cells(BTreeMap::from([
            ("title".to_owned(), v("root")),
            ("primary".to_owned(), Value::Uuid(included.0)),
            ("secondary".to_owned(), Value::Uuid(excluded.0)),
        ])),
    );

    let shape = Query::from("roots")
        .include("primary")
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut peer = PeerState::for_author(user(0xa1));
    let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();

    assert_view_update_only_ships_rows(&update, BTreeSet::from([root, included]));
    let metrics = peer.maintained_subscription_view_metrics();
    assert_eq!(metrics.unsupported_skips_out, 0);
}

#[test]
fn retained_user_param_filter_graph_matches_literal_filter() {
    let schema = JazzSchema::new([TableSchema::new(
        "docs",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::shape(
        Query::from("docs").filter(eq(col("owner"), claim("sub"))),
    ))
    .with_write_policy(Policy::public())]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let owner = user(0xa1);
    core.set_session_claims(owner, BTreeMap::from([("sub".to_owned(), Value::Uuid(owner.0))]));
    accept_global(
        &mut core,
        MergeableCommit::new("docs", row(0xd1), 10).cells(BTreeMap::from([
            ("title".to_owned(), v("owned")),
            ("owner".to_owned(), Value::Uuid(owner.0)),
        ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("docs", row(0xd2), 11).cells(BTreeMap::from([
            ("title".to_owned(), v("other")),
            ("owner".to_owned(), Value::Uuid(user(0xb2).0)),
        ])),
    );

    let shape = Query::from("docs")
        .filter(eq(col("owner"), param("owner")))
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([("owner".to_owned(), Value::Uuid(owner.0))]))
        .unwrap();
    let (shape, binding, plan) = core
        .prepare_query_binding_for_link(&shape, &binding, DurabilityTier::Global, owner)
        .unwrap();
    let rows = core
        .query_rows_with_prepared_plan_for_identity(
            &shape,
            &binding,
            DurabilityTier::Global,
            Some(&plan),
            owner,
        )
        .unwrap();

    assert_eq!(
        rows.into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0xd1)])
    );
    assert!(shape.params().contains_key("owner"));
    assert!(binding.values().contains_key("owner"));
}

#[test]
fn retained_param_used_as_filter_and_reachable_seed_matches_literal_query() {
    let (_core_dir, mut core) = open_node_with_schema(node(9), recursive_reachable_schema());
    seed_recursive_reachable_fixture(&mut core);
    let shape = Query::from("docs")
        .reachable_via_with_access_filters(
            "teamAccess",
            "doc",
            "team",
            param("team"),
            [eq(col("team"), param("team"))],
            "teamEdges",
            "member",
            "parent",
            [],
        )
        .validate(&core.catalogue.schema)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([("team".to_owned(), Value::Uuid(team(1)))]))
        .unwrap();

    let (prepared_shape, prepared_binding, prepared_plan) = core
        .prepare_query_binding_for_link(&shape, &binding, DurabilityTier::Global, user(0xa1))
        .unwrap();
    let prepared_rows = core
        .query_rows_with_prepared_plan_for_identity(
            &prepared_shape,
            &prepared_binding,
            DurabilityTier::Global,
            Some(&prepared_plan),
            user(0xa1),
        )
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();

    assert_eq!(prepared_rows, BTreeSet::from([row(0xd1)]));
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
        result_member_adds,
        result_member_removes,
        ..
    } = update
    else {
        panic!("expected view update");
    };
    let mut result_member_adds = result_member_adds;
    let mut result_member_removes = result_member_removes;
    result_member_adds.sort();
    result_member_removes.sort();
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
    assert_eq!(result_member_adds, expected_adds);
    assert_eq!(result_member_removes, expected_removes);
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

fn seed_recursive_reachable_fixture(core: &mut NodeState<RocksDbStorage>) {
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
    accept_global(core, edge_commit(0xe2, 2, 3, 11));
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

fn assert_query_engine_maintained_seed_matches_public_rows_and_witnesses(
    core: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    binding: &Binding,
    identity: AuthorId,
    expected_witnesses: impl IntoIterator<Item = (TxId, RowUuid, VersionLayer)>,
    expected_replacements: impl IntoIterator<Item = (RowUuid, VersionLayer, bool)>,
) {
    let expected_rows = core
        .query_rows_for_link(shape, binding, DurabilityTier::Global, identity)
        .unwrap();
    let (receiver, maintained, _terminal_schemas, transitions, _tables) = core
        .open_seeded_maintained_subscription_view(
            shape,
            binding,
            identity,
            DurabilityTier::Global,
        )
        .unwrap();
    core.unsubscribe_groove_subscription(receiver.id());

    assert_eq!(
        transitions
            .adds
            .iter()
            .filter_map(crate::protocol::ResultMemberEntry::as_row)
            .map(|(table, row_uuid, _tx_id)| (table.to_string(), row_uuid))
            .collect::<BTreeSet<_>>(),
        expected_rows
            .iter()
            .map(|row| (row.table().to_owned(), row.row_uuid()))
            .collect::<BTreeSet<_>>(),
        "seeded query-engine maintained membership must match public rows"
    );
    assert!(transitions.removes.is_empty());

    for (tx_id, row_uuid, layer) in expected_witnesses {
        assert!(
            maintained
                .versions_by_tx(tx_id)
                .iter()
                .any(|version| version.row_uuid() == row_uuid && version.layer() == layer),
            "seeded query-engine maintained view must include expected {layer:?} witness for {row_uuid:?} in {tx_id:?}"
        );
    }
    for (row_uuid, layer, should_exist) in expected_replacements {
        let (content, deletion) = maintained.replacement_for("todos", row_uuid);
        let actual = match layer {
            VersionLayer::Content => content,
            VersionLayer::Deletion => deletion,
        };
        assert_eq!(
            actual.is_some(),
            should_exist,
            "seeded query-engine maintained replacement witness presence for {row_uuid:?}/{layer:?}"
        );
    }
}

fn assert_maintained_view_cold_snapshot_seed_matches_one_shot(
    core: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    binding: &Binding,
    identity: AuthorId,
) {
    let expected_rows = core
        .query_rows_for_link(shape, binding, DurabilityTier::Global, identity)
        .unwrap()
        .into_iter()
        .map(|row| (groove::Intern::new(row.table().to_owned()), row.row_uuid()))
        .collect::<BTreeSet<_>>();
    let mut peer = if identity == AuthorId::SYSTEM {
        PeerState::new()
    } else {
        PeerState::for_author(identity)
    };
    let update = peer.rehydrate_query(core, shape, binding).unwrap();
    let (adds, removes) = canonical_view_update_rows(&update);

    assert_eq!(
        adds.into_iter()
            .map(|(table, row_uuid, _tx_id)| (table, row_uuid))
            .collect::<BTreeSet<_>>(),
        expected_rows,
        "maintained subscription cold snapshot should match public query rows"
    );
    assert!(removes.is_empty());
    let metrics = peer.maintained_subscription_view_metrics();
    assert_eq!(metrics.hits_out, 1);
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

#[test]
fn nullable_claim_equality_policy_branch_allows_matching_row() {
    let reader = user(0xa1);
    let schema = JazzSchema::new([
        TableSchema::new(
            "chats",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("joinCode", ColumnType::String.nullable()),
            ],
        )
        .with_read_policy(Policy::shape(
            Query::from("chats")
                .filter(Predicate::Any(Vec::new()))
                .policy_branch(PolicyBranch::from_query(
                    Query::from("chats").filter(eq(col("joinCode"), claim("join_code"))),
                )),
        ))
        .with_write_policy(Policy::public()),
    ]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let matching = row(0x91);
    let other = row(0x92);
    let tx_matching = core
        .commit_mergeable(
            MergeableCommit::new("chats", matching, 10).cells(BTreeMap::from([
                ("title".to_owned(), Value::String("matching".to_owned())),
                (
                    "joinCode".to_owned(),
                    Value::Nullable(Some(Box::new(Value::String("secret-123".to_owned())))),
                ),
            ])),
        )
        .unwrap();
    core.apply_fate_update(
        tx_matching,
        Fate::Accepted,
        Some(GlobalSeq(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let tx_other = core
        .commit_mergeable(
            MergeableCommit::new("chats", other, 11).cells(BTreeMap::from([
                ("title".to_owned(), Value::String("other".to_owned())),
                (
                    "joinCode".to_owned(),
                    Value::Nullable(Some(Box::new(Value::String("wrong".to_owned())))),
                ),
            ])),
        )
        .unwrap();
    core.apply_fate_update(
        tx_other,
        Fate::Accepted,
        Some(GlobalSeq(2)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    core.set_session_claims(
        reader,
        BTreeMap::from([(
            "join_code".to_owned(),
            Value::String("secret-123".to_owned()),
        )]),
    );
    let mut edge = PeerState::edge_client(reader);

    assert_view_update_only_references_rows(
        &edge.current_rows_update(&mut core, "chats").unwrap(),
        BTreeSet::from([matching]),
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
fn reverse_referencing_select_policy_allows_root_row_through_source_row() {
    let schema = JazzSchema::new([
        TableSchema::new("files", [ColumnSchema::new("name", ColumnType::String)])
            .with_read_policy(Policy::shape(Query::from("files").join_via(
                "attachments",
                "fileId",
                [eq(col("ownerId"), claim("user_id"))],
            )))
            .with_write_policy(Policy::public()),
        TableSchema::new(
            "attachments",
            [
                ColumnSchema::new("fileId", ColumnType::Uuid),
                ColumnSchema::new("ownerId", ColumnType::String),
            ],
        )
        .with_reference("fileId", "files")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
    ]);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let alice = user(0xa1);
    let bob = user(0xb2);
    let alice_file = row(0xf1);
    let unlinked_file = row(0xf2);

    for (file, name) in [(alice_file, "alice"), (unlinked_file, "unlinked")] {
        accept_global(
            &mut core,
            MergeableCommit::new("files", file, 10).cells(BTreeMap::from([(
                "name".to_owned(),
                Value::String(name.to_owned()),
            )])),
        );
    }
    accept_global(
        &mut core,
        MergeableCommit::new("attachments", row(0xa7), 20).cells(BTreeMap::from([
            ("fileId".to_owned(), Value::Uuid(alice_file.0)),
            ("ownerId".to_owned(), Value::String(alice.0.to_string())),
        ])),
    );

    assert!(
        core.dry_run_read_current_allows("files", alice_file, alice)
            .unwrap()
    );
    assert!(
        !core
            .dry_run_read_current_allows("files", alice_file, bob)
            .unwrap()
    );
    assert!(
        !core
            .dry_run_read_current_allows("files", unlinked_file, alice)
            .unwrap()
    );
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
