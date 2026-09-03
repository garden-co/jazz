//! Structured and flat maintained-result delta behavior.

use super::*;

// Internal regression: a new public one-shot query would open another receiver
// and hide stale decoded state in the existing stream's snapshot cache.
#[test]
fn structured_live_snapshot_keeps_child_edits_across_parent_reordering() {
    fn retained_snapshot(subscription: &SubscriptionStream) -> RelationSnapshot {
        let state = subscription._state.borrow();
        super::super::super::materialized_subscription_snapshot(
            &state.snapshot,
            &state.snapshot_index,
        )
        .unwrap()
    }
    let schema = relation_schema();
    let db = open_db(0xcf, AuthorSubject::for_test_bytes([0xcf; 16]), &schema);
    for (id, name) in [(0xa1, "alice"), (0xb1, "bob")] {
        db.insert(
            "users",
            BTreeMap::from([("name".to_owned(), Value::String(name.to_owned()))]),
            crate::db::InsertOptions {
                row_id: Some(row(id)),
                ..Default::default()
            },
        )
        .unwrap();
    }
    for (id, owner, title) in [(0x11, 0xa1, "first"), (0x21, 0xb1, "untouched")] {
        db.insert(
            "todos",
            BTreeMap::from([
                ("title".to_owned(), Value::String(title.to_owned())),
                ("owner_id".to_owned(), Value::Uuid(row(owner).0)),
            ]),
            crate::db::InsertOptions {
                row_id: Some(row(id)),
                ..Default::default()
            },
        )
        .unwrap();
    }
    let query = Query::from("users")
        .order_by("name", OrderDirection::Asc)
        .array_subquery(ArraySubquery::new(
            "todosViaOwner",
            "todos",
            "owner_id",
            "id",
        ));
    let prepared_query = prepared(&db, &query);
    let mut subscription = block_on(db.subscribe(
        &prepared_query,
        ReadOpts {
            tier: DurabilityTier::Local,
            ..Default::default()
        },
    ))
    .unwrap();
    block_on(subscription.next_raw()).unwrap();

    db.update(
        "todos",
        row(0x11),
        BTreeMap::from([("title".to_owned(), Value::String("edited".to_owned()))]),
        Default::default(),
    )
    .unwrap();
    block_on(subscription.next_raw()).unwrap();
    let snapshot = retained_snapshot(&subscription);
    assert_eq!(
        terminal_nested_text_values(&snapshot, row(0xa1), "todosViaOwner", "title"),
        vec!["edited"]
    );
    assert_eq!(
        terminal_nested_text_values(&snapshot, row(0xb1), "todosViaOwner", "title"),
        vec!["untouched"]
    );

    db.update(
        "users",
        row(0xa1),
        BTreeMap::from([("name".to_owned(), Value::String("zulu".to_owned()))]),
        Default::default(),
    )
    .unwrap();
    block_on(subscription.next_raw()).unwrap();
    let snapshot = retained_snapshot(&subscription);
    assert_eq!(row_ids(&snapshot.rows), vec![row(0xb1), row(0xa1)]);
    assert_eq!(
        terminal_nested_text_values(&snapshot, row(0xa1), "todosViaOwner", "title"),
        vec!["edited"]
    );

    db.update(
        "todos",
        row(0x11),
        BTreeMap::from([("title".to_owned(), Value::String("after move".to_owned()))]),
        Default::default(),
    )
    .unwrap();
    block_on(subscription.next_raw()).unwrap();
    let snapshot = retained_snapshot(&subscription);
    assert_eq!(
        terminal_nested_text_values(&snapshot, row(0xa1), "todosViaOwner", "title"),
        vec!["after move"]
    );

    db.delete("todos", row(0x11), Default::default()).unwrap();
    block_on(subscription.next_raw()).unwrap();
    let snapshot = retained_snapshot(&subscription);
    assert!(terminal_nested_text_values(&snapshot, row(0xa1), "todosViaOwner", "title").is_empty());
    assert_eq!(
        terminal_nested_text_values(&snapshot, row(0xb1), "todosViaOwner", "title"),
        vec!["untouched"]
    );

    db.delete("users", row(0xa1), Default::default()).unwrap();
    block_on(subscription.next_raw()).unwrap();
    let snapshot = retained_snapshot(&subscription);
    assert_eq!(row_ids(&snapshot.rows), vec![row(0xb1)]);
    assert_eq!(
        terminal_nested_text_values(&snapshot, row(0xb1), "todosViaOwner", "title"),
        vec!["untouched"]
    );
}

#[test]
fn array_subquery_live_subscription_publishes_only_terminal_root_rows() {
    let schema = relation_schema();
    let db = open_db(0xc1, AuthorSubject::for_test_bytes([0xc1; 16]), &schema);
    db.insert(
        "users",
        BTreeMap::from([("name".to_owned(), Value::String("alice".to_owned()))]),
        crate::db::InsertOptions {
            row_id: Some(row(0xa1)),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "users",
        BTreeMap::from([("name".to_owned(), Value::String("bob".to_owned()))]),
        crate::db::InsertOptions {
            row_id: Some(row(0xb1)),
            ..Default::default()
        },
    )
    .unwrap();

    let query = Query::from("users")
        .filter(eq(col("id"), lit(Value::Uuid(row(0xa1).0))))
        .array_subquery(ArraySubquery::new(
            "todosViaOwner",
            "todos",
            "owner_id",
            "id",
        ));
    let prepared_query = prepared(&db, &query);
    let mut subscription = block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    assert_eq!(db.active_groove_subscriptions_for_test(), 1);

    let opened = block_on(subscription.next_raw()).unwrap();
    let SubscriptionEvent::Delta { .. } = &opened else {
        panic!("expected terminal reset")
    };
    let snapshot = snapshot_from_event(opened);
    assert_eq!(
        terminal_nested_text_values(&snapshot, row(0xa1), "todosViaOwner", "title"),
        Vec::<String>::new(),
        "an empty nested collection is encoded in the surviving root"
    );
    assert!(snapshot.edges.is_empty());

    db.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("first".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
        ]),
        crate::db::InsertOptions {
            row_id: Some(row(0x11)),
            ..Default::default()
        },
    )
    .unwrap();
    db.tick().unwrap();
    let mut child_added = block_on(subscription.next_raw()).unwrap();
    while let Some(next) = subscription.try_next_event() {
        child_added = next;
    }
    let SubscriptionEvent::Delta {
        reset,
        added,
        updated,
        removed,
        terminal_operations,
        ..
    } = &child_added
    else {
        panic!("expected root replacement")
    };
    assert!(!*reset, "a child insertion must remain incremental");
    assert!(added.is_empty());
    assert!(
        updated.is_empty(),
        "a descendant patch does not replace its root"
    );
    assert!(removed.is_empty());
    assert!(
        terminal_operations
            .iter()
            .any(|operation| matches!(operation.edit, groove::ivm::TerminalEdit::Insert { .. })),
        "child insertion is delivered as a terminal path insert"
    );

    db.update(
        "todos",
        row(0x11),
        BTreeMap::from([("owner_id".to_owned(), Value::Uuid(row(0xb1).0))]),
        Default::default(),
    )
    .unwrap();
    let removed_child = block_on(subscription.next_raw()).unwrap();
    assert!(matches!(
        removed_child,
        SubscriptionEvent::Delta { terminal_operations, .. }
            if terminal_operations.iter().any(|operation| matches!(operation.edit, groove::ivm::TerminalEdit::Remove { .. }))
    ));

    db.update(
        "todos",
        row(0x11),
        BTreeMap::from([("owner_id".to_owned(), Value::Uuid(row(0xa1).0))]),
        Default::default(),
    )
    .unwrap();
    let restored_child = block_on(subscription.next_raw()).unwrap();
    assert!(matches!(
        restored_child,
        SubscriptionEvent::Delta { terminal_operations, .. }
            if terminal_operations.iter().any(|operation| matches!(operation.edit, groove::ivm::TerminalEdit::Insert { .. }))
    ));
}

#[test]
fn structured_subscription_splices_in_terminal_root_order_after_insert() {
    let schema = relation_schema();
    let db = open_db(0xc4, AuthorSubject::for_test_bytes([0xc4; 16]), &schema);
    db.insert(
        "users",
        BTreeMap::from([("name".to_owned(), Value::String("zulu".to_owned()))]),
        crate::db::InsertOptions {
            row_id: Some(row(0xa1)),
            ..Default::default()
        },
    )
    .unwrap();

    let query = Query::from("users")
        .order_by("name", OrderDirection::Asc)
        .array_subquery(ArraySubquery::new(
            "todosViaOwner",
            "todos",
            "owner_id",
            "id",
        ));
    let prepared_query = prepared(&db, &query);
    let mut subscription = block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    let initial = block_on(subscription.next_raw()).unwrap();
    let snapshot = snapshot_from_event(initial);
    assert_eq!(row_ids(&snapshot.rows), vec![row(0xa1)]);

    db.insert(
        "users",
        BTreeMap::from([("name".to_owned(), Value::String("alpha".to_owned()))]),
        crate::db::InsertOptions {
            row_id: Some(row(0xb1)),
            ..Default::default()
        },
    )
    .unwrap();
    db.tick().unwrap();
    let reordered = block_on(subscription.next_raw()).unwrap();
    let SubscriptionEvent::Delta {
        reset,
        added,
        updated,
        removed,
        terminal_operations,
        ..
    } = &reordered
    else {
        panic!("expected terminal splice")
    };
    assert!(!*reset, "root reordering must remain incremental");
    assert!(removed.is_empty());
    assert!(updated.is_empty());
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].row.row_uuid(), row(0xb1));
    assert_eq!(added[0].index, 0);
    assert!(terminal_operations.is_empty());

    // A local runtime replacement is a legitimate reset boundary.  The old
    // test injected an unscoped `AuthorityResultKey` into a Local stream,
    // which has no usage-site receipt and therefore cannot name a
    // `CoveredInput` closure under INV-SYNC-36.
    db.invalidate_groove_runtime_for_test();
    assert_eq!(db.refresh_subscriptions().unwrap(), 1);
    let reset = block_on(subscription.next_raw()).unwrap();
    assert!(matches!(
        reset,
        SubscriptionEvent::Delta {
            reset: true,
            terminal_operations,
            ..
        } if terminal_operations.is_empty()
    ));
    assert_eq!(
        db.active_groove_subscriptions_for_test(),
        1,
        "a runtime reset must replace, not leak, its Groove terminal"
    );

    db.update(
        "users",
        row(0xb1),
        BTreeMap::from([("name".to_owned(), Value::String("zzzz".to_owned()))]),
        Default::default(),
    )
    .unwrap();
    db.tick().unwrap();
    let updated = block_on(subscription.next_raw()).unwrap();
    let SubscriptionEvent::Delta {
        reset,
        added,
        updated,
        removed,
        terminal_operations,
        ..
    } = &updated
    else {
        panic!("expected update splice")
    };
    assert!(!*reset);
    assert!(removed.is_empty());
    assert!(added.is_empty());
    assert_eq!(updated.len(), 1);
    assert_eq!(updated[0].row.row_uuid(), row(0xb1));
    assert_eq!(updated[0].previous_index, Some(0));
    assert_eq!(updated[0].index, 1);
    assert!(terminal_operations.is_empty());

    db.delete("users", row(0xa1), Default::default()).unwrap();
    db.tick().unwrap();
    let removed = block_on(subscription.next_raw()).unwrap();
    let SubscriptionEvent::Delta { reset, .. } = &removed else {
        panic!("expected removal splice")
    };
    assert!(!*reset);
    let SubscriptionEvent::Delta {
        removed,
        terminal_operations,
        ..
    } = removed
    else {
        unreachable!()
    };
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].row_uuid, row(0xa1));
    assert_eq!(removed[0].index, 0);
    assert!(terminal_operations.is_empty());
}

/// A normal reader rehydrates a structured message subscription after a
/// separately invite-scoped connection writes its membership.
///
/// Alice's invite-scoped connection ──membership──► server ──coverage──►
/// Alice's normal connection ──structured subscribe──► message + sender.
///
/// This also exercises the bounded-stack peer admission path: the server must
/// process the membership commit without carrying inactive Subscribe-arm state.
#[test]
fn limit_one_subscription_replaces_winner_on_insert_and_retraction() {
    let schema = relation_schema();
    let db = open_db(0xc5, AuthorSubject::for_test_bytes([0xc5; 16]), &schema);
    let prepared_query = prepared(&db, &Query::from("users").limit(1));
    let mut subscription = block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    let mut snapshot = snapshot_from_event(block_on(subscription.next_raw()).unwrap());
    assert!(snapshot.rows.is_empty());

    db.insert(
        "users",
        BTreeMap::from([("name".to_owned(), Value::String("later".to_owned()))]),
        crate::db::InsertOptions {
            row_id: Some(row(0xb1)),
            ..Default::default()
        },
    )
    .unwrap();
    db.tick().unwrap();
    apply_subscription_event(&mut snapshot, block_on(subscription.next_raw()).unwrap());
    assert_eq!(row_ids(&snapshot.rows), [row(0xb1)]);

    db.insert(
        "users",
        BTreeMap::from([("name".to_owned(), Value::String("winner".to_owned()))]),
        crate::db::InsertOptions {
            row_id: Some(row(0xa1)),
            ..Default::default()
        },
    )
    .unwrap();
    db.tick().unwrap();
    let inserted_replacement = block_on(subscription.next_raw()).unwrap();
    assert!(matches!(
        &inserted_replacement,
        SubscriptionEvent::Delta { reset: false, .. }
    ));
    apply_subscription_event(&mut snapshot, inserted_replacement);
    assert_eq!(row_ids(&snapshot.rows), [row(0xa1)]);

    db.delete("users", row(0xa1), Default::default()).unwrap();
    db.tick().unwrap();
    let retracted_replacement = block_on(subscription.next_raw()).unwrap();
    assert!(matches!(
        &retracted_replacement,
        SubscriptionEvent::Delta { reset: false, .. }
    ));
    apply_subscription_event(&mut snapshot, retracted_replacement);
    assert_eq!(row_ids(&snapshot.rows), [row(0xb1)]);
}

#[test]
fn propagated_structured_subscription_rehydrates_after_membership_scoped_one_shot() {
    let schema = membership_scoped_relation_schema();
    let reader = AuthorSubject::for_test_bytes([0xb2; 16]);
    let normal_claims = BTreeMap::from([(
        crate::query::provider_claim_key("sub"),
        Value::String(reader.test_uuid().to_string()),
    )]);
    let invite_claims = BTreeMap::from([
        (
            crate::query::provider_claim_key("sub"),
            Value::String(reader.test_uuid().to_string()),
        ),
        (
            crate::query::provider_claim_key("join_code"),
            Value::String("invite-code".to_owned()),
        ),
    ]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc4, reader, &schema);
    let invite_client = open_db(0xc5, reader, &schema);
    client.set_test_provider_claims(reader, normal_claims.clone());
    invite_client.set_test_provider_claims(reader, invite_claims.clone());
    // The normal connection remains live while a separately scoped invite
    // connection writes its membership. This is the production handoff.
    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber_with_claims(server_transport, reader, normal_claims);
    let (invite_transport, server_invite_transport) = duplex();
    let _invite_upstream = crate::db::block_on(invite_client.connect_upstream(invite_transport));
    let _invite_subscriber =
        server.accept_subscriber_with_claims(server_invite_transport, reader, invite_claims);
    let chat = row(0xc1);
    let sender = row(0xa1);
    let message = row(0xb1);
    server
        .insert_with_id(
            "chats",
            chat,
            BTreeMap::from([
                ("name".to_owned(), Value::String("private".to_owned())),
                ("is_public".to_owned(), Value::Bool(false)),
                ("created_by".to_owned(), Value::String("author".to_owned())),
                (
                    "join_code".to_owned(),
                    Value::Nullable(Some(Box::new(Value::String("invite-code".to_owned())))),
                ),
            ]),
        )
        .unwrap();
    server
        .insert_with_id(
            "profiles",
            sender,
            BTreeMap::from([
                ("user_id".to_owned(), Value::String("alice".to_owned())),
                ("name".to_owned(), Value::String("alice".to_owned())),
                ("avatar".to_owned(), Value::Nullable(None)),
            ]),
        )
        .unwrap();
    // The browser fixture also has another visible profile which is unrelated
    // to the message's sender. Keep that cardinality here: correlated include
    // lowering must not lose the root because a support table has extra rows.
    server
        .insert_with_id(
            "profiles",
            row(0xa2),
            BTreeMap::from([
                ("user_id".to_owned(), Value::String("unrelated".to_owned())),
                ("name".to_owned(), Value::String("unrelated".to_owned())),
                ("avatar".to_owned(), Value::Nullable(None)),
            ]),
        )
        .unwrap();
    server
        .insert_with_id(
            "messages",
            message,
            BTreeMap::from([
                ("chat_id".to_owned(), Value::Uuid(chat.0)),
                ("sender_id".to_owned(), Value::Uuid(sender.0)),
                ("text".to_owned(), Value::String("visible".to_owned())),
                ("created_at".to_owned(), Value::U64(1_700_000_000_000)),
            ]),
        )
        .unwrap();

    let invite_chat_query = prepared(
        &invite_client,
        &Query::from("chats").filter(eq(col("id"), lit(chat.0))),
    );
    let invite_attachment = invite_client
        .attach_query_with_opts(&invite_chat_query, edge_subscribe_opts())
        .unwrap();
    invite_client.tick().unwrap();
    server.tick().unwrap();
    invite_client.tick().unwrap();
    assert!(invite_client.query_attachment_is_covered(&invite_attachment));
    assert_eq!(
        block_on(invite_client.all(&invite_chat_query, edge_subscribe_opts()))
            .unwrap()
            .len(),
        1,
        "the invite-scoped connection can read the private chat before acceptance",
    );
    invite_client.detach_query(invite_attachment);

    // The ordinary session has the same authenticated identity, but not the
    // invite claim. Its view must remain private until the separate invite
    // session commits membership.
    let normal_chat_query = prepared(
        &client,
        &Query::from("chats").filter(eq(col("id"), lit(chat.0))),
    );
    let normal_chat_attachment = client
        .attach_query_with_opts(&normal_chat_query, edge_subscribe_opts())
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(client.query_attachment_is_covered(&normal_chat_attachment));
    assert!(
        block_on(client.all(&normal_chat_query, edge_subscribe_opts()))
            .unwrap()
            .is_empty(),
        "the invite claim must not leak from its connection into Bob's normal session",
    );
    client.detach_query(normal_chat_attachment);

    let accepted_membership = invite_client
        .insert(
            "chat_members",
            BTreeMap::from([
                ("chat_id".to_owned(), Value::Uuid(chat.0)),
                (
                    "user_id".to_owned(),
                    Value::String(reader.test_uuid().to_string()),
                ),
                ("join_code".to_owned(), Value::Nullable(None)),
            ]),
            crate::db::InsertOptions {
                row_id: Some(row(0xc2)),
                ..Default::default()
            },
        )
        .unwrap();
    invite_client.tick().unwrap();
    server.tick().unwrap();
    invite_client.tick().unwrap();
    client.tick().unwrap();
    block_on(accepted_membership.wait(DurabilityTier::Global))
        .expect("the invite connection's membership write must settle");

    let member_query = prepared(
        &client,
        &Query::from("chat_members").filter(eq(col("chat_id"), lit(chat.0))),
    );
    let member_attachment = client
        .attach_query_with_opts(&member_query, edge_subscribe_opts())
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(client.query_attachment_is_covered(&member_attachment));
    assert_eq!(
        block_on(client.all(&member_query, edge_subscribe_opts()))
            .unwrap()
            .len(),
        1,
        "the client first receives its membership through ordinary coverage",
    );
    client.detach_query(member_attachment);
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let plain_message_query = prepared(
        &client,
        &Query::from("messages").filter(eq(col("chat_id"), lit(chat.0))),
    );
    let plain_attachment = client
        .attach_query_with_opts(&plain_message_query, edge_subscribe_opts())
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(client.query_attachment_is_covered(&plain_attachment));
    assert_eq!(
        block_on(client.all(&plain_message_query, edge_subscribe_opts()))
            .unwrap()
            .len(),
        1,
        "the client receives the root before it requests the structured shape",
    );
    client.detach_query(plain_attachment);
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let query = Query::from("messages")
        .filter(eq(col("chat_id"), lit(chat.0)))
        .array_subquery(ArraySubquery::new("sender", "profiles", "id", "sender_id"))
        .order_by("created_at", OrderDirection::Desc)
        .limit(21);
    let prepared_query = prepared(&client, &query);
    let attachment = client
        .attach_query_with_opts(&prepared_query, edge_subscribe_opts())
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(client.query_attachment_is_covered(&attachment));
    assert_eq!(
        block_on(client.all_relation_snapshot(&prepared_query, edge_subscribe_opts()))
            .unwrap()
            .root_count,
        1,
    );
    client.detach_query(attachment);
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let mut subscription =
        block_on(client.subscribe(&prepared_query, edge_subscribe_opts())).unwrap();
    // Client-local subscriptions suppress the provisional empty opening until
    // their authority has supplied a settled result set.
    assert!(subscription.try_next_event().is_none());
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let snapshot = snapshot_from_event(block_on(subscription.next_event()).unwrap());
    assert!(
        snapshot
            .rows
            .iter()
            .any(|row| row.table() == "messages" && row.row_uuid() == message)
    );
}

#[test]
fn flat_subscription_hydrates_in_declared_root_order() {
    let schema = relation_schema();
    let db = open_db(0xd4, AuthorSubject::for_test_bytes([0xd4; 16]), &schema);
    db.insert(
        "users",
        BTreeMap::from([("name".to_owned(), Value::String("zulu".to_owned()))]),
        crate::db::InsertOptions {
            row_id: Some(row(0xa1)),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "users",
        BTreeMap::from([("name".to_owned(), Value::String("alpha".to_owned()))]),
        crate::db::InsertOptions {
            row_id: Some(row(0xb1)),
            ..Default::default()
        },
    )
    .unwrap();

    let query = Query::from("users").order_by("name", OrderDirection::Desc);
    let prepared_query = prepared(&db, &query);
    let mut subscription = block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    let initial = snapshot_from_event(block_on(subscription.next_raw()).unwrap());

    assert_eq!(row_ids(&initial.rows), vec![row(0xa1), row(0xb1)]);
}

#[test]
fn flat_subscription_hydrates_in_default_row_id_order() {
    let schema = relation_schema();
    let db = open_db(0xd7, AuthorSubject::for_test_bytes([0xd7; 16]), &schema);
    for id in [0xb1, 0xa1] {
        db.insert(
            "users",
            BTreeMap::from([("name".to_owned(), Value::String(format!("user-{id}")))]),
            crate::db::InsertOptions {
                row_id: Some(row(id)),
                ..Default::default()
            },
        )
        .unwrap();
    }

    let prepared_query = prepared(&db, &Query::from("users"));
    let mut subscription = block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    let initial = snapshot_from_event(block_on(subscription.next_raw()).unwrap());

    assert_eq!(row_ids(&initial.rows), vec![row(0xa1), row(0xb1)]);
}

#[test]
fn flat_subscription_inserts_at_declared_root_position() {
    let schema = relation_schema();
    let db = open_db(0xd5, AuthorSubject::for_test_bytes([0xd5; 16]), &schema);
    let query = Query::from("users").order_by("name", OrderDirection::Desc);
    let prepared_query = prepared(&db, &query);
    let mut subscription = block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    let _initial = block_on(subscription.next_raw()).unwrap();

    for (id, name) in [(0xa1, "zulu"), (0xb1, "zzzz")] {
        db.insert(
            "users",
            BTreeMap::from([("name".to_owned(), Value::String(name.to_owned()))]),
            crate::db::InsertOptions {
                row_id: Some(row(id)),
                ..Default::default()
            },
        )
        .unwrap();
        db.tick().unwrap();
        let event = block_on(subscription.next_raw()).unwrap();
        if id == 0xb1 {
            let SubscriptionEvent::Delta {
                added,
                updated,
                removed,
                terminal_operations,
                ..
            } = &event
            else {
                panic!("unexpected flat root event: {event:?}");
            };
            assert_eq!(added.len(), 1);
            assert_eq!(added[0].row.row_uuid(), row(0xb1));
            assert_eq!(added[0].index, 0);
            assert!(updated.is_empty());
            assert!(removed.is_empty());
            assert!(terminal_operations.is_empty());
        }
    }

    db.update(
        "users",
        row(0xa1),
        BTreeMap::from([("name".to_owned(), Value::String("yyyy".to_owned()))]),
        Default::default(),
    )
    .unwrap();
    db.tick().unwrap();
    let event = block_on(subscription.next_raw()).unwrap();
    let SubscriptionEvent::Delta {
        added,
        updated,
        removed,
        terminal_operations,
        ..
    } = event
    else {
        panic!("expected an indexed root update");
    };
    assert!(added.is_empty());
    assert_eq!(updated.len(), 1);
    assert_eq!(updated[0].row.row_uuid(), row(0xa1));
    assert_eq!(updated[0].previous_index, Some(1));
    assert_eq!(updated[0].index, 1);
    assert!(removed.is_empty());
    assert!(terminal_operations.is_empty());
}

#[test]
fn flat_subscription_updates_with_nullable_sort_payload() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("users")
                .column("name", PublicColumnType::Text)
                .nullable_column("rank", PublicColumnType::Integer),
        ),
    );
    let db = open_db(0xd6, AuthorSubject::for_test_bytes([0xd6; 16]), &schema);
    db.insert(
        "users",
        BTreeMap::from([
            ("name".to_owned(), Value::String("before".to_owned())),
            ("rank".to_owned(), Value::Nullable(None)),
        ]),
        crate::db::InsertOptions {
            row_id: Some(row(0xa1)),
            ..Default::default()
        },
    )
    .unwrap();
    for (id, rank) in [(0xb1, 1), (0xc1, 2)] {
        db.insert(
            "users",
            BTreeMap::from([
                ("name".to_owned(), Value::String(format!("rank-{rank}"))),
                (
                    "rank".to_owned(),
                    Value::Nullable(Some(Box::new(Value::I32(rank)))),
                ),
            ]),
            crate::db::InsertOptions {
                row_id: Some(row(id)),
                ..Default::default()
            },
        )
        .unwrap();
    }
    let query = Query::from("users").order_by("rank", OrderDirection::Asc);
    let prepared_query = prepared(&db, &query);
    let mut subscription = block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    let _initial = block_on(subscription.next_raw()).unwrap();

    db.update(
        "users",
        row(0xa1),
        BTreeMap::from([("name".to_owned(), Value::String("after".to_owned()))]),
        Default::default(),
    )
    .unwrap();
    db.tick().unwrap();
    let SubscriptionEvent::Delta {
        added,
        updated,
        removed,
        terminal_operations,
        ..
    } = block_on(subscription.next_raw()).unwrap()
    else {
        panic!("title-only update must emit an indexed delta");
    };
    assert!(added.is_empty());
    assert_eq!(updated.len(), 1);
    assert_eq!(updated[0].row.row_uuid(), row(0xa1));
    assert_eq!(updated[0].previous_index, Some(0));
    assert_eq!(updated[0].index, 0);
    assert!(removed.is_empty());
    assert!(terminal_operations.is_empty());
}

#[test]
fn flat_subscription_update_respects_descending_row_id_tie_break() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("users")
                .column("name", PublicColumnType::Text)
                .column("rank", PublicColumnType::Integer),
        ),
    );
    let db = open_db(0xd8, AuthorSubject::for_test_bytes([0xd8; 16]), &schema);
    for (id, rank) in [(0xf0, 1), (0xe0, 1), (0x10, 2)] {
        db.insert(
            "users",
            BTreeMap::from([
                ("name".to_owned(), Value::String(format!("user-{id}"))),
                ("rank".to_owned(), Value::I32(rank)),
            ]),
            InsertOptions {
                row_id: Some(row(id)),
                ..Default::default()
            },
        )
        .unwrap();
    }
    let query = Query::from("users")
        .order_by("rank", OrderDirection::Asc)
        .order_by("id", OrderDirection::Desc);
    let prepared_query = prepared(&db, &query);
    let mut subscription = block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    let initial = snapshot_from_event(block_on(subscription.next_raw()).unwrap());
    assert_eq!(
        row_ids(&initial.rows),
        vec![row(0xf0), row(0xe0), row(0x10)]
    );

    db.update(
        "users",
        row(0x10),
        BTreeMap::from([("rank".to_owned(), Value::I32(1))]),
        Default::default(),
    )
    .unwrap();
    db.tick().unwrap();
    let SubscriptionEvent::Delta {
        added,
        updated,
        removed,
        terminal_operations,
        ..
    } = block_on(subscription.next_raw()).unwrap()
    else {
        panic!("rank update must emit an indexed delta");
    };
    assert!(added.is_empty());
    assert_eq!(updated.len(), 1);
    assert_eq!(updated[0].row.row_uuid(), row(0x10));
    assert_eq!(updated[0].previous_index, Some(2));
    assert_eq!(updated[0].index, 2);
    assert!(removed.is_empty());
    assert!(terminal_operations.is_empty());
}

#[test]
fn flat_subscription_update_moves_largest_descending_row_id_to_front() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("users")
                .column("name", PublicColumnType::Text)
                .column("rank", PublicColumnType::Integer),
        ),
    );
    let db = open_db(0xd9, AuthorSubject::for_test_bytes([0xd9; 16]), &schema);
    for (id, rank) in [(0xf0, 1), (0xe0, 1), (0xff, 2)] {
        db.insert(
            "users",
            BTreeMap::from([
                ("name".to_owned(), Value::String(format!("user-{id}"))),
                ("rank".to_owned(), Value::I32(rank)),
            ]),
            InsertOptions {
                row_id: Some(row(id)),
                ..Default::default()
            },
        )
        .unwrap();
    }
    let query = Query::from("users")
        .order_by("rank", OrderDirection::Asc)
        .order_by("id", OrderDirection::Desc);
    let prepared_query = prepared(&db, &query);
    let mut subscription = block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    let _initial = block_on(subscription.next_raw()).unwrap();

    db.update(
        "users",
        row(0xff),
        BTreeMap::from([("rank".to_owned(), Value::I32(1))]),
        Default::default(),
    )
    .unwrap();
    db.tick().unwrap();
    let SubscriptionEvent::Delta {
        added,
        updated,
        removed,
        terminal_operations,
        ..
    } = block_on(subscription.next_raw()).unwrap()
    else {
        panic!("rank update must emit an indexed delta");
    };
    assert!(added.is_empty());
    assert_eq!(updated.len(), 1);
    assert_eq!(updated[0].row.row_uuid(), row(0xff));
    assert_eq!(updated[0].previous_index, Some(2));
    assert_eq!(updated[0].index, 0);
    assert!(removed.is_empty());
    assert!(terminal_operations.is_empty());
}

#[test]
fn flat_subscription_shifts_offset_window_when_leading_row_is_deleted() {
    let schema = relation_schema();
    let db = open_db(0xd8, AuthorSubject::for_test_bytes([0xd8; 16]), &schema);
    for (id, name) in [(0xa1, "a"), (0xb1, "b"), (0xc1, "c"), (0xd1, "d")] {
        db.insert(
            "users",
            BTreeMap::from([("name".to_owned(), Value::String(name.to_owned()))]),
            crate::db::InsertOptions {
                row_id: Some(row(id)),
                ..Default::default()
            },
        )
        .unwrap();
    }
    let query = Query::from("users")
        .order_by("name", OrderDirection::Asc)
        .offset(1)
        .limit(2);
    let prepared_query = prepared(&db, &query);
    let mut subscription = block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    let initial = snapshot_from_event(block_on(subscription.next_raw()).unwrap());
    assert_eq!(row_ids(&initial.rows), vec![row(0xb1), row(0xc1)]);

    db.delete("users", row(0xa1), Default::default()).unwrap();
    db.tick().unwrap();
    let event = block_on(subscription.next_raw()).unwrap();
    let SubscriptionEvent::Delta {
        added,
        updated,
        removed,
        terminal_operations,
        ..
    } = event
    else {
        panic!("expected an indexed window shift");
    };
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].row.row_uuid(), row(0xd1));
    assert_eq!(added[0].index, 1);
    assert!(updated.is_empty());
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].row_uuid, row(0xb1));
    assert_eq!(removed[0].index, 0);
    assert!(terminal_operations.is_empty());
}

/// Alice removes two adjacent visible rows in one transaction while Bob keeps
/// an ordered, offset subscription. Both removals retain their positions in
/// the complete result before that transaction's frame is applied.
///
/// alice ──delete b,c──► maintained view ──one delta──► bob
#[test]
fn flat_subscription_batch_removals_keep_pre_frame_indices() {
    let schema = relation_schema();
    let db = open_db(0xda, AuthorSubject::for_test_bytes([0xda; 16]), &schema);
    for (id, name) in [(0xa1, "a"), (0xb1, "b"), (0xc1, "c"), (0xd1, "d")] {
        db.insert(
            "users",
            BTreeMap::from([("name".to_owned(), Value::String(name.to_owned()))]),
            InsertOptions {
                row_id: Some(row(id)),
                ..Default::default()
            },
        )
        .unwrap();
    }
    let prepared_query = prepared(
        &db,
        &Query::from("users").order_by("name", OrderDirection::Asc),
    );
    let mut subscription = block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    let initial = snapshot_from_event(block_on(subscription.next_raw()).unwrap());
    assert_eq!(
        row_ids(&initial.rows),
        vec![row(0xa1), row(0xb1), row(0xc1), row(0xd1)]
    );

    let tx = block_on(db.mergeable_tx()).unwrap();
    block_on(tx.delete("users", row(0xb1), Default::default())).unwrap();
    block_on(tx.delete("users", row(0xc1), Default::default())).unwrap();
    block_on(tx.commit()).unwrap();
    db.tick().unwrap();

    let SubscriptionEvent::Delta {
        added,
        updated,
        removed,
        terminal_operations,
        ..
    } = block_on(subscription.next_raw()).unwrap()
    else {
        panic!("expected one indexed batch-removal delta");
    };
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert!(terminal_operations.is_empty());
    assert_eq!(
        removed
            .iter()
            .map(|removed| (removed.row_uuid, removed.index))
            .collect::<Vec<_>>(),
        vec![(row(0xb1), 1), (row(0xc1), 2)],
        "removed indices address the snapshot before this delta"
    );

    db.update(
        "users",
        row(0xd1),
        BTreeMap::from([("name".to_owned(), Value::String("z".to_owned()))]),
        Default::default(),
    )
    .unwrap();
    db.tick().unwrap();
    let SubscriptionEvent::Delta { updated, .. } = block_on(subscription.next_raw()).unwrap()
    else {
        panic!("the retained root must remain indexed after a batch removal");
    };
    assert_eq!(updated.len(), 1);
    assert_eq!(updated[0].row.row_uuid(), row(0xd1));
    assert_eq!(updated[0].previous_index, Some(1));
    assert_eq!(updated[0].index, 1);
}

#[test]
fn array_subquery_subscription_reflects_child_mutations_and_parent_removal() {
    let schema = relation_schema();
    let db = open_db(0xc2, AuthorSubject::for_test_bytes([0xc2; 16]), &schema);
    db.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("parent".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
        ]),
        crate::db::InsertOptions {
            row_id: Some(row(0x21)),
            ..Default::default()
        },
    )
    .unwrap();
    let query = Query::from("todos")
        .array_subquery(ArraySubquery::new("comments", "comments", "todo_id", "id"));
    let prepared_query = prepared(&db, &query);
    let mut subscription = block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();

    let snapshot = snapshot_from_event(block_on(subscription.next_raw()).unwrap());
    assert_eq!(
        terminal_nested_text_values(&snapshot, row(0x21), "comments", "body"),
        Vec::<String>::new()
    );

    db.insert(
        "comments",
        BTreeMap::from([
            ("body".to_owned(), Value::String("first".to_owned())),
            ("todo_id".to_owned(), Value::Uuid(row(0x21).0)),
        ]),
        crate::db::InsertOptions {
            row_id: Some(row(0xc1)),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(matches!(
        block_on(subscription.next_raw()).unwrap(),
        SubscriptionEvent::Delta { terminal_operations, .. }
            if terminal_operations.iter().any(|operation| matches!(
                operation.edit,
                groove::ivm::TerminalEdit::Insert { .. }
            ))
    ));

    db.update(
        "comments",
        row(0xc1),
        BTreeMap::from([("body".to_owned(), Value::String("edited".to_owned()))]),
        Default::default(),
    )
    .unwrap();
    let SubscriptionEvent::Delta {
        terminal_operations,
        ..
    } = block_on(subscription.next_raw()).unwrap()
    else {
        panic!("child update must emit a terminal delta");
    };
    let child_operations = terminal_operations
        .iter()
        .filter(|operation| !operation.path.is_empty())
        .collect::<Vec<_>>();
    let [remove, insert] = child_operations.as_slice() else {
        panic!("a child replacement must emit exactly one remove and one insert");
    };
    let groove::ivm::TerminalEdit::Remove { key: remove_key } = &remove.edit else {
        panic!("a child replacement must begin with Remove");
    };
    let groove::ivm::TerminalEdit::Insert {
        key: insert_key, ..
    } = &insert.edit
    else {
        panic!("a child replacement must end with Insert");
    };
    assert_eq!(
        remove_key, insert_key,
        "canonical replacement must address one stable child identity"
    );

    db.delete("comments", row(0xc1), Default::default())
        .unwrap();
    assert!(matches!(
        block_on(subscription.next_raw()).unwrap(),
        SubscriptionEvent::Delta { terminal_operations, .. }
            if terminal_operations.iter().any(|operation| matches!(
                operation.edit,
                groove::ivm::TerminalEdit::Remove { .. }
            ))
    ));

    db.insert(
        "comments",
        BTreeMap::from([
            ("body".to_owned(), Value::String("second".to_owned())),
            ("todo_id".to_owned(), Value::Uuid(row(0x21).0)),
        ]),
        crate::db::InsertOptions {
            row_id: Some(row(0xc2)),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(matches!(
        block_on(subscription.next_raw()).unwrap(),
        SubscriptionEvent::Delta { terminal_operations, .. }
            if terminal_operations.iter().any(|operation| matches!(
                operation.edit,
                groove::ivm::TerminalEdit::Insert { .. }
            ))
    ));

    db.delete("todos", row(0x21), Default::default()).unwrap();
    assert!(matches!(
        block_on(subscription.next_raw()).unwrap(),
        SubscriptionEvent::Delta {
            removed,
            terminal_operations,
            ..
        } if removed.iter().any(|removed| removed.row_uuid == row(0x21))
            && terminal_operations.iter().all(|operation| !operation.path.is_empty())
    ));
}

#[test]
fn array_subquery_subscription_updates_child_order_limit_boundary() {
    let schema = relation_schema();
    let db = open_db(0xc3, AuthorSubject::for_test_bytes([0xc3; 16]), &schema);
    db.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("parent".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
        ]),
        crate::db::InsertOptions {
            row_id: Some(row(0x31)),
            ..Default::default()
        },
    )
    .unwrap();
    let query = Query::from("todos").array_subquery(
        ArraySubquery::new("comments", "comments", "todo_id", "id")
            .order_by("body", OrderDirection::Asc)
            .offset(1)
            .limit(1),
    );
    let prepared_query = prepared(&db, &query);
    let mut subscription = block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();

    let snapshot = snapshot_from_event(block_on(subscription.next_raw()).unwrap());
    assert_eq!(
        terminal_nested_text_values(&snapshot, row(0x31), "comments", "body"),
        Vec::<String>::new()
    );

    db.insert(
        "comments",
        BTreeMap::from([
            ("body".to_owned(), Value::String("b".to_owned())),
            ("todo_id".to_owned(), Value::Uuid(row(0x31).0)),
        ]),
        crate::db::InsertOptions {
            row_id: Some(row(0xd1)),
            ..Default::default()
        },
    )
    .unwrap();
    db.tick().unwrap();
    assert!(
        subscription.try_next_event().is_none(),
        "a child outside the actual collector window must not publish a root update"
    );
    assert_eq!(
        terminal_nested_text_values(&snapshot, row(0x31), "comments", "body"),
        Vec::<String>::new()
    );

    db.insert(
        "comments",
        BTreeMap::from([
            ("body".to_owned(), Value::String("c".to_owned())),
            ("todo_id".to_owned(), Value::Uuid(row(0x31).0)),
        ]),
        crate::db::InsertOptions {
            row_id: Some(row(0xd2)),
            ..Default::default()
        },
    )
    .unwrap();
    db.tick().unwrap();
    let expect_inserted_child = |event: SubscriptionEvent, expected: RowUuid| match event {
        SubscriptionEvent::Delta {
            terminal_operations,
            ..
        } => assert!(terminal_operations.iter().any(|operation| {
            matches!(
                &operation.edit,
                groove::ivm::TerminalEdit::Insert { key, .. }
                    if key.as_slice()
                        == [10]
                            .into_iter()
                            .chain(expected.0.as_bytes().iter().copied())
                            .collect::<Vec<_>>()
            )
        })),
        other => panic!("expected terminal patch event, got {other:?}"),
    };
    expect_inserted_child(block_on(subscription.next_raw()).unwrap(), row(0xd2));

    db.insert(
        "comments",
        BTreeMap::from([
            ("body".to_owned(), Value::String("a".to_owned())),
            ("todo_id".to_owned(), Value::Uuid(row(0x31).0)),
        ]),
        crate::db::InsertOptions {
            row_id: Some(row(0xd3)),
            ..Default::default()
        },
    )
    .unwrap();
    db.tick().unwrap();
    expect_inserted_child(block_on(subscription.next_raw()).unwrap(), row(0xd1));

    db.update(
        "comments",
        row(0xd3),
        BTreeMap::from([("body".to_owned(), Value::String("z".to_owned()))]),
        Default::default(),
    )
    .unwrap();
    db.tick().unwrap();
    expect_inserted_child(block_on(subscription.next_raw()).unwrap(), row(0xd2));
}

#[test]
fn array_subquery_policy_oracle_filters_child_array_contents_per_identity() {
    let schema = policy_relation_schema();
    let member = AuthorSubject::for_test_bytes([0xa1; 16]);
    let other = AuthorSubject::for_test_bytes([0xb1; 16]);
    let spy = AuthorSubject::for_test_bytes([0xc1; 16]);
    let db = open_db(0xc4, AuthorSubject::SYSTEM, &schema);
    for identity in [member, other, spy] {
        db.set_test_provider_claims(
            identity,
            BTreeMap::from([(
                crate::query::provider_claim_key("sub"),
                Value::Uuid(identity.test_uuid()),
            )]),
        );
    }
    db.insert(
        "todos",
        BTreeMap::from([("title".to_owned(), Value::String("parent".to_owned()))]),
        crate::db::InsertOptions {
            row_id: Some(row(0x41)),
            ..Default::default()
        },
    )
    .unwrap();
    for (id, body, owner) in [
        (0xe1, "member-visible", member),
        (0xe2, "other-visible", other),
    ] {
        db.insert(
            "comments",
            BTreeMap::from([
                ("body".to_owned(), Value::String(body.to_owned())),
                ("todo_id".to_owned(), Value::Uuid(row(0x41).0)),
                ("owner".to_owned(), Value::Uuid(owner.test_uuid())),
            ]),
            crate::db::InsertOptions {
                row_id: Some(row(id)),
                ..Default::default()
            },
        )
        .unwrap();
    }
    let query = Query::from("todos")
        .array_subquery(ArraySubquery::new("comments", "comments", "todo_id", "id"));
    let prepared_query = prepared(&db, &query);

    let admin = block_on(db.all_relation_snapshot_for_identity(
        &prepared_query,
        ReadOpts::default(),
        AuthorSubject::SYSTEM,
    ))
    .unwrap();
    assert_eq!(
        terminal_nested_text_values(&admin, row(0x41), "comments", "body"),
        vec!["member-visible".to_owned(), "other-visible".to_owned()]
    );

    let member_snapshot = block_on(db.all_relation_snapshot_for_identity(
        &prepared_query,
        ReadOpts::default(),
        member,
    ))
    .unwrap();
    assert_eq!(
        terminal_nested_text_values(&member_snapshot, row(0x41), "comments", "body"),
        vec!["member-visible".to_owned()]
    );

    let spy_snapshot =
        block_on(db.all_relation_snapshot_for_identity(&prepared_query, ReadOpts::default(), spy))
            .unwrap();
    assert_eq!(
        terminal_nested_text_values(&spy_snapshot, row(0x41), "comments", "body"),
        Vec::<String>::new()
    );
}

#[test]
fn array_subquery_one_shot_and_maintained_subscription_are_equivalent() {
    let schema = relation_schema();
    let db = open_db(0xc5, AuthorSubject::for_test_bytes([0xc5; 16]), &schema);
    db.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("parent".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
        ]),
        crate::db::InsertOptions {
            row_id: Some(row(0x51)),
            ..Default::default()
        },
    )
    .unwrap();
    for (id, body) in [(0xf1, "first"), (0xf2, "second")] {
        db.insert(
            "comments",
            BTreeMap::from([
                ("body".to_owned(), Value::String(body.to_owned())),
                ("todo_id".to_owned(), Value::Uuid(row(0x51).0)),
            ]),
            crate::db::InsertOptions {
                row_id: Some(row(id)),
                ..Default::default()
            },
        )
        .unwrap();
    }
    let query = Query::from("todos").array_subquery(
        ArraySubquery::new("comments", "comments", "todo_id", "id")
            .order_by("body", OrderDirection::Asc),
    );
    let prepared_query = prepared(&db, &query);
    let one_shot =
        block_on(db.all_relation_snapshot(&prepared_query, ReadOpts::default())).unwrap();
    let mut subscription = block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    let maintained = snapshot_from_event(block_on(subscription.next_raw()).unwrap());

    assert_eq!(
        terminal_nested_text_values(&maintained, row(0x51), "comments", "body"),
        terminal_nested_text_values(&one_shot, row(0x51), "comments", "body")
    );
}

#[test]
fn array_subquery_subscription_projects_late_root_and_existing_forward_target() {
    let schema = relation_schema();
    let db = open_db(0xc7, AuthorSubject::for_test_bytes([0xc7; 16]), &schema);
    db.insert(
        "users",
        BTreeMap::from([("name".to_owned(), Value::String("owner".to_owned()))]),
        crate::db::InsertOptions {
            row_id: Some(row(0xa1)),
            ..Default::default()
        },
    )
    .unwrap();
    let query = Query::from("todos")
        .select(["title"])
        .array_subquery(ArraySubquery::new("owner", "users", "id", "owner_id").select(["name"]));
    let prepared_query = prepared(&db, &query);
    let mut subscription = block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    let opened = snapshot_from_event(block_on(subscription.next_raw()).unwrap());
    assert!(opened.rows.is_empty());

    db.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("late root".to_owned())),
            ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
        ]),
        crate::db::InsertOptions {
            row_id: Some(row(0x52)),
            ..Default::default()
        },
    )
    .unwrap();
    let SubscriptionEvent::Delta {
        added,
        terminal_operations,
        ..
    } = block_on(subscription.next_raw()).unwrap()
    else {
        panic!("expected an indexed late-root insertion");
    };
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].row.row_uuid(), row(0x52));
    assert_eq!(added[0].index, 0);
    assert!(
        terminal_operations
            .iter()
            .all(|operation| !operation.path.is_empty()),
        "the root insertion is indexed while its existing child is a descendant patch"
    );
}

#[test]
fn array_subquery_subscription_projects_late_camel_case_root_and_existing_forward_target() {
    let schema = issue_schema();
    let db = open_db(0xc8, AuthorSubject::for_test_bytes([0xc8; 16]), &schema);
    db.insert(
        "projects",
        BTreeMap::from([("name".to_owned(), Value::String("project".to_owned()))]),
        crate::db::InsertOptions {
            row_id: Some(row(0xa2)),
            ..Default::default()
        },
    )
    .unwrap();
    let query = Query::from("issues").select(["title"]).array_subquery(
        ArraySubquery::new("project", "projects", "id", "project").select(["name"]),
    );
    let prepared_query = prepared(&db, &query);
    let mut subscription = block_on(db.subscribe(&prepared_query, ReadOpts::default())).unwrap();
    let opened = snapshot_from_event(block_on(subscription.next_raw()).unwrap());
    assert!(opened.rows.is_empty());

    db.insert(
        "issues",
        issue_cells(
            "late issue",
            "open",
            AuthorSubject::for_test_bytes([0xa8; 16]),
            row(0xa2),
            1,
            &[],
            None,
        ),
        crate::db::InsertOptions {
            row_id: Some(row(0x53)),
            ..Default::default()
        },
    )
    .unwrap();
    let SubscriptionEvent::Delta {
        added,
        terminal_operations,
        ..
    } = block_on(subscription.next_raw()).unwrap()
    else {
        panic!("expected an indexed late-root insertion");
    };
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].row.row_uuid(), row(0x53));
    assert_eq!(added[0].index, 0);
    assert!(
        terminal_operations
            .iter()
            .all(|operation| !operation.path.is_empty()),
        "the root insertion is indexed while its existing child is a descendant patch"
    );
}

#[test]
fn array_subquery_remote_subscription_hydrates_edge_referenced_child_rows() {
    let schema = relation_schema();
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client_author = AuthorSubject::for_test_bytes([0xc6; 16]);
    let client = open_db(0xc6, client_author, &schema);
    let (client_transport, server_transport) = byte_duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("users").array_subquery(ArraySubquery::new(
        "todosViaOwner",
        "todos",
        "owner_id",
        "id",
    ));
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let opened = snapshot_from_event(block_on(subscription.next_raw()).unwrap());
    assert!(opened.rows.is_empty());

    server
        .insert_with_id(
            "users",
            row(0xa6),
            BTreeMap::from([("name".to_owned(), Value::String("remote user".to_owned()))]),
        )
        .unwrap();
    server
        .insert_with_id(
            "todos",
            row(0x66),
            BTreeMap::from([
                ("title".to_owned(), Value::String("remote child".to_owned())),
                ("owner_id".to_owned(), Value::Uuid(row(0xa6).0)),
            ]),
        )
        .unwrap();

    let mut delivered = None;
    for _ in 0..20 {
        client.tick().unwrap();
        server.server.tick().unwrap();
        client.tick().unwrap();
        if let Some(event) = subscription.try_next_event() {
            let SubscriptionEvent::Delta {
                reset,
                terminal_operations,
                ..
            } = &event
            else {
                panic!("expected authority-covered receiver delta")
            };
            let reset = *reset;
            let terminal_operations_empty = terminal_operations.is_empty();
            let snapshot = snapshot_from_event(event);
            if terminal_nested_text_values(&snapshot, row(0xa6), "todosViaOwner", "title")
                == vec!["remote child".to_owned()]
            {
                assert!(
                    reset,
                    "the first scoped covered-input frontier publishes its complete collector tree as a reset"
                );
                assert!(
                    terminal_operations_empty,
                    "a reset carries the receiver-local collector snapshot rather than replaying its construction patches"
                );
                delivered = Some(snapshot);
                break;
            }
        }
    }
    assert!(
        delivered.is_some(),
        "remote maintained array subscription must deliver the Groove terminal parent"
    );

    server
        .insert_with_id(
            "todos",
            row(0x67),
            BTreeMap::from([
                ("title".to_owned(), Value::String("second child".to_owned())),
                ("owner_id".to_owned(), Value::Uuid(row(0xa6).0)),
            ]),
        )
        .unwrap();
    let mut delivered_patch = None;
    for _ in 0..20 {
        client.tick().unwrap();
        server.server.tick().unwrap();
        client.tick().unwrap();
        if let Some(event @ SubscriptionEvent::Delta { .. }) = subscription.try_next_event() {
            if matches!(
                &event,
                SubscriptionEvent::Delta {
                    reset: false,
                    terminal_operations,
                    added,
                    updated,
                    removed,
                    ..
                } if added.is_empty()
                    && updated.is_empty()
                    && removed.is_empty()
                    && terminal_operations.iter().any(|operation| {
                        !operation.path.is_empty()
                            && matches!(operation.edit, groove::ivm::TerminalEdit::Insert { .. })
                    })
            ) {
                delivered_patch = Some(event);
                break;
            }
        }
    }
    assert!(
        delivered_patch.is_some(),
        "framed peer delivery must preserve a generic terminal patch without row replacement"
    );
}

#[test]
fn indexed_root_delta_preserves_typed_union_occurrence_ids_for_duplicate_rows() {
    let schema = schema();
    let db = open_db(0xd1, AuthorSubject::SYSTEM, &schema);
    let root = row(0xd2);
    db.insert(
        "todos",
        BTreeMap::from([
            ("title".to_owned(), Value::String("same source".to_owned())),
            ("done".to_owned(), Value::Bool(false)),
            ("owner".to_owned(), Value::Uuid(row(0xd3).0)),
        ]),
        crate::db::InsertOptions {
            row_id: Some(root),
            ..Default::default()
        },
    )
    .unwrap();
    let source_row = prepared_one(&db, &Query::from("todos")).expect("inserted source row");
    let left = OutputOccurrenceId::with_union_arms(
        ObjectId::from_uuid(root.0),
        [ObjectId::from_uuid(row(0xd4).0)],
        [(0, "left".to_owned())],
    )
    .expect("typed union occurrence");
    let right = OutputOccurrenceId::with_union_arms(
        ObjectId::from_uuid(root.0),
        [ObjectId::from_uuid(row(0xd4).0)],
        [(0, "right".to_owned())],
    )
    .expect("distinct typed union occurrence");
    let previous = RelationSnapshot {
        root_count: 2,
        rows: vec![source_row.clone(), source_row.clone()],
        edges: Vec::new(),
    };
    let current = RelationSnapshot {
        root_count: 1,
        rows: vec![source_row],
        edges: Vec::new(),
    };

    let event = subscription_terminal_delta_event(
        DurabilityTier::Edge,
        true,
        &previous,
        &[left.clone(), right.clone()],
        &current,
        std::slice::from_ref(&right),
    )
    .expect("sidecar-preserving indexed root delta");
    let SubscriptionEvent::Delta {
        removed,
        added,
        updated,
        ..
    } = event
    else {
        panic!("expected delta");
    };
    assert!(added.is_empty());
    assert_eq!(
        removed
            .iter()
            .map(|row| (&row.occurrence_id, row.index))
            .collect::<Vec<_>>(),
        vec![(&left, 0)],
        "the removed arm keeps its exact typed identity and prior position"
    );
    assert_eq!(
        updated
            .iter()
            .map(|row| (&row.occurrence_id, row.previous_index, row.index))
            .collect::<Vec<_>>(),
        vec![(&right, Some(1), 0)],
        "the surviving arm moves under its original typed identity"
    );
}
