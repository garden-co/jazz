//! Authoritative reset and settled-result materialization.

use super::*;

#[test]
fn maintained_physical_point_subscription_stays_live_for_only_its_row() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let db = open_db(0xc1, author, &schema);
    let target = row(0x71);
    let other = row(0x72);
    for (row_id, title) in [(target, "target"), (other, "other")] {
        db.insert(
            "todos",
            cells(title, false, author),
            crate::db::InsertOptions {
                row_id: Some(row_id),
                ..Default::default()
            },
        )
        .unwrap();
    }

    let query = Query::from("todos").filter(eq(col("id"), lit(Value::Uuid(target.0))));
    let mut subscription = prepared_subscribe(&db, &query, ReadOpts::default()).unwrap();
    let SubscriptionEvent::Delta { added, .. } = block_on(subscription.next_raw()).unwrap() else {
        panic!("expected opening point-subscription delta");
    };
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].row_uuid(), target);

    db.update(
        "todos",
        other,
        BTreeMap::from([("title".to_owned(), Value::String("unrelated".to_owned()))]),
        Default::default(),
    )
    .unwrap();
    assert!(subscription.try_next_event().is_none());

    db.update(
        "todos",
        target,
        BTreeMap::from([("title".to_owned(), Value::String("changed".to_owned()))]),
        Default::default(),
    )
    .unwrap();
    let SubscriptionEvent::Delta { updated, .. } = block_on(subscription.next_raw()).unwrap()
    else {
        panic!("expected target-row point-subscription delta");
    };
    assert_eq!(updated.len(), 1);
    assert_eq!(updated[0].row_uuid(), target);
}

/// A deferred local write refreshes a projected current-view subscription on
/// its owning tick, even when the stream opened before that tick.
///
/// alice ──open projected view──► local current-view collector
/// alice ──deferred insert──────► owner tick ──reset/delta──► subscription
#[test]
fn deferred_local_publication_refreshes_projected_current_view() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc2; 16]);
    let db = open_db(0xc2, author, &schema);
    db.set_deferred_local_persistence(true);

    let query = Query::from("todos")
        .select(["title", "$createdBy"])
        .order_by("title", OrderDirection::Asc);
    let mut subscription = prepared_subscribe(&db, &query, ReadOpts::default())
        .expect("open projected local subscription");
    let SubscriptionEvent::Delta { added, .. } = block_on(subscription.next_raw()).unwrap() else {
        panic!("expected empty opening projected-subscription delta");
    };
    assert!(added.is_empty());

    let inserted = row(0x73);
    db.insert(
        "todos",
        cells("settles after opening", false, author),
        crate::db::InsertOptions {
            row_id: Some(inserted),
            ..Default::default()
        },
    )
    .expect("queue deferred local insert");
    db.tick()
        .expect("settle deferred publication and refresh subscription");

    let Some(SubscriptionEvent::Delta { added, .. }) = subscription.try_next_event() else {
        panic!("expected projected delta after deferred local insert");
    };
    assert_eq!(
        added.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        [inserted]
    );
    db.tick()
        .expect("quiet owner tick after projected publication");
    assert!(
        subscription.try_next_event().is_none(),
        "the settlement refresh publishes the queued local write once"
    );
}

/// Mergeable transaction publication takes the same deferred-local admission
/// path as a direct write, including projection-only current views.
#[test]
fn deferred_mergeable_commit_refreshes_projected_current_view_once() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc3; 16]);
    let db = open_db(0xc3, author, &schema);
    db.set_deferred_local_persistence(true);

    let query = Query::from("todos")
        .select(["title", "$createdBy"])
        .order_by("title", OrderDirection::Asc);
    let mut subscription = prepared_subscribe(&db, &query, ReadOpts::default())
        .expect("open projected local subscription");
    let SubscriptionEvent::Delta { added, .. } = block_on(subscription.next_raw()).unwrap() else {
        panic!("expected empty opening projected-subscription delta");
    };
    assert!(added.is_empty());

    let inserted = row(0x74);
    let tx = db.mergeable_tx().expect("open mergeable transaction");
    tx.insert(
        "todos",
        cells("mergeable after opening", false, author),
        crate::db::InsertOptions {
            row_id: Some(inserted),
            ..Default::default()
        },
    )
    .expect("stage deferred mergeable insert");
    tx.commit()
        .expect("admit deferred mergeable publication and refresh subscription");

    let Some(SubscriptionEvent::Delta { added, .. }) = subscription.try_next_event() else {
        panic!("expected projected delta after deferred mergeable commit");
    };
    assert_eq!(
        added.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        [inserted]
    );
    db.tick()
        .expect("quiet owner tick after projected mergeable publication");
    assert!(
        subscription.try_next_event().is_none(),
        "the mergeable settlement refresh publishes the queued local write once"
    );
}

/// Exclusive transaction publication takes the same deferred-local admission
/// path as a direct write, including projection-only current views.
#[test]
fn deferred_exclusive_commit_refreshes_projected_current_view_once() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc4; 16]);
    let db = open_db(0xc4, author, &schema);
    db.set_deferred_local_persistence(true);

    let query = Query::from("todos")
        .select(["title", "$createdBy"])
        .order_by("title", OrderDirection::Asc);
    let mut subscription = prepared_subscribe(&db, &query, ReadOpts::default())
        .expect("open projected local subscription");
    let SubscriptionEvent::Delta { added, .. } = block_on(subscription.next_raw()).unwrap() else {
        panic!("expected empty opening projected-subscription delta");
    };
    assert!(added.is_empty());

    let inserted = row(0x75);
    let tx = db.exclusive_tx().expect("open exclusive transaction");
    tx.insert(
        "todos",
        cells("exclusive after opening", false, author),
        crate::db::InsertOptions {
            row_id: Some(inserted),
            ..Default::default()
        },
    )
    .expect("stage deferred exclusive insert");
    tx.commit()
        .expect("admit deferred exclusive publication and refresh subscription");

    let Some(SubscriptionEvent::Delta { added, .. }) = subscription.try_next_event() else {
        panic!("expected projected delta after deferred exclusive commit");
    };
    assert_eq!(
        added.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        [inserted]
    );
    db.tick()
        .expect("quiet owner tick after projected exclusive publication");
    assert!(
        subscription.try_next_event().is_none(),
        "the exclusive settlement refresh publishes the queued local write once"
    );
}

#[test]
fn server_reset_subscription_materializes_without_local_snapshot_eval() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("first", false, owner));
    seed(&server, "todos", cells("second", true, owner));

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());

    client.tick().unwrap();
    server.tick().unwrap();
    client
        .node
        .node
        .borrow_mut()
        .reset_subscription_snapshot_for_link_call_count();
    let stats = client.tick_stats().unwrap();
    assert_eq!(stats.subscription_events, 1);
    assert_eq!(
        client
            .node
            .node
            .borrow()
            .subscription_snapshot_for_link_call_count(),
        0,
        "authoritative server reset should not re-run the subscription query locally"
    );

    let event = block_on(subscription.next_raw()).unwrap();
    let SubscriptionEvent::Delta {
        reset,
        added,
        updated,
        removed,
        settled,
        ..
    } = event
    else {
        panic!("expected subscription delta");
    };
    assert!(reset);
    assert!(settled);
    assert_eq!(added.len(), 2);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
}

#[test]
fn authoritative_reset_rebuilds_occurrence_sidecar_after_order_and_count_change() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);

    let first = row(0x71);
    let middle = row(0x72);
    let last = row(0x73);
    let first_write = client
        .insert(
            "todos",
            cells("alpha", false, client_author),
            crate::db::InsertOptions {
                row_id: Some(first),
                ..Default::default()
            },
        )
        .unwrap();
    let _middle_write = client
        .insert(
            "todos",
            cells("middle", false, client_author),
            crate::db::InsertOptions {
                row_id: Some(middle),
                ..Default::default()
            },
        )
        .unwrap();
    let last_write = client
        .insert(
            "todos",
            cells("omega", false, client_author),
            crate::db::InsertOptions {
                row_id: Some(last),
                ..Default::default()
            },
        )
        .unwrap();
    client.tick().unwrap();

    let query = Query::from("todos").order_by("title", OrderDirection::Asc);
    let prepared = prepared(&client, &query);
    let opts = ReadOpts::default();
    let mut subscription = block_on(client.subscribe(&prepared, opts.clone())).unwrap();
    let SubscriptionEvent::Delta { added, .. } = block_on(subscription.next_raw()).unwrap() else {
        panic!("expected opening subscription delta");
    };
    assert_eq!(
        added.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        vec![first, middle, last]
    );

    let first_updated = client
        .update(
            "todos",
            first,
            BTreeMap::from([("title".to_owned(), Value::String("zulu".to_owned()))]),
            Default::default(),
        )
        .unwrap();
    let binding_view_key = BindingViewKey::new(
        prepared.shape().shape_id(),
        prepared.binding().binding_id(),
        RegisterShapeOptions {
            tier: opts.tier,
            read_view: opts.read_view,
            ..RegisterShapeOptions::default()
        }
        .read_view_key(),
    );
    client
        .node
        .node
        .borrow_mut()
        .inject_pending_authoritative_reset_for_test(
            binding_view_key,
            [
                ResultMemberEntry::row((
                    "todos".to_owned().into(),
                    first,
                    first_updated.mergeable_tx_id(),
                )),
                ResultMemberEntry::row((
                    "todos".to_owned().into(),
                    last,
                    last_write.mergeable_tx_id(),
                )),
            ],
            GlobalTime(42),
        );

    assert_eq!(client.refresh_subscriptions().unwrap(), 1);
    let event = block_on(subscription.next_raw()).unwrap();
    let reset = if matches!(event, SubscriptionEvent::Delta { reset: true, .. }) {
        event
    } else {
        block_on(subscription.next_raw()).unwrap()
    };
    assert!(matches!(
        reset,
        SubscriptionEvent::Delta { reset: true, .. }
    ));
    let SubscriptionEvent::Delta { added, .. } = &reset else {
        unreachable!("reset was checked above");
    };
    assert_eq!(
        added.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        vec![last, first],
        "the reset wire payload must preserve the maintained snapshot order"
    );

    let state = subscription._state.borrow();
    let SubscriptionKind::Prepared {
        maintained_subscription: Some(maintained),
        ..
    } = &state.kind
    else {
        panic!("expected maintained subscription state");
    };
    let paired = subscription_outputs_with_occurrence_sidecar(
        &state.snapshot,
        maintained.root_occurrence_ids(),
    )
    .expect("authoritative reset must atomically replace the root occurrence sidecar");
    assert_eq!(
        paired
            .iter()
            .map(|output| output.row_uuid())
            .collect::<Vec<_>>(),
        vec![last, first],
        "reset rows reordered after the title update and removed the middle row"
    );
    assert_eq!(
        paired
            .iter()
            .map(|output| output.occurrence_id.clone())
            .collect::<Vec<_>>(),
        vec![
            OutputOccurrenceId::single_source(ObjectId::from_uuid(last.0)),
            OutputOccurrenceId::single_source(ObjectId::from_uuid(first.0)),
        ],
        "each reset row remains paired with its current occurrence root"
    );
    assert_ne!(
        first_write.mergeable_tx_id(),
        first_updated.mergeable_tx_id()
    );
}

#[test]
fn authoritative_reset_with_missing_payload_falls_back_to_refresh() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let prepared = prepared(&client, &query);
    let opts = global_subscribe_opts();
    let mut subscription = block_on(client.subscribe(&prepared, opts.clone())).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(
        event_settled(&block_on(subscription.next_raw()).unwrap()),
        "the injected reset test needs a real current-connection authority receipt"
    );
    // Keep the reset-fallback assertion focused on the resulting publication,
    // while retaining the real connection receipt required by Edge/Global.
    subscription._state.borrow_mut().settled = false;

    let missing_tx = TxId::new(
        TxTime(116_898_697_390_129_152),
        NodeUuid::from_bytes([0x77; 16]),
    );
    let binding_view_key = BindingViewKey::new(
        prepared.shape().shape_id(),
        prepared.binding().binding_id(),
        RegisterShapeOptions {
            tier: opts.tier,
            read_view: opts.read_view,
            ..RegisterShapeOptions::default()
        }
        .read_view_key(),
    );
    client
        .node
        .node
        .borrow_mut()
        .inject_pending_authoritative_reset_for_test(
            binding_view_key,
            [ResultMemberEntry::row((
                "todos".to_owned().into(),
                row(0x7a),
                missing_tx,
            ))],
            GlobalTime(42),
        );
    client
        .node
        .node
        .borrow_mut()
        .reset_subscription_snapshot_for_link_call_count();

    let changed = client.refresh_subscriptions().unwrap();
    assert_eq!(changed, 1);
    let node = client.node.node.borrow();
    assert_eq!(
        node.sync_metrics()
            .authoritative_reset_missing_payload_fallbacks,
        1
    );
    assert_eq!(node.subscription_snapshot_for_link_call_count(), 1);
    assert!(
        node.has_pending_authoritative_reset_for_test(binding_view_key),
        "missing payload fallback must keep the authoritative reset pending for a later retry"
    );
    drop(node);
    assert!(prepared_all(&client, &query, ReadOpts::default()).is_empty());
}

#[test]
fn authoritative_reset_skips_stale_member_without_falling_back() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);

    let query = Query::from("todos");
    let prepared = prepared(&client, &query);
    let opts = global_subscribe_opts();
    let mut subscription = block_on(client.subscribe(&prepared, opts.clone())).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());

    let live_row = row(0x7a);
    let stale_row = row(0x7b);
    let tx_id = client
        .node
        .node
        .borrow_mut()
        .commit_mergeable_settled(
            MergeableCommit::new("todos", live_row, client.next_now_ms())
                .made_by(client_author)
                .permission_subject(client_author)
                .cells(cells("live", false, client_author)),
        )
        .unwrap();

    let binding_view_key = BindingViewKey::new(
        prepared.shape().shape_id(),
        prepared.binding().binding_id(),
        RegisterShapeOptions {
            tier: opts.tier,
            read_view: opts.read_view,
            ..RegisterShapeOptions::default()
        }
        .read_view_key(),
    );
    client
        .node
        .node
        .borrow_mut()
        .inject_pending_authoritative_reset_for_test(
            binding_view_key,
            [
                ResultMemberEntry::row(("todos".to_owned().into(), live_row, tx_id)),
                ResultMemberEntry::row(("todos".to_owned().into(), stale_row, tx_id)),
            ],
            GlobalTime(42),
        );
    client
        .node
        .node
        .borrow_mut()
        .reset_subscription_snapshot_for_link_call_count();

    let changed = client.refresh_subscriptions().unwrap();
    assert_eq!(changed, 1);
    assert_eq!(
        client
            .node
            .node
            .borrow()
            .subscription_snapshot_for_link_call_count(),
        0,
        "stale members with present tx metadata must not force local query fallback"
    );
    let event = block_on(subscription.next_raw()).unwrap();
    let SubscriptionEvent::Delta {
        reset,
        added,
        updated,
        removed,
        settled,
        ..
    } = event
    else {
        panic!("expected subscription delta");
    };
    assert!(reset);
    assert!(
        !settled,
        "an injected durable reset is not a fresh current-connection ViewUpdate receipt"
    );
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].row_uuid(), live_row);
}

#[test]
fn client_tier_routing_scans_local_overlay_but_uses_global_settled_members_at_edge() {
    // The client holds an extra raw row locally while the serving host has
    // only the published row. This guards against an Edge facade widening
    // server scope by re-scanning a broad local transport cache.
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let db = open_db(0xc1, client_author, &schema);
    let published = seed(&server, "todos", cells("published", false, client_author));
    let server_overemitted = row(0x72);
    let published_tx = db
        .node
        .node
        .borrow_mut()
        .commit_mergeable_settled(
            MergeableCommit::new("todos", published, db.next_now_ms())
                .made_by(client_author)
                .permission_subject(client_author)
                .cells(cells("not published", false, client_author)),
        )
        .unwrap();
    let overemitted_tx = db
        .node
        .node
        .borrow_mut()
        .commit_mergeable_settled(
            MergeableCommit::new("todos", server_overemitted, db.next_now_ms())
                .made_by(client_author)
                .permission_subject(client_author)
                .cells(cells("published", false, client_author)),
        )
        .unwrap();
    {
        let mut node = db.node.node.borrow_mut();
        node.apply_fate_update(
            published_tx,
            Fate::Accepted,
            None,
            Some(DurabilityTier::Edge),
        )
        .unwrap();
        node.apply_fate_update(
            overemitted_tx,
            Fate::Accepted,
            None,
            Some(DurabilityTier::Edge),
        )
        .unwrap();
    }

    let query = Query::from("todos").filter(in_list(
        col("id"),
        [lit(published.0), lit(server_overemitted.0)],
    ));
    let prepared = prepared(&db, &query);
    let ids = |rows: Vec<CurrentRow>| {
        rows.into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>()
    };
    let none_opts = ReadOpts {
        tier: DurabilityTier::None,
        local_updates: LocalUpdates::Deferred,
        propagation: Propagation::LocalOnly,
        include_deleted: false,
        ..ReadOpts::default()
    };
    let local_opts = ReadOpts {
        tier: DurabilityTier::Local,
        local_updates: LocalUpdates::Deferred,
        propagation: Propagation::LocalOnly,
        include_deleted: false,
        ..ReadOpts::default()
    };
    assert_eq!(
        ids(block_on(db.all(&prepared, none_opts)).unwrap()),
        BTreeSet::from([published, server_overemitted]),
        "None reads scan the complete process-local overlay"
    );
    assert_eq!(
        ids(block_on(db.all(&prepared, local_opts)).unwrap()),
        BTreeSet::from([published, server_overemitted]),
        "Local reads scan the complete durable local overlay"
    );

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(db.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, client_author);
    let attachment = db
        .attach_query_with_opts(&prepared, edge_subscribe_opts())
        .expect("attach Edge coverage");
    db.tick().unwrap();
    server.tick().unwrap();
    db.tick().unwrap();
    assert!(db.query_attachment_is_covered(&attachment));

    // Coverage acknowledgements are usage-site scoped. A second attachment
    // shares the canonical Global result set, but must wait for its own server
    // response rather than treating the older attachment's empty/non-empty
    // state as fresh coverage.
    let fresh_attachment = db
        .attach_query_with_opts(&prepared, edge_subscribe_opts())
        .expect("attach a second Edge coverage request");
    db.tick().unwrap();
    let concurrent_attachment = db
        .attach_query_with_opts(&prepared, edge_subscribe_opts())
        .expect("attach concurrent Edge coverage");
    db.tick().unwrap();
    assert!(
        !db.query_attachment_is_covered(&fresh_attachment),
        "a prior canonical result set must not acknowledge a new attachment"
    );
    assert!(
        !db.query_attachment_is_covered(&concurrent_attachment),
        "concurrent attachments require a later shared receipt"
    );
    db.tick().unwrap();
    server.tick().unwrap();
    db.tick().unwrap();
    assert!(db.query_attachment_is_covered(&fresh_attachment));
    assert!(db.query_attachment_is_covered(&concurrent_attachment));
    db.detach_query(fresh_attachment);
    db.detach_query(concurrent_attachment);

    assert_eq!(
        ids(block_on(db.all(&prepared, edge_subscribe_opts())).unwrap()),
        BTreeSet::from([published]),
        "Edge reads consume the canonical Global settled member set"
    );
    assert_eq!(
        ids(block_on(db.all(&prepared, global_subscribe_opts())).unwrap()),
        BTreeSet::from([published]),
        "Global reads consume the canonical Global settled member set"
    );
    db.detach_query(attachment);
    let reattached = db
        .attach_query_with_opts(&prepared, edge_subscribe_opts())
        .expect("re-attach Edge coverage after unsubscribe");
    db.tick().unwrap();
    assert!(
        !db.query_attachment_is_covered(&reattached),
        "unsubscribe then re-attach requires a newer receipt"
    );
    server.tick().unwrap();
    db.tick().unwrap();
    assert!(db.query_attachment_is_covered(&reattached));
    db.detach_query(reattached);
    let mut edge_subscription =
        block_on(db.subscribe(&prepared, edge_subscribe_opts())).expect("open edge subscription");
    assert!(opened_rows(block_on(edge_subscription.next_raw()).unwrap()).is_empty());
    db.tick().unwrap();
    server.tick().unwrap();
    db.tick().unwrap();
    assert_eq!(
        ids(opened_rows(block_on(edge_subscription.next_raw()).unwrap())),
        BTreeSet::from([published]),
        "Edge maintained facades consume Global result members instead of raw local rows"
    );
    let refresh_attachment = db
        .attach_query_with_opts(&prepared, edge_subscribe_opts())
        .expect("refresh a deduplicated Edge attachment");
    db.tick().unwrap();
    assert!(
        !db.query_attachment_is_covered(&refresh_attachment),
        "a deduplicated attachment must request a later logical receipt"
    );
    server.tick().unwrap();
    db.tick().unwrap();
    assert!(db.query_attachment_is_covered(&refresh_attachment));
    db.detach_query(refresh_attachment);
    assert_eq!(
        ids(block_on(
            db.all_for_identity(&prepared, edge_subscribe_opts(), AuthorSubject::SYSTEM,)
        )
        .unwrap()),
        BTreeSet::from([published, server_overemitted]),
        "serving hosts remain TrustedServing and do not consume a client result cache"
    );
}

#[test]
fn client_settled_file_member_reads_bytes_for_bound_id_read() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("files")
                    .column("mime_type", PublicColumnType::Text)
                    .column("data", PublicColumnType::Bytea),
            )
            .table(PublicTableSchemaBuilder::new("attachments").fk_column("file_id", "files")),
    );
    let client_author = AuthorSubject::for_test_bytes([0xc2; 16]);
    let server = open_core(0x5f, AuthorSubject::SYSTEM, &schema);
    let db = open_db(0xc2, client_author, &schema);
    let bytes = vec![0, 1, 9, 3, 255, 64, 128, 200];
    let file = seed(
        &server,
        "files",
        BTreeMap::from([
            (
                "mime_type".to_owned(),
                Value::String("application/x-proof".to_owned()),
            ),
            ("data".to_owned(), Value::Bytes(bytes.clone())),
        ]),
    );
    // Keep an attachment-shaped policy-evidence row in the serving snapshot:
    // the file payload must still be materialized from the file member itself.
    seed(
        &server,
        "attachments",
        BTreeMap::from([("file_id".to_owned(), Value::Uuid(file.0))]),
    );
    let query = Query::from("files").filter(eq(col("id"), lit(file.0)));
    let prepared = prepared(&db, &query);
    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(db.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, client_author);
    let attachment = db
        .attach_query_with_opts(&prepared, edge_subscribe_opts())
        .expect("attach file coverage");
    db.tick().unwrap();
    server.tick().unwrap();
    db.tick().unwrap();
    assert!(db.query_attachment_is_covered(&attachment));
    let rows = block_on(db.all(&prepared, edge_subscribe_opts())).unwrap();
    assert_eq!(
        rows.len(),
        1,
        "settled file member must materialize as an Edge row"
    );
    assert_eq!(rows[0].row_uuid(), file);
    let table = schema
        .tables
        .iter()
        .find(|table| table.name == "files")
        .unwrap();
    let Value::Bytes(handle) = rows[0].cell(table, "data").unwrap() else {
        panic!("file data must be ordinary bytes");
    };
    assert!(
        !handle.is_empty(),
        "the received file row retains its byte payload"
    );
}

#[test]
fn propagated_authoritative_reset_uses_delivered_binding_view() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);

    let query = Query::from("todos");
    let prepared = prepared(&client, &query);
    let opts = ReadOpts {
        tier: DurabilityTier::Local,
        local_updates: LocalUpdates::Deferred,
        propagation: Propagation::Full,
        include_deleted: false,
        ..ReadOpts::default()
    };
    let mut subscription = block_on(client.subscribe(&prepared, opts.clone())).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());

    let live_row = row(0x7c);
    let tx_id = client
        .node
        .node
        .borrow_mut()
        .commit_mergeable_settled(
            MergeableCommit::new("todos", live_row, client.next_now_ms())
                .made_by(client_author)
                .permission_subject(client_author)
                .cells(cells("delivered reset", false, client_author)),
        )
        .unwrap();
    let delivered_binding_view_key = BindingViewKey::new(
        prepared.shape().shape_id(),
        prepared.binding().binding_id(),
        RegisterShapeOptions {
            tier: opts.tier,
            read_view: opts.read_view,
            ..RegisterShapeOptions::default()
        }
        .read_view_key(),
    );
    client
        .node
        .node
        .borrow_mut()
        .inject_pending_authoritative_reset_for_test(
            delivered_binding_view_key,
            [ResultMemberEntry::row((
                "todos".to_owned().into(),
                live_row,
                tx_id,
            ))],
            GlobalTime(42),
        );
    client
        .node
        .node
        .borrow_mut()
        .reset_subscription_snapshot_for_link_call_count();

    let changed = client.refresh_subscriptions().unwrap();
    assert_eq!(changed, 1);
    assert_eq!(
        client
            .node
            .node
            .borrow()
            .subscription_snapshot_for_link_call_count(),
        0,
        "propagated resets are delivered under the app subscription binding view, not the upstream global coverage key"
    );
    let event = block_on(subscription.next_raw()).unwrap();
    let SubscriptionEvent::Delta {
        reset,
        added,
        updated,
        removed,
        settled,
        ..
    } = event
    else {
        panic!("expected subscription delta");
    };
    assert!(reset);
    assert!(
        !settled,
        "this synthetic unit injects only the delivered binding-view reset; real upstream traffic also advances the global coverage settle stamp"
    );
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].row_uuid(), live_row);
}

#[test]
fn view_update_is_not_empty_when_it_only_carries_program_facts() {
    let subscription = crate::protocol::SubscriptionKey {
        shape_id: crate::query::ShapeId(uuid::Uuid::from_bytes([0x11; 16])),
        binding_id: crate::query::BindingId(uuid::Uuid::from_bytes([0x22; 16])),
        read_view: Default::default(),
    };
    let empty = SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription,
        settled_through: crate::time::GlobalTime(0),
        reset_result_set: false,
        version_carriers: Vec::new(),
        peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
        result_member_adds: Vec::new(),
        result_member_removes: Vec::new(),
        terminal_operations: Vec::new(),
        program_fact_adds: Vec::new(),
        program_fact_removes: Vec::new(),
    });
    assert!(view_update_is_empty(&empty));

    let fact_only = SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription,
        settled_through: crate::time::GlobalTime(0),
        reset_result_set: false,
        version_carriers: Vec::new(),
        peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
        result_member_adds: Vec::new(),
        result_member_removes: Vec::new(),
        terminal_operations: Vec::new(),
        program_fact_adds: vec![crate::protocol::ViewFactEntry::PathCorrelationCoverage(
            crate::protocol::PathCorrelationCoverageEntry {
                path: "owner".to_owned(),
                source_table: "todos".to_owned().into(),
                source_row: row(1),
                correlation_key: vec![1],
                complete: true,
            },
        )],
        program_fact_removes: Vec::new(),
    });
    assert!(!view_update_is_empty(&fact_only));
}
