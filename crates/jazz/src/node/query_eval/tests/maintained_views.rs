//! maintained views query-evaluation tests.

use super::*;
use futures::executor::block_on;

fn covered_input_rows(update: &SyncMessage, additions: bool) -> BTreeSet<RowUuid> {
    let SyncMessage::ViewUpdate(payload) = update else {
        panic!("expected ViewUpdate");
    };
    let facts = if additions {
        &payload.program_fact_adds
    } else {
        &payload.program_fact_removes
    };
    facts
        .iter()
        .filter_map(|fact| match fact {
            ProgramFactEntry::CoveredInput(input) => Some(input.source_row),
            _ => None,
        })
        .collect()
}

/// These direct controls model an actual trusted backend reader.  The
/// subscription's immutable policy binding must therefore be installed before
/// a peer rehydrates it; an unscoped test `Subscribe` followed by a SYSTEM
/// peer would exercise the deliberately rejected scope-replacement path.
fn subscribe_query_binding_as_system(
    node: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    binding: &Binding,
) {
    let values = shape
        .params()
        .keys()
        .map(|name| {
            binding
                .values()
                .get(name)
                .cloned()
                .expect("bound parameter")
        })
        .collect();
    node.apply_sync_message_settled(SyncMessage::Subscribe(Subscribe {
        shape_id: shape.shape_id(),
        subscription: SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: RegisterShapeOptions::default().read_view_key(),
        },
        values,
        known_state: None,
        delegated_session: Some(crate::protocol::DelegatedSessionBinding {
            identity: AuthorSubject::SYSTEM,
            claims: BTreeMap::new(),
        }),
    }))
    .expect("register SYSTEM-scoped test subscription");
}

/// A direct maintained-view opening must not wait for a cold source that may
/// require peer progress after this call returns. The publication owner keeps
/// Stream A gated until it receives the first complete Stream B snapshot.
#[test]
fn maintained_open_defers_cold_multisink_witnesses_to_its_publication_owner() {
    let test_schema = schema();
    let column_families = test_schema.column_families();
    let families = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let storage = groove::storage::YieldingStorage::wrap(
        groove::storage::MemoryStorage::new(&families).expect("open memory storage"),
    );
    let mut node = NodeState::new(NodeUuid::from_bytes([23; 16]), test_schema, storage.clone())
        .expect("open yielding node");
    let issue = row(23);
    let shape = Query::from("issues")
        .select(["title", "state", "assignee", "priority"])
        .validate(&schema())
        .expect("validate issues query");
    let binding = shape.bind(BTreeMap::new()).expect("bind issues query");
    let tx_id = node
        .commit_mergeable_settled(
            MergeableCommit::new("issues", issue, 23_000)
                .made_by(AuthorSubject::SYSTEM)
                .cells(BTreeMap::from([
                    ("title".to_owned(), Value::String("cold issue".to_owned())),
                    ("state".to_owned(), Value::String("open".to_owned())),
                    ("assignee".to_owned(), Value::Uuid(author(23).test_uuid())),
                    ("priority".to_owned(), Value::U64(23)),
                ])),
        )
        .expect("commit cold issue");
    node.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalTime(23)),
        Some(DurabilityTier::Global),
    )
    .expect("accept cold issue");
    storage.evict_all();
    let (shape, binding, plan) = node
        .prepare_query_binding_for_link_in_authorization_mode(
            &shape,
            &binding,
            DurabilityTier::Local,
            AuthorSubject::SYSTEM,
            QueryAuthorizationMode::ClientLocal,
        )
        .expect("prepare cold maintained query");
    let (local, initial) = node
        .open_maintained_view_subscription_in_authorization_mode(
            &shape,
            &binding,
            AuthorSubject::SYSTEM,
            DurabilityTier::Local,
            &ReadViewSpec::default(),
            Some(plan),
            QueryAuthorizationMode::ClientLocal,
        )
        .expect("open cold maintained snapshot without waiting for a peer turn");
    assert!(!local.initial_received);
    assert_eq!(initial.root_count, 0);
}

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
    subscribe_query_binding_as_system(&mut server, &shape, &binding);
    register_query_shape(&mut client, &shape, opts.clone());
    subscribe_query_binding_as_system(&mut client, &shape, &binding);

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
    let authority_result_key = client
        .authority_result_key_for_subscription(subscription)
        .expect("applied ViewUpdate registers its exact authority receipt");
    assert!(client.has_settled_authority_result(&authority_result_key));

    let (
        strict_receiver,
        _strict_maintained,
        _strict_schemas,
        strict_initial,
        _strict_tables,
        strict_received,
        _strict_inputs,
    ) = client
        .open_seeded_relay_edge_subscription_view_with_waker(
            &shape,
            &binding,
            AuthorSubject::SYSTEM,
            &ReadViewSpec::default(),
            subscription.read_view,
            authority_result_key.clone(),
            None,
        )
        .expect("open strict receiver from exact authority closure");
    assert!(strict_received);
    assert!(
        !strict_initial.terminal_operations.is_empty(),
        "strict receiver installs its initial authority closure before local writes"
    );

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
    assert_eq!(
        client
            .current_rows("issues", DurabilityTier::Local)
            .expect("read local current winner")
            .into_iter()
            .find(|row| row.row_uuid() == issue)
            .and_then(|row| row.cell(client.table("issues").expect("issues table"), "title")),
        Some(Value::String("updated title".to_owned())),
        "the local pending/current relation itself selects the new version"
    );
    block_on(client.drive_ready_query_runtime()).expect("drive local write through active graphs");
    assert!(
        matches!(
            strict_receiver.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ),
        "strict remote receiver must exclude an unacknowledged local successor"
    );

    let update = client
        .drain_local_maintained_view_subscription(&mut local, Some(authority_result_key.clone()))
        .expect("drain client-local maintained update")
        .expect("ordinary content update produces a delta");
    let LocalMaintainedViewSubscriptionUpdate::Structured {
        terminal_operations,
    } = update
    else {
        panic!("public root collector must use the shared structured terminal reducer");
    };
    assert!(
        !terminal_operations.is_empty(),
        "the ordinary local content update must reach the same terminal reducer"
    );
    let updated = terminal_operations
        .iter()
        .find_map(|operation| match &operation.edit {
            groove::ivm::TerminalEdit::Insert { value, .. }
            | groove::ivm::TerminalEdit::Update { value, .. } => Some(OwnedRecord::new(
                value.clone(),
                operation.root_descriptor.clone(),
            )),
            groove::ivm::TerminalEdit::Remove { .. } | groove::ivm::TerminalEdit::Move { .. } => {
                None
            }
        })
        .expect("local update produces a root payload through the terminal reducer");
    assert_eq!(
        updated.get("user_title").expect("decode terminal title"),
        Value::String("updated title".to_owned())
    );

    client
        .apply_sync_message_settled(SyncMessage::FateUpdate {
            tx_id: updated_tx,
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            global_time: None,
            durability: None,
        })
        .expect("reject local successor");
    let restored = client
        .drain_local_maintained_view_subscription(&mut local, Some(authority_result_key.clone()))
        .expect("drain rejected local successor")
        .expect("rejection restores the authority source through the same graph");
    let LocalMaintainedViewSubscriptionUpdate::Structured {
        terminal_operations,
    } = restored
    else {
        panic!("retraction must use the shared structured terminal reducer");
    };
    let restored = terminal_operations
        .iter()
        .find_map(|operation| match &operation.edit {
            groove::ivm::TerminalEdit::Insert { value, .. }
            | groove::ivm::TerminalEdit::Update { value, .. } => Some(OwnedRecord::new(
                value.clone(),
                operation.root_descriptor.clone(),
            )),
            groove::ivm::TerminalEdit::Remove { .. } | groove::ivm::TerminalEdit::Move { .. } => {
                None
            }
        })
        .expect("retraction emits the restored root row");
    assert_eq!(
        restored
            .get("user_title")
            .expect("decode restored terminal title"),
        Value::String("issue-0".to_owned()),
        "retraction removes the local winner and reveals the exact authority carrier"
    );

    // A newer authority closure arriving while another local successor is
    // pending is just another input to the same per-source arg-max. It must
    // deterministically replace that pending winner, not create a second
    // remote result path.
    client
        .commit_mergeable_settled(
            MergeableCommit::new("issues", issue, 2_500)
                .made_by(AuthorSubject::SYSTEM)
                .parents(vec![initial_tx])
                .cells(BTreeMap::from([
                    (
                        "title".to_owned(),
                        Value::String("second pending title".to_owned()),
                    ),
                    ("state".to_owned(), Value::String("open".to_owned())),
                    ("assignee".to_owned(), Value::Uuid(uuid::Uuid::nil())),
                    ("priority".to_owned(), Value::U64(0)),
                ])),
        )
        .expect("commit second pending local successor");
    let _ = client
        .drain_local_maintained_view_subscription(&mut local, Some(authority_result_key.clone()))
        .expect("drain second pending local successor")
        .expect("second pending local successor reaches the shared graph");
    let authority_update_tx = server
        .commit_mergeable_settled(
            MergeableCommit::new("issues", issue, 3_000)
                .made_by(AuthorSubject::SYSTEM)
                .parents(vec![initial_tx])
                .cells(BTreeMap::from([
                    (
                        "title".to_owned(),
                        Value::String("authority title".to_owned()),
                    ),
                    ("state".to_owned(), Value::String("open".to_owned())),
                    ("assignee".to_owned(), Value::Uuid(uuid::Uuid::nil())),
                    ("priority".to_owned(), Value::U64(0)),
                ])),
        )
        .expect("commit newer authority successor");
    server
        .apply_fate_update(
            authority_update_tx,
            Fate::Accepted,
            Some(GlobalTime(2)),
            Some(DurabilityTier::Global),
        )
        .expect("accept newer authority successor");
    let authority_update = peer
        .query_update_for_subscription_with_opts(&mut server, subscription, &shape, &binding, opts)
        .expect("serve newer authority closure")
        .expect("authority closure changed");
    client
        .apply_sync_message_settled(authority_update)
        .expect("install newer exact authority closure");
    let concurrent = client
        .drain_local_maintained_view_subscription(&mut local, Some(authority_result_key))
        .expect("drain concurrent authority successor")
        .expect("new authority source replaces the pending local winner");
    let LocalMaintainedViewSubscriptionUpdate::Structured {
        terminal_operations,
    } = concurrent
    else {
        panic!("authority replacement must use the shared structured terminal reducer");
    };
    let concurrent = terminal_operations
        .iter()
        .find_map(|operation| match &operation.edit {
            groove::ivm::TerminalEdit::Insert { value, .. }
            | groove::ivm::TerminalEdit::Update { value, .. } => Some(OwnedRecord::new(
                value.clone(),
                operation.root_descriptor.clone(),
            )),
            groove::ivm::TerminalEdit::Remove { .. } | groove::ivm::TerminalEdit::Move { .. } => {
                None
            }
        })
        .expect("authority replacement emits the current root row");
    assert_eq!(
        concurrent
            .get("user_title")
            .expect("decode authority title"),
        Value::String("authority title".to_owned()),
        "higher-HLC authority version wins deterministically while the local write remains pending"
    );
}

/// A relay may open its downstream maintained receiver after the selected
/// authority ViewUpdate has already become live.  Opening must seed from that
/// retained exact authority result rather than waiting for a second upstream
/// delta which may never arrive.
#[test]
fn relay_edge_open_after_live_authority_receipt_seeds_initial_membership() {
    let (_server_dir, mut server) = open_node();
    let (_client_dir, mut client) = open_node();
    let issue = row(41);
    let shape = Query::from("issues")
        .select(["title", "state", "assignee", "priority"])
        .validate(&schema())
        .expect("validate issues query");
    let binding = shape.bind(BTreeMap::new()).expect("bind issues query");
    let opts = RegisterShapeOptions {
        tier: DurabilityTier::Edge,
        ..RegisterShapeOptions::default()
    };
    register_query_shape(&mut server, &shape, opts.clone());
    subscribe_query_binding_as_system(&mut server, &shape, &binding);
    register_query_shape(&mut client, &shape, opts.clone());
    subscribe_query_binding_as_system(&mut client, &shape, &binding);

    commit_global_issue(&mut server, 41, "open", author(41), 41);
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };
    let mut server_peer = PeerState::edge_client(AuthorSubject::SYSTEM);
    let authority = server_peer
        .rehydrate_query_for_subscription_with_opts(
            &mut server,
            subscription,
            &shape,
            &binding,
            opts,
        )
        .expect("serve authority receipt")
        .expect("authority receipt is ready");
    client
        .apply_sync_message_settled(authority)
        .expect("apply authority receipt before opening relay child");
    let authority_key = client
        .authority_result_key_for_subscription(subscription)
        .expect("exact retained authority receipt");

    let (_receiver, _maintained, _schemas, transitions, _tables, initial_received, _inputs) =
        client
            .open_seeded_relay_edge_subscription_view_with_waker(
                &shape,
                &binding,
                AuthorSubject::SYSTEM,
                &ReadViewSpec::default(),
                subscription.read_view,
                authority_key,
                None,
            )
            .expect("open relay child after live authority receipt");
    assert!(
        initial_received,
        "opening must seed from already-live authority membership"
    );
    assert!(
        !transitions.terminal_operations.is_empty(),
        "the retained CoveredInput closure must seed the receiver-local terminal"
    );
    // The receiver may retain internal member bookkeeping, but the public
    // initial tree comes from its terminal reducer, never authority output.
    assert!(transitions.result_payload_adds.is_empty());
    assert!(
        transitions.terminal_operations.iter().any(|operation| {
            matches!(
                &operation.edit,
                groove::ivm::TerminalEdit::Insert { value, .. }
                    if OwnedRecord::new(value.clone(), operation.root_descriptor.clone())
                        .get("row_uuid")
                        .is_ok_and(|value| value == Value::Uuid(issue.0))
            )
        }),
        "the seeded terminal inserts the authority-authorized row"
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
    let initial_rows = covered_input_rows(&initial, true);
    assert!(initial_rows.contains(&resource1));
    assert!(!initial_rows.contains(&resource2));

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
    assert!(covered_input_rows(&grant, true).contains(&resource2));

    delete_global(&mut core, "teamTeamMemberships", row(302), 18, 8);
    let revoke = peer.query_update(&mut core, &shape, &binding).unwrap();
    let removed = covered_input_rows(&revoke, false);
    assert!(removed.contains(&resource1));
    assert!(removed.contains(&resource2));
}
