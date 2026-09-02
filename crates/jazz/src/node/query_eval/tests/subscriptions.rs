//! subscriptions query-evaluation tests.

use super::*;
use crate::legacy_test_future::FutureResolveExt as _;
use crate::peer::PeerState;
use crate::protocol::{DelegatedSessionBinding, PolicyBindingKey};

/// A receiver has only the exact policy-scoped source closure delivered by its
/// authority.  Test reads from that node must therefore use the client-local
/// execution path, never the trusted-serving path that expects complete
/// current-table capabilities.
fn receiver_rows(
    node: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    binding: &Binding,
    tier: DurabilityTier,
) -> Vec<CurrentRow> {
    node.query_rows_for_client(shape, binding, tier, AuthorSubject::SYSTEM)
        .resolve()
        .expect("read receiver-local covered-input result")
}

fn receiver_rows_in_read_view(
    node: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    binding: &Binding,
    tier: DurabilityTier,
    read_view: &ReadViewSpec,
) -> Vec<CurrentRow> {
    node.query_relation_snapshot_for_client(shape, binding, tier, AuthorSubject::SYSTEM, read_view)
        .resolve()
        .expect("read receiver-local covered-input relation")
        .rows
}

/// Direct test peers terminate SYSTEM themselves. Their counterpart's
/// subscription must record that same immutable reader scope; an unscoped
/// `Subscribe` models neither a direct peer nor a multiplexed relay.
fn subscribe_query_binding_as_system_with_opts(
    node: &mut NodeState<RocksDbStorage>,
    shape: &ValidatedQuery,
    binding: &Binding,
    opts: RegisterShapeOptions,
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
            read_view: opts.read_view_key(),
        },
        values,
        known_state: None,
        delegated_session: Some(DelegatedSessionBinding {
            identity: AuthorSubject::SYSTEM,
            claims: BTreeMap::new(),
        }),
    }))
    .expect("register SYSTEM-scoped test subscription");
}

fn graph_contains_point_scan(graph: &GraphBuilder) -> bool {
    match graph {
        GraphBuilder::Table {
            scan: Some(groove::ivm::StaticScanSpec::Point(_)),
            ..
        } => true,
        GraphBuilder::Recursive { seed, step, .. } => {
            graph_contains_point_scan(seed) || graph_contains_point_scan(step)
        }
        GraphBuilder::RecursiveStepWitness { recursive } => graph_contains_point_scan(recursive),
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
        | GraphBuilder::InputSource { .. }
        | GraphBuilder::Index { .. }
        | GraphBuilder::FrontierSource { .. }
        | GraphBuilder::BindingSource { .. } => false,
    }
}

#[test]
fn retained_root_window_descriptor_requires_exact_source_order_and_containment() {
    let (_dir, node) = open_node();
    let source = Query::from("issues")
        .order_by("state", OrderDirection::Asc)
        .offset(8)
        .limit(16)
        .validate(&node.catalogue.schema)
        .expect("validate source window");
    let descriptor = RetainedRootWindowSource::for_shape(&source);

    let contained = Query::from("issues")
        .order_by("state", OrderDirection::Asc)
        .offset(10)
        .limit(2)
        .validate(&node.catalogue.schema)
        .expect("validate contained window");
    assert!(descriptor.contains_target(&contained));
    assert_eq!(descriptor.relative_window_for(&contained), (2, Some(2)));

    let same_window = Query::from("issues")
        .order_by("state", OrderDirection::Asc)
        .offset(8)
        .limit(16)
        .validate(&node.catalogue.schema)
        .expect("validate same window");
    assert!(descriptor.contains_target(&same_window));
    assert_eq!(
        descriptor.relative_window_for(&same_window),
        (0, Some(16)),
        "an exact page is still a post-window source, not a raw table input"
    );

    let outside = Query::from("issues")
        .order_by("state", OrderDirection::Asc)
        .offset(20)
        .limit(8)
        .validate(&node.catalogue.schema)
        .expect("validate outside window");
    assert!(!descriptor.contains_target(&outside));

    let different_order = Query::from("issues")
        .order_by("state", OrderDirection::Desc)
        .offset(10)
        .limit(2)
        .validate(&node.catalogue.schema)
        .expect("validate differently ordered window");
    assert!(
        !descriptor.contains_target(&different_order),
        "matching table and offsets do not erase compiler-owned order/tie semantics"
    );
}

#[test]
fn retained_root_window_sources_do_not_cross_policy_scopes() {
    let (_dir, mut node) = open_node();
    node.set_non_durable_client();
    let source = Query::from("issues")
        .order_by("state", OrderDirection::Asc)
        .offset(8)
        .limit(16)
        .validate(&node.catalogue.schema)
        .expect("validate source window");
    let binding = source.bind(BTreeMap::new()).expect("bind source window");
    let view = BindingViewKey::new(
        source.shape_id(),
        binding.binding_id(),
        RegisterShapeOptions {
            tier: DurabilityTier::Edge,
            ..RegisterShapeOptions::default()
        }
        .read_view_key(),
    );
    let alice = PolicyBindingKey::from_delegated_session(&DelegatedSessionBinding {
        identity: author(0x51),
        claims: BTreeMap::new(),
    });
    let bob = PolicyBindingKey::from_delegated_session(&DelegatedSessionBinding {
        identity: author(0x52),
        claims: BTreeMap::new(),
    });
    let descriptor = RetainedRootWindowSource::for_shape(&source);
    node.query.retained_root_window_sources.insert(
        AuthorityResultKey::policy_scoped(view, alice),
        descriptor.clone(),
    );
    node.query
        .retained_root_window_sources
        .insert(AuthorityResultKey::policy_scoped(view, bob), descriptor);

    let target = Query::from("issues")
        .order_by("state", OrderDirection::Asc)
        .offset(10)
        .limit(2)
        .validate(&node.catalogue.schema)
        .expect("validate contained target");
    assert!(
        node.client_settled_binding_view_for_query(
            &target,
            &binding,
            DurabilityTier::Local,
            &ReadViewSpec::default(),
        )
        .is_none(),
        "a binding-only Local facade must not guess which of two policy-scoped pages it may reuse"
    );
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

/// Two delegated sessions can receive interleaved updates for the same
/// canonical query binding without sharing authority membership.
///
/// alice authority ──reset/add/remove──► relay receipt A
/// bob authority ────reset/add─────────► relay receipt B
///
/// The relay stores both receipts independently even though the application
/// query, parameter binding, and read view are identical.
#[test]
fn policy_scoped_authority_results_do_not_collide_on_one_binding_view() {
    let (dir, mut relay) = open_node();
    let shape = Query::from("issues")
        .validate(&relay.catalogue.schema)
        .unwrap();
    register_query_shape(&mut relay, &shape, RegisterShapeOptions::default());
    let _binding = shape.bind(BTreeMap::new()).unwrap();

    let subscription = |byte, identity| Subscribe {
        shape_id: shape.shape_id(),
        subscription: SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: BindingId(uuid::Uuid::from_bytes([byte; 16])),
            read_view: Default::default(),
        },
        values: Vec::new(),
        known_state: None,
        delegated_session: Some(crate::protocol::DelegatedSessionBinding {
            identity,
            claims: BTreeMap::new(),
        }),
    };
    let alice = author(1);
    let bob = author(2);
    let alice_subscribe = subscription(0xa1, alice);
    let bob_subscribe = subscription(0xb2, bob);
    relay
        .apply_sync_message_settled(SyncMessage::Subscribe(alice_subscribe.clone()))
        .unwrap();
    relay
        .apply_sync_message_settled(SyncMessage::Subscribe(bob_subscribe.clone()))
        .unwrap();

    let alice_key = relay
        .authority_result_key_for_subscription(alice_subscribe.subscription)
        .unwrap();
    let bob_key = relay
        .authority_result_key_for_subscription(bob_subscribe.subscription)
        .unwrap();
    assert_eq!(alice_key.binding_view, bob_key.binding_view);
    assert_ne!(alice_key, bob_key);
    let mut peer = PeerState::relay();
    peer.set_subscription_authority_result_source(alice_subscribe.subscription, alice_key.clone());
    peer.set_subscription_authority_result_source(bob_subscribe.subscription, bob_key.clone());
    assert_eq!(
        peer.canonical_subscription_settlement_time(&relay, alice_subscribe.subscription),
        crate::time::GlobalTime::default(),
        "an unclaimed source closure must not borrow Bob's receipt"
    );
    assert!(
        relay
            .unique_authority_result_key_for_binding_view(alice_key.binding_view)
            .is_none(),
        "a BindingViewKey-only reader must refuse an ambiguous multiplexed receipt"
    );

    // Opening a subscription establishes an in-memory, policy-scoped usage
    // key. Persisted receiver state begins only once the authority supplies a
    // complete CoveredInput closure, not from a synthetic result-member set.
    drop(dir);
}

/// Lifecycle bookkeeping follows the exact delegated receipt, rather than
/// collapsing through the shared query binding view. In particular, an
/// opening reset for Alice and a deferred reset for Bob can each be consumed,
/// requeued, and completed without either stream starving the other.
#[test]
fn interleaved_policy_scoped_lifecycles_keep_reset_and_defer_receipts_separate() {
    let (_dir, mut relay) = open_node();
    let shape = Query::from("issues")
        .validate(&relay.catalogue.schema)
        .unwrap();
    register_query_shape(&mut relay, &shape, RegisterShapeOptions::default());

    let subscription = |byte, identity| Subscribe {
        shape_id: shape.shape_id(),
        subscription: SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: BindingId(uuid::Uuid::from_bytes([byte; 16])),
            read_view: Default::default(),
        },
        values: Vec::new(),
        known_state: None,
        delegated_session: Some(DelegatedSessionBinding {
            identity,
            claims: BTreeMap::new(),
        }),
    };
    let alice_subscribe = subscription(0xa1, author(1));
    let bob_subscribe = subscription(0xb2, author(2));
    relay
        .apply_sync_message_settled(SyncMessage::Subscribe(alice_subscribe.clone()))
        .unwrap();
    relay
        .apply_sync_message_settled(SyncMessage::Subscribe(bob_subscribe.clone()))
        .unwrap();
    let update = |subscription, reset_result_set: bool, opening_pending, defer_settlement| {
        let program_fact_adds = reset_result_set
            .then(|| {
                vec![
                    crate::protocol::ProgramFactEntry::ProgramSourceCoverage(
                        crate::protocol::ProgramSourceCoverageEntry {
                            source: crate::protocol::ProgramSourceId {
                                table: "issues".to_owned().into(),
                                path: vec![crate::protocol::ProgramSourceRole::Root],
                            },
                            complete: true,
                        },
                    ),
                    crate::protocol::ProgramFactEntry::ProgramSourceCoverage(
                        crate::protocol::ProgramSourceCoverageEntry {
                            source: crate::protocol::ProgramSourceId {
                                table: "users".to_owned().into(),
                                path: vec![
                                    crate::protocol::ProgramSourceRole::Root,
                                    crate::protocol::ProgramSourceRole::Alias(
                                        "include:18446744073709551615:0".to_owned(),
                                    ),
                                ],
                            },
                            complete: true,
                        },
                    ),
                ]
            })
            .unwrap_or_default();
        crate::node::ViewUpdateParts {
            subscription,
            settled_through: crate::time::GlobalTime(7),
            defer_settlement,
            reset_result_set,
            version_carriers: Vec::new(),
            peer_complete_tx_payload_refs: Vec::new(),
            authorization_progress: Some(3),
            opening_pending,
            result_member_adds: Vec::new(),
            result_member_removes: Vec::new(),
            program_fact_adds,
            program_fact_removes: Vec::new(),
        }
    };

    relay
        .apply_view_update(update(alice_subscribe.subscription, true, true, false))
        .resolve()
        .unwrap();
    relay
        .apply_view_update(update(bob_subscribe.subscription, true, false, false))
        .resolve()
        .unwrap();

    let alice_key = relay
        .authority_result_key_for_subscription(alice_subscribe.subscription)
        .unwrap();
    let bob_key = relay
        .authority_result_key_for_subscription(bob_subscribe.subscription)
        .unwrap();
    assert_eq!(alice_key.binding_view, bob_key.binding_view);
    assert_ne!(alice_key, bob_key);
    assert!(relay.opening_pending_for_authority_result(&alice_key));
    assert!(!relay.publication_deferred_for_authority_result(&alice_key));
    assert!(!relay.opening_pending_for_authority_result(&bob_key));
    assert!(!relay.publication_deferred_for_authority_result(&bob_key));
    assert_eq!(relay.applied_authority_result_generation(&alice_key), 1);
    assert_eq!(relay.applied_authority_result_generation(&bob_key), 1);

    let pending = relay.take_pending_authoritative_resets();
    assert_eq!(
        pending,
        BTreeSet::from([alice_key.clone(), bob_key.clone()])
    );
    // Bob can defer a later publication while Alice's opening continues. That
    // deferred marker is also exact-policy state, not binding-view state.
    relay
        .apply_view_update(update(bob_subscribe.subscription, false, false, true))
        .resolve()
        .unwrap();
    assert!(relay.publication_deferred_for_authority_result(&bob_key));
    assert!(!relay.publication_deferred_for_authority_result(&alice_key));

    for authority_result_key in &pending {
        relay.defer_authoritative_reset(authority_result_key);
    }
    assert_eq!(
        relay.take_pending_authoritative_resets(),
        BTreeSet::from([alice_key.clone(), bob_key.clone()]),
        "each exact reset can be deferred and retried without ambiguity"
    );

    relay
        .apply_view_update(update(alice_subscribe.subscription, false, false, false))
        .resolve()
        .unwrap();
    relay
        .apply_view_update(update(bob_subscribe.subscription, false, false, false))
        .resolve()
        .unwrap();
    assert!(!relay.opening_pending_for_authority_result(&alice_key));
    assert!(!relay.publication_deferred_for_authority_result(&bob_key));
    assert!(
        relay.applied_authority_result_generation(&alice_key) > 1,
        "Alice's lifecycle keeps progressing after her opening reset"
    );
    assert!(
        relay.applied_authority_result_generation(&bob_key) > 1,
        "Bob's deferred lifecycle keeps progressing independently"
    );
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
        receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Global)
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
        receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Global)
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
        receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Global)
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0), row(1)])
    );

    // A newly authored content version can restore the same row in one
    // transaction. The result member names that content transaction, so the
    // serving path must include its `Restored` register sibling before the
    // transaction bundle is marked emitted. A separate receiver otherwise
    // learns the new body but keeps the row deleted in its ordinary current
    // register.
    delete_global(&mut server, "notes", row(0), 5, 5);
    let second_removal = peer
        .query_update(&mut server, &shape, &binding)
        .expect("serve scalar second removal update");
    reader
        .apply_sync_message_settled(second_removal)
        .expect("separate reader applies scalar second removal update");
    assert!(
        receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Global)
            .iter()
            .all(|current| current.row_uuid() != row(0)),
        "the reader must observe the precondition deletion"
    );
    let same_tx_open = OpenTransactionId::new();
    server
        .open_exclusive(same_tx_open)
        .expect("open exclusive transaction for paired content and restoration");
    server
        .tx_write(
            same_tx_open,
            "notes",
            row(0),
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("restored-with-content".to_owned()),
            )]),
            None,
        )
        .expect("stage visible content in paired transaction");
    server
        .tx_write(
            same_tx_open,
            "notes",
            row(0),
            BTreeMap::<String, Value>::new(),
            Some(crate::tx::DeletionEvent::Restored),
        )
        .expect("stage restoration register sibling in paired transaction");
    let (same_tx_restore, _) = server
        .commit_exclusive_settled(same_tx_open, AuthorSubject::SYSTEM, 6)
        .expect("commit content and restoration together");
    server
        .apply_fate_update(
            same_tx_restore,
            Fate::Accepted,
            Some(GlobalTime(6)),
            Some(DurabilityTier::Global),
        )
        .expect("accept content and restoration together");
    let same_tx_update = peer
        .query_update(&mut server, &shape, &binding)
        .expect("serve same-transaction content restoration");
    let same_tx_bundles = match &same_tx_update {
        SyncMessage::ViewUpdate(payload) => {
            crate::protocol::expand_version_carriers(&payload.version_carriers)
                .expect("same-transaction restore carriers should expand")
        }
        _ => panic!("scalar subscription must produce a view update"),
    };
    let same_tx_bundle = same_tx_bundles
        .iter()
        .find(|bundle| bundle.tx.tx_id == same_tx_restore)
        .expect("served update contains the visible content transaction");
    assert!(
        same_tx_bundle.versions.iter().any(|version| {
            version.table() == "notes"
                && version.row_uuid() == row(0)
                && version.deletion().is_none()
        }),
        "same transaction carries the visible content version"
    );
    assert!(
        same_tx_bundle.versions.iter().any(|version| {
            version.table() == "notes"
                && version.row_uuid() == row(0)
                && version.deletion() == Some(crate::tx::DeletionEvent::Restored)
        }),
        "same transaction carries the visible restoration winner"
    );
    reader
        .apply_sync_message_settled(same_tx_update)
        .expect("separate reader applies same-transaction restoration");
    assert!(
        receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Global)
            .iter()
            .any(|current| current.row_uuid() == row(0)),
        "ordinary reader lookup must expose the row restored alongside content"
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
        // Edge is the foreground/relay handoff frontier. A durable node
        // consumes its upstream Global frontier instead, so model the client
        // receiver explicitly rather than accidentally asserting that a
        // durable authority reads an Edge-only receipt.
        if tier == DurabilityTier::Edge {
            reader.set_non_durable_client();
        }
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
                .contains(&crate::node::query_engine::ProgramFactKey::VersionWitnesses),
            "{tier:?} scalar maintained view omits source version witnesses"
        );
        assert!(
            program
                .request
                .output
                .facts
                .contains(&crate::node::query_engine::ProgramFactKey::ReplacementWitnesses),
            "{tier:?} scalar maintained view retains replacement witnesses so a deletion/restore transition can name the current register winner"
        );
        let opts = RegisterShapeOptions {
            tier,
            ..RegisterShapeOptions::default()
        };
        let read_view = opts.read_view.clone();
        let subscription = SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: opts.read_view_key(),
        };
        for node in [&mut server, &mut reader] {
            register_query_shape(node, &shape, opts.clone());
            subscribe_query_binding_as_system_with_opts(node, &shape, &binding, opts.clone());
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
            receiver_rows_in_read_view(&mut reader, &shape, &binding, tier, &read_view).is_empty(),
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
            receiver_rows_in_read_view(&mut reader, &shape, &binding, tier, &read_view)
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
                receiver_rows_in_read_view(
                    &mut reader,
                    &shape,
                    &binding,
                    DurabilityTier::Edge,
                    &read_view,
                )
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
    subscribe_query_binding_as_system_with_opts(&mut server, &shape, &binding, edge_opts.clone());

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
    reader.set_non_durable_client();
    register_query_shape(&mut reader, &shape, edge_opts.clone());
    subscribe_query_binding_as_system_with_opts(&mut reader, &shape, &binding, edge_opts.clone());
    let update = PeerState::new()
        .rehydrate_query_with_opts(&mut server, &shape, &binding, edge_opts.clone())
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
        receiver_rows_in_read_view(
            &mut reader,
            &shape,
            &binding,
            DurabilityTier::Edge,
            &edge_opts.read_view,
        )
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0)]),
        "fresh reader matches the source's filter-before-argmax Edge view"
    );
}

#[test]
fn authority_result_key_is_explicit_and_does_not_replace_direct_edge_source() {
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

    node.set_relay_authority_session_owner_for_test();
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
    let relay_authority = AuthorityResultKey::policy_scoped(
        ordinary_direct,
        PolicyBindingKey::from_delegated_session(&DelegatedSessionBinding {
            identity: author(0x34),
            claims: BTreeMap::from([("role".to_owned(), Value::String("reader".to_owned()))]),
        }),
    );
    assert_eq!(
        relay_authority.binding_view, ordinary_direct,
        "the authority receipt shares canonical query routing, but retains the exact policy scope separately",
    );
    assert!(relay_authority.policy_binding.is_some());
}

#[test]
fn maintained_policy_point_subscription_keeps_full_current_source_for_deletion_liveness() {
    let schema = owner_policy_schema();
    let (_dir, mut node) = open_node_with_uuid(NodeUuid::from_bytes([0xc2; 16]), schema.clone());
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
    let program = node
        .compile_current_query_program_for_read_view(
            &shape,
            &binding,
            DurabilityTier::Global,
            AuthorSubject::SYSTEM,
            CurrentQueryProgramOutput::MaintainedView,
            &ReadViewSpec::default(),
        )
        .unwrap();
    assert!(
        program
            .request
            .output
            .facts
            .contains(&ProgramFactKey::VersionWitnesses),
        "a configured root read policy must keep Stream-B witnesses even for a System seed"
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
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload { program_fact_adds, .. })
            if program_fact_adds.iter().any(|fact| matches!(
                fact,
                crate::protocol::ProgramFactEntry::CoveredInput(input)
                    if input.source_row == target && input.version.tx == initial_tx
            ))
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
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload { program_fact_removes, .. })
            if program_fact_removes.iter().any(|fact| matches!(
                fact,
                crate::protocol::ProgramFactEntry::CoveredInput(input)
                    if input.source_row == target && input.version.tx == initial_tx
            ))
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
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload { program_fact_adds, .. })
            if program_fact_adds.iter().any(|fact| matches!(
                fact,
                crate::protocol::ProgramFactEntry::CoveredInput(input)
                    if input.source_row == target && input.version.tx == restored_tx
            ))
    ));
    delete_global(&mut node, "issues", target, 4, 4);
    let delete_update = peer.query_update(&mut node, &shape, &binding).unwrap();
    assert!(matches!(
        delete_update,
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload { program_fact_removes, .. })
            if program_fact_removes.iter().any(|fact| matches!(
                fact,
                crate::protocol::ProgramFactEntry::CoveredInput(input)
                    if input.source_row == target && input.version.tx == restored_tx
            ))
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
        receiver_rows(&mut reader, &shape, &alice_binding, DurabilityTier::Global)
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([row(0)])
    );
    assert_eq!(
        receiver_rows(&mut reader, &shape, &bob_binding, DurabilityTier::Global)
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
        receiver_rows(&mut reader, &shape, &alice_binding, DurabilityTier::Global)
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
        receiver_rows(&mut reader, &shape, &alice_binding, DurabilityTier::Global).len(),
        3
    );
}

#[test]
fn settled_binding_view_sources_reject_trusted_source_coverage_reopening() {
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

    let scoped_subscribe = Subscribe {
        shape_id: shape.shape_id(),
        subscription: SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: Default::default(),
        },
        values: vec![Value::String("alice".to_owned())],
        known_state: None,
        // The direct test peer is a trusted relay acting for its actual
        // SYSTEM reader. Keep the registration in the exact policy scope
        // that rehydration will later use; an unscoped test fixture is not a
        // valid predecessor under scope-isolated receipts.
        delegated_session: Some(DelegatedSessionBinding {
            identity: AuthorSubject::SYSTEM,
            claims: BTreeMap::new(),
        }),
    };
    register_query_shape(&mut server, &shape, RegisterShapeOptions::default());
    server
        .apply_sync_message_settled(SyncMessage::Subscribe(scoped_subscribe.clone()))
        .unwrap();
    register_query_shape(&mut reader, &shape, RegisterShapeOptions::default());
    reader
        .apply_sync_message_settled(SyncMessage::Subscribe(scoped_subscribe))
        .unwrap();

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
        .insert(ProgramFactKey::ProgramSourceCoverage(
            CoverageScope::Program,
        ));

    let error = reader
        .compile_query_program_request(request)
        .expect_err("a partial receiver must not reopen trusted source coverage");
    assert!(
        matches!(error, crate::node::Error::QueryCapability(_)),
        "only an authority can create a coverage closure; the receiver consumes it"
    );
}

#[test]
fn settled_binding_view_root_with_reference_include_sources_rejects_trusted_reopening() {
    // A receiver's retained view is derived from CoveredInput. It is not a
    // trusted source relation, so a mixed settled-root/current-auxiliary
    // request must fail rather than recreate authority source coverage.
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
        .insert(ProgramFactKey::ProgramSourceCoverage(
            CoverageScope::Program,
        ));

    let sources = format!("{:?}", request.reads);
    assert!(sources.contains("SettledBindingView"), "{sources}");
    assert!(
        !sources.contains("VisibleCurrent"),
        "all receiver sources are covered-input derived: {sources}"
    );
    assert!(
        matches!(
            reader.compile_query_program_request(request).resolve(),
            Err(crate::node::Error::QueryCapability(_))
        ),
        "a partial root plus include must not reopen current tables on a receiver"
    );
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
        result_member_adds,
        program_fact_adds,
        ..
    }) = &update
    else {
        panic!("expected view update");
    };
    assert!(
        result_member_adds.is_empty(),
        "peer receipts must not carry authority-rendered result members"
    );
    let covered_source_tables = program_fact_adds
        .iter()
        .filter_map(|fact| match fact {
            crate::protocol::ProgramFactEntry::CoveredInput(input) => {
                Some(input.version_table.to_string())
            }
            _ => None,
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        covered_source_tables,
        BTreeSet::from([
            "issues".to_owned(),
            "issue_members".to_owned(),
            "users".to_owned(),
        ])
    );
    reader.apply_sync_message_settled(update).unwrap();

    let local_rows = receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Local)
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();
    assert_eq!(local_rows, BTreeSet::from([row(0)]));
    let settled_rows = receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Global)
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<BTreeSet<_>>();
    assert_eq!(settled_rows, BTreeSet::from([row(0)]));
}
