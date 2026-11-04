//! Main-only admission-context regressions. No public pin-selection test API.
use crate::db::{
    Db, DbConfig, DbIdentity, ReadOpts, SubscriptionEvent, SubscriptionStream, block_on,
};
use crate::ids::{AuthorSubject, NodeUuid, RowUuid};
use crate::query::Query;
use crate::records::Value;
use crate::schema::JazzSchema;
use jazz_model::model::public_schema::{
    ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};
use std::collections::BTreeMap;

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([17; 16])
}

fn policies(p: PolicyExpr) -> TablePolicies {
    TablePolicies::new()
        .with_select(p)
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True)
}

fn open(s: JazzSchema) -> Db {
    let db = block_on(Db::open(DbConfig::new(
        s,
        DbIdentity {
            node: NodeUuid::from_bytes([9; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    db.set_authority();
    db
}

fn seed(db: &Db, t: &str, n: u8, cells: BTreeMap<String, Value>) {
    db.seed_settled_mergeable_for_bootstrap(
        t,
        RowUuid::from_bytes([n; 16]),
        AuthorSubject::SYSTEM,
        cells,
    )
    .unwrap();
}

fn admitted_view(db: &Db, schema: &JazzSchema) -> (Db, Box<dyn super::Transport>) {
    let bundle = block_on(db.publish_bundle(schema.public_schema())).unwrap();
    let (peer, transport) = super::duplex();
    let claims = db
        .inner
        .borrow()
        .claims
        .get(&alice())
        .cloned()
        .unwrap_or_default();
    let subscriber = db
        .accept_pinned_subscriber(transport, alice(), Some(claims), Some(bundle))
        .unwrap();
    (db.session_db(subscriber.slot), peer)
}

fn boolean_schema(allowed: bool) -> JazzSchema {
    JazzSchema::new(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .policies(policies(if allowed {
                        PolicyExpr::True
                    } else {
                        PolicyExpr::False
                    })),
            )
            .build(),
    )
    .unwrap()
}

/// Alice's two authenticated sessions share names, author and claims but have
/// opposite bundle policies. Retained registration and queries prepared on the
/// other handle must never replace the executing session's pin.
/// This lower-crate test inspects the session Db after real admission because
/// main intentionally exposes no public operation that manufactures pinned Dbs.
#[test]
fn registered_session_handles_keep_their_own_authenticated_bundle() {
    for preparing_allowed in [false, true] {
        let db = open(boolean_schema(true));
        seed(
            &db,
            "notes",
            1,
            BTreeMap::from([("body".into(), Value::String("secret".into()))]),
        );
        let (preparing, _peer_a) = admitted_view(&db, &boolean_schema(preparing_allowed));
        let (executing, _peer_b) = admitted_view(&db, &boolean_schema(!preparing_allowed));
        let query = preparing.prepare_query(&Query::from("notes")).unwrap();
        // Registration uses the admitted structural schema and its original
        // metadata. Governance must still come from the authenticated parent.
        let registered = block_on(executing.register_schema_view(boolean_schema(true))).unwrap();
        for owner_allowed in [false, true] {
            block_on(db.activate_schema(boolean_schema(owner_allowed))).unwrap();
            block_on(db.publish_bundle(boolean_schema(owner_allowed).public_schema())).unwrap();
            for handle in [&executing, &registered] {
                let rows = block_on(handle.all_for_identity(&query, ReadOpts::default(), alice()))
                    .unwrap();
                assert_eq!(rows.len(), usize::from(!preparing_allowed));
            }
            assert_eq!(
                block_on(preparing.all_for_identity(&query, ReadOpts::default(), alice()))
                    .unwrap()
                    .len(),
                usize::from(preparing_allowed)
            );
        }
    }
}

/// Alice prepares against one support key and executes under an authenticated
/// bundle requiring another. Bob revokes the latter; the live result must evict.
/// This lower-crate test accesses the admitted session Db to exercise cross-handle
/// preparation without adding main's absent integration-only pin-view API.
#[test]
fn cross_handle_subscription_watches_the_executing_support_key() {
    let schema = |policy| {
        JazzSchema::new(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("support")
                        .column("principal", ColumnType::Text)
                        .policies(policies(PolicyExpr::True)),
                )
                .table(
                    TableSchemaBuilder::new("notes")
                        .column("body", ColumnType::Text)
                        .policies(policies(policy)),
                )
                .build(),
        )
        .unwrap()
    };
    let policy = |field: &str| PolicyExpr::Exists {
        table: "support".into(),
        condition: Box::new(PolicyExpr::eq_session(
            "principal",
            vec!["claims".into(), field.into()],
        )),
    };
    let db = open(schema(policy("first")));
    for (id, principal) in [(1, "first"), (2, "second")] {
        seed(
            &db,
            "support",
            id,
            BTreeMap::from([("principal".into(), Value::String(principal.into()))]),
        );
    }
    seed(
        &db,
        "notes",
        3,
        BTreeMap::from([("body".into(), Value::String("secret".into()))]),
    );
    db.set_identity_claims(
        alice(),
        BTreeMap::from([
            ("\0claims:first".into(), Value::String("first".into())),
            ("\0claims:second".into(), Value::String("second".into())),
        ]),
    );
    let prepared = db.prepare_query(&Query::from("notes")).unwrap();
    let (selected, _peer) = admitted_view(&db, &schema(policy("second")));
    let mut stream =
        block_on(selected.subscribe_for_identity(&prepared, ReadOpts::default(), alice())).unwrap();
    let count = |stream: &mut SubscriptionStream, mut n: usize| {
        while let Some(event) = stream.try_next_event() {
            match event {
                SubscriptionEvent::Delta {
                    reset,
                    added,
                    removed,
                    ..
                } => {
                    if reset {
                        n = 0;
                    }
                    n = n.checked_sub(removed.len()).unwrap() + added.len();
                }
                SubscriptionEvent::Rejected { reason } => panic!("{reason}"),
                _ => {}
            }
        }
        n
    };
    assert_eq!(count(&mut stream, 0), 1);
    block_on(db.delete("support", RowUuid::from_bytes([2; 16]), Default::default())).unwrap();
    assert!(
        block_on(selected.all_for_identity(&prepared, ReadOpts::default(), alice()))
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        count(&mut stream, 1),
        0,
        "selected support key must invalidate the result"
    );
}

/// Alice's authenticated bundle remains selected when Bob renames the owner's
/// table and column; her registered handle keeps the admitted old names.
/// This lower-crate test needs the private handle from a real subscriber admission
/// so it can register a retained view without exposing a public pin selector.
#[test]
fn authenticated_pin_survives_retained_table_and_column_rename() {
    use crate::protocol::{LensOp, MigrationLens, TableLens};
    for allowed in [false, true] {
        let db = open(boolean_schema(true));
        seed(
            &db,
            "notes",
            1,
            BTreeMap::from([("body".into(), Value::String("secret".into()))]),
        );
        let (selected, _peer) = admitted_view(&db, &boolean_schema(allowed));
        let retained = block_on(selected.register_schema_view(boolean_schema(true))).unwrap();
        let query = retained.prepare_query(&Query::from("notes")).unwrap();
        let renamed = JazzSchema::new(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("tasks")
                        .column("title", ColumnType::Text)
                        .policies(policies(if allowed {
                            PolicyExpr::False
                        } else {
                            PolicyExpr::True
                        })),
                )
                .build(),
        )
        .unwrap();
        let lens = MigrationLens::new(
            boolean_schema(true).version_id(),
            renamed.version_id(),
            vec![TableLens {
                source_table: "notes".into(),
                target_table: "tasks".into(),
                ops: vec![LensOp::RenameColumn {
                    from: "body".into(),
                    to: "title".into(),
                }],
            }],
        )
        .unwrap();
        block_on(db.activate_schema_with_lenses(renamed, &[lens])).unwrap();
        let rows =
            block_on(retained.all_for_identity(&query, ReadOpts::default(), alice())).unwrap();
        assert_eq!(rows.len(), usize::from(allowed));
        if allowed {
            assert_eq!(rows[0].cell("body"), Some(Value::String("secret".into())));
        }
    }
}

/// Alice's equal-SQL owner and two real admitted bundles need separate groups.
/// Bob changes only owner policy; both selected bundles keep their grants.
/// Private placement obtains the session handles through actual admission.
#[test]
fn equal_sql_groups_keep_current_owner_and_distinct_admitted_bundles() {
    let sharing_schema = |notes_allowed: bool, audit_allowed: bool| {
        JazzSchema::new(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("notes")
                        .column("body", ColumnType::Text)
                        .policies(policies(if notes_allowed {
                            PolicyExpr::True
                        } else {
                            PolicyExpr::False
                        })),
                )
                .table(
                    TableSchemaBuilder::new("audit")
                        .column("body", ColumnType::Text)
                        .policies(policies(if audit_allowed {
                            PolicyExpr::True
                        } else {
                            PolicyExpr::False
                        })),
                )
                .build(),
        )
        .unwrap()
    };
    for owner_first in [false, true] {
        let owner = open(sharing_schema(true, true));
        seed(
            &owner,
            "notes",
            1,
            BTreeMap::from([("body".into(), Value::String("secret".into()))]),
        );
        let (first, _first_transport) = admitted_view(&owner, &sharing_schema(true, true));
        let (second, _second_transport) = admitted_view(&owner, &sharing_schema(true, false));
        assert_eq!(
            owner.catalogue().unwrap().bundles.len(),
            2,
            "two distinct authenticated bundles"
        );
        let query = owner.prepare_query(&Query::from("notes")).unwrap();
        let handles = [&owner, &first, &second];
        let order = if owner_first { [0, 1, 2] } else { [2, 1, 0] };
        let mut streams: [Option<SubscriptionStream>; 3] = [None, None, None];
        for index in order {
            streams[index] = Some(
                block_on(handles[index].subscribe_for_identity(
                    &query,
                    ReadOpts::default(),
                    alice(),
                ))
                .unwrap(),
            );
        }
        let count = |stream: &mut SubscriptionStream, mut n: usize| {
            while let Some(event) = stream.try_next_event() {
                match event {
                    SubscriptionEvent::Delta {
                        reset,
                        added,
                        removed,
                        ..
                    } => {
                        if reset {
                            n = 0;
                        }
                        n = n.checked_sub(removed.len()).unwrap() + added.len();
                    }
                    SubscriptionEvent::Rejected { reason } => panic!("{reason}"),
                    _ => {}
                }
            }
            n
        };
        for stream in &mut streams {
            assert_eq!(count(stream.as_mut().unwrap(), 0), 1);
        }
        assert_eq!(
            owner.fanout_stats().groups,
            3,
            "equal SQL does not erase exact bundle provenance"
        );
        block_on(owner.activate_schema(sharing_schema(false, true))).unwrap();
        for (index, (handle, stream)) in handles.into_iter().zip(&mut streams).enumerate() {
            let expected = usize::from(index != 0);
            assert_eq!(
                count(stream.as_mut().unwrap(), 1),
                expected,
                "only the current-owner group loses access"
            );
            let fresh = handle
                .prepare_query(&Query::from("notes").limit(2))
                .unwrap();
            assert_eq!(
                block_on(handle.all_for_identity(&fresh, ReadOpts::default(), alice()))
                    .unwrap()
                    .len(),
                expected
            );
        }
        drop(streams[0].take());
        block_on(owner.tick()).unwrap();
        assert_eq!(
            owner.fanout_stats().groups,
            2,
            "dropping owner preserves both selected groups"
        );
        let before = owner.fanout_stats();
        let mut joined =
            block_on(first.subscribe_for_identity(&query, ReadOpts::default(), alice())).unwrap();
        assert_eq!(count(&mut joined, 0), 1);
        assert_eq!(owner.fanout_stats().groups, 2);
        assert_eq!(
            owner.fanout_stats().shared_joins,
            before.shared_joins + 1,
            "same admitted bundle still shares"
        );
        assert_eq!(
            count(streams[2].as_mut().unwrap(), 1),
            1,
            "other bundle survives unchanged"
        );
    }
}

/// Alice's observation request must use her actual admitted Selected handle
/// and fixed claims while Bob changes the owner/registry. This lower-crate
/// test uses real private admission because no public pin selector exists.
#[test]
fn local_observation_context_keeps_admitted_policy_claims_and_generation() {
    for allowed in [false, true] {
        let owner = open(boolean_schema(!allowed));
        block_on(owner.insert(
            "notes",
            BTreeMap::from([("body".into(), Value::String("held".into()))]),
            Default::default(),
        ))
        .unwrap();
        let schema = boolean_schema(allowed);
        let bundle = block_on(owner.publish_bundle(schema.public_schema())).unwrap();
        let (_peer, transport) = super::duplex();
        let fixed = BTreeMap::from([("scope".into(), Value::String("admitted".into()))]);
        let slot = owner
            .accept_pinned_subscriber(transport, alice(), Some(fixed.clone()), Some(bundle))
            .unwrap();
        owner.set_identity_claims(
            alice(),
            BTreeMap::from([("scope".into(), Value::String("registry".into()))]),
        );
        let (handle, subject, claims, policy) = owner.admitted_observation_context(slot).unwrap();
        assert_eq!(
            policy,
            Some(alice()),
            "foreign authority session keeps policy enforcement"
        );
        assert_eq!(subject, alice());
        assert_eq!(claims, fixed.clone());
        let query = handle
            .prepare_query(&Query::from("notes"))
            .unwrap()
            .with_identity_claims(subject, claims);
        assert_eq!(
            block_on(handle.all_for_identity(&query, ReadOpts::default(), subject))
                .unwrap()
                .len(),
            usize::from(allowed)
        );
        let retained = handle
            .retain_local_authority_read(&query, false, Some(subject))
            .unwrap();
        assert_eq!(retained.rows.len(), usize::from(allowed));
        let cut = handle
            .local_observation_cut_for_admitted_session(&query, slot)
            .unwrap();
        let observed = handle.observe_local(&cut, &query, false).unwrap();
        assert_eq!(observed.rows.len(), usize::from(allowed));
        assert_eq!(observed.evidence.len(), observed.rows.len());
        owner.disconnect_subscriber(slot);
        assert!(owner.admitted_observation_context(slot).is_err());
        assert!(handle.observe_local(&cut, &query, false).is_err());
    }
}

/// A non-SYSTEM store refuses SYSTEM admission. A legally SYSTEM-owned
/// store serves that identity with owner-registry claims, not supplied fixed
/// claims. The unreachable foreign-SYSTEM path gets no runtime claim here.
#[test]
fn local_observation_system_session_uses_actual_owner_operands() {
    let non_system = block_on(Db::open(DbConfig::new(
        boolean_schema(true),
        DbIdentity {
            node: NodeUuid::from_bytes([93; 16]),
            author: alice(),
        },
    )))
    .unwrap();
    non_system.set_authority();
    let claims = BTreeMap::from([("scope".into(), Value::String("owner".into()))]);
    let wrong = BTreeMap::from([("scope".into(), Value::String("fixed".into()))]);
    let (_peer, transport) = super::duplex();
    let error = non_system
        .accept_pinned_subscriber(transport, AuthorSubject::SYSTEM, Some(wrong.clone()), None)
        .unwrap_err();
    assert_eq!(error.reason, Some(crate::db::Reason::IdentityMismatch));

    let owner = open(boolean_schema(true));
    owner.set_identity_claims(AuthorSubject::SYSTEM, claims.clone());
    owner.set_identity_claims(alice(), wrong.clone());
    let (_peer, transport) = super::duplex();
    let slot = owner
        .accept_pinned_subscriber(transport, AuthorSubject::SYSTEM, Some(wrong), None)
        .unwrap();
    let (_handle, author, actual, policy) = owner.admitted_observation_context(slot).unwrap();
    assert_eq!(author, AuthorSubject::SYSTEM);
    assert_eq!(actual, claims);
    assert_eq!(policy, None);
    owner.disconnect_subscriber(slot);
    assert!(owner.admitted_observation_context(slot).is_err());
}
