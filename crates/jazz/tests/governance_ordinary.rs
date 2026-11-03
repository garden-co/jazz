use crate::accepted_bundle_reads::{open, schema, seed};
use crate::accepted_rpc_reads::alice;
use crate::common::compile_schema;
use jazz::db::block_on;
use jazz::ids::AuthorSubject;
use jazz::query::Query;

/// Alice's ordinary prepared reads follow the owner on unpinned retained
/// handles and preserve an admitted pin across a retained-name transformation.
#[test]
fn ordinary_retained_reads_follow_explicit_policy_provenance() {
    for owner in [false, true] {
        for pin in [None, Some(false), Some(true)] {
            let server = open(9, AuthorSubject::SYSTEM, owner, true);
            seed(&server);
            let base = match pin {
                Some(allowed) => {
                    let bundle = block_on(server.publish_bundle(&schema(allowed))).unwrap();
                    server.check_session_pin(alice(), Some(&bundle)).unwrap();
                    Some(
                        server
                            .pinned_view(compile_schema(&schema(owner)).version_id(), Some(&bundle))
                            .unwrap()
                            .unwrap(),
                    )
                }
                None => None,
            };
            let parent = base.as_ref().unwrap_or(&server);
            let view =
                block_on(parent.register_schema_view(compile_schema(&schema(owner)))).unwrap();
            let prepared = view.prepare_query(&Query::from("notes")).unwrap();
            block_on(server.activate_schema(compile_schema(&schema(!owner)))).unwrap();
            let rows =
                block_on(view.all_for_identity(&prepared, Default::default(), alice())).unwrap();
            assert_eq!(
                rows.len(),
                usize::from(pin.unwrap_or(!owner)),
                "owner={owner}, pin={pin:?}"
            );
        }
    }
}

/// Alice's explicitly selected policies survive registration of admitted names,
/// regardless of the owner's opposite policy or a later owner revision.
#[test]
fn selected_registration_preserves_parent_policy_context() {
    for allowed in [false, true] {
        let server = open(9, AuthorSubject::SYSTEM, !allowed, true);
        seed(&server);
        let bundle = block_on(server.publish_bundle(&schema(allowed))).unwrap();
        server.check_session_pin(alice(), Some(&bundle)).unwrap();
        let parent = server
            .pinned_view(
                compile_schema(&schema(!allowed)).version_id(),
                Some(&bundle),
            )
            .unwrap()
            .unwrap();
        let view =
            block_on(parent.register_schema_view(compile_schema(&schema(!allowed)))).unwrap();
        let prepared = view.prepare_query(&Query::from("notes")).unwrap();
        for revised in [false, true] {
            if revised {
                block_on(server.activate_schema(compile_schema(&schema(allowed)))).unwrap();
            }
            let read = view
                .accepted_read(&prepared, None, false, Some(alice()))
                .unwrap();
            assert_eq!(
                read.rows.len(),
                usize::from(allowed),
                "pin={allowed}, revised={revised}"
            );
        }
    }
}

/// Mallory cannot use a query prepared on an open pin to grant Alice access
/// when the executing trusted handle selects the closed pin, or vice versa.
#[test]
fn prepared_query_cannot_import_another_handles_policy_context() {
    let server = open(9, AuthorSubject::SYSTEM, true, true);
    seed(&server);
    for preparing_allowed in [true, false] {
        let preparing_bundle = block_on(server.publish_bundle(&schema(preparing_allowed))).unwrap();
        let executing_bundle =
            block_on(server.publish_bundle(&schema(!preparing_allowed))).unwrap();
        let version = compile_schema(&schema(true)).version_id();
        server
            .check_session_pin(alice(), Some(&preparing_bundle))
            .unwrap();
        server
            .check_session_pin(alice(), Some(&executing_bundle))
            .unwrap();
        let preparing = server
            .pinned_view(version, Some(&preparing_bundle))
            .unwrap()
            .unwrap();
        let executing = server
            .pinned_view(version, Some(&executing_bundle))
            .unwrap()
            .unwrap();
        let query = preparing.prepare_query(&Query::from("notes")).unwrap();
        let rows =
            block_on(executing.all_for_identity(&query, Default::default(), alice())).unwrap();
        assert_eq!(
            rows.len(),
            usize::from(!preparing_allowed),
            "preparing={preparing_allowed}"
        );
    }
}

fn observed_rows(stream: &mut jazz::db::SubscriptionStream, previous: usize) -> usize {
    let mut count = previous;
    while let Some(event) = stream.try_next_event() {
        match event {
            jazz::db::SubscriptionEvent::Delta {
                reset,
                added,
                removed,
                ..
            } => {
                if reset {
                    count = 0;
                }
                count = count
                    .checked_sub(removed.len())
                    .expect("removed visible rows")
                    + added.len();
            }
            jazz::db::SubscriptionEvent::Rejected { reason } => {
                panic!("subscription rejected: {reason}")
            }
            _ => {}
        }
    }
    count
}

/// Alice's current-owner and two pinned subscriptions keep their own policy
/// context through owner activation and a retained table/column rename.
/// Dropping and reopening a member must not disturb another group's key.
#[test]
fn ordinary_subscriptions_preserve_pins_and_refresh_current_owner() {
    use crate::common::read_and_allow_all_writes;
    use jazz::db::ReadOpts;
    use jazz::protocol::{LensOp, MigrationLens, TableLens};
    use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TableSchemaBuilder};
    for owner in [false, true] {
        let server = open(9, AuthorSubject::SYSTEM, owner, true);
        seed(&server);
        let mut handles = Vec::new();
        for pin in [None, Some(false), Some(true)] {
            let parent = match pin {
                None => server
                    .governed_view(compile_schema(&schema(owner)).version_id())
                    .unwrap(),
                Some(allowed) => {
                    let bundle = block_on(server.publish_bundle(&schema(allowed))).unwrap();
                    server.check_session_pin(alice(), Some(&bundle)).unwrap();
                    server
                        .pinned_view(compile_schema(&schema(owner)).version_id(), Some(&bundle))
                        .unwrap()
                        .unwrap()
                }
            };
            let view =
                block_on(parent.register_schema_view(compile_schema(&schema(owner)))).unwrap();
            handles.push((view, pin));
        }
        // All execute a query prepared on the open pin; it supplies names only.
        let query = handles[2].0.prepare_query(&Query::from("notes")).unwrap();
        let mut streams = handles
            .iter()
            .map(|(db, _)| {
                block_on(db.subscribe_for_identity(&query, ReadOpts::default(), alice())).unwrap()
            })
            .collect::<Vec<_>>();
        let mut counts = [0; 3];
        for (i, (_, pin)) in handles.iter().enumerate() {
            counts[i] = observed_rows(&mut streams[i], counts[i]);
            assert_eq!(counts[i], usize::from(pin.unwrap_or(owner)));
        }
        for renamed in [false, true] {
            let allowed = if renamed { owner } else { !owner };
            if renamed {
                let new_schema = compile_schema(
                    &SchemaBuilder::new()
                        .table(
                            TableSchemaBuilder::new("tasks")
                                .column("title", ColumnType::Text)
                                .policies(read_and_allow_all_writes(if allowed {
                                    PolicyExpr::True
                                } else {
                                    PolicyExpr::False
                                })),
                        )
                        .build(),
                );
                let lens = MigrationLens::new(
                    compile_schema(&schema(owner)).version_id(),
                    new_schema.version_id(),
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
                block_on(server.activate_schema_with_lenses(new_schema, &[lens])).unwrap();
            } else {
                block_on(server.activate_schema(compile_schema(&schema(allowed)))).unwrap();
            }
            for (i, (db, pin)) in handles.iter().enumerate() {
                counts[i] = observed_rows(&mut streams[i], counts[i]);
                assert_eq!(
                    counts[i],
                    usize::from(pin.unwrap_or(allowed)),
                    "owner={owner}, pin={pin:?}, renamed={renamed}"
                );
                let rows =
                    block_on(db.all_for_identity(&query, ReadOpts::default(), alice())).unwrap();
                assert_eq!(rows.len(), counts[i]);
                // A fresh member may share only the matching execution context.
                let mut fresh =
                    block_on(db.subscribe_for_identity(&query, ReadOpts::default(), alice()))
                        .unwrap();
                assert_eq!(observed_rows(&mut fresh, 0), counts[i]);
                drop(fresh);
                block_on(server.tick()).unwrap();
            }
        }
    }
}

fn support_schema(policy: jazz::tools::PolicyExpr) -> jazz::tools::public_schema::Schema {
    use crate::common::{allow_all_policies, read_and_allow_all_writes};
    use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
    SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("support")
                .column("principal", ColumnType::Text)
                .policies(allow_all_policies()),
        )
        .table(
            TableSchemaBuilder::new("notes")
                .column("body", ColumnType::Text)
                .fk_column("support", "support")
                .policies(read_and_allow_all_writes(policy)),
        )
        .build()
}

fn support_server(policy: jazz::tools::PolicyExpr) -> jazz::db::Db {
    use jazz::db::{Db, DbConfig, DbIdentity};
    use jazz::ids::{NodeUuid, RowUuid};
    use jazz::records::Value;
    use std::collections::BTreeMap;
    let db = block_on(Db::open(DbConfig::new(
        compile_schema(&support_schema(policy)),
        DbIdentity {
            node: NodeUuid::from_bytes([9; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    db.set_authority();
    for (id, name) in [(5, "first"), (6, "second")] {
        db.seed_settled_mergeable_for_bootstrap(
            "support",
            RowUuid::from_bytes([id; 16]),
            AuthorSubject::SYSTEM,
            BTreeMap::from([("principal".into(), Value::String(name.into()))]),
        )
        .unwrap();
    }
    db.seed_settled_mergeable_for_bootstrap(
        "notes",
        RowUuid::from_bytes([4; 16]),
        AuthorSubject::SYSTEM,
        BTreeMap::from([
            ("body".into(), Value::String("secret".into())),
            (
                "support".into(),
                Value::Uuid(uuid::Uuid::from_bytes([6; 16])),
            ),
        ]),
    )
    .unwrap();
    db.set_identity_claims(
        alice(),
        BTreeMap::from([
            ("\0claims:first".into(), Value::String("first".into())),
            ("\0claims:second".into(), Value::String("second".into())),
        ]),
    );
    db
}

/// Alice prepares under the first support key, but executes under a pin
/// requiring the second key. Revoking the second key must evict her row.
#[test]
fn cross_handle_subscription_watches_the_executing_support_key() {
    use crate::common::{exists, session_eq};
    use jazz::db::ReadOpts;
    use jazz::ids::RowUuid;
    let first = exists(
        "support",
        vec![session_eq("principal", &["claims", "first"])],
    );
    let second = exists(
        "support",
        vec![session_eq("principal", &["claims", "second"])],
    );
    let server = support_server(first.clone());
    let prepared = server.prepare_query(&Query::from("notes")).unwrap();
    let bundle = block_on(server.publish_bundle(&support_schema(second))).unwrap();
    server.check_session_pin(alice(), Some(&bundle)).unwrap();
    let selected = server
        .pinned_view(
            compile_schema(&support_schema(first)).version_id(),
            Some(&bundle),
        )
        .unwrap()
        .unwrap();
    let mut stream =
        block_on(selected.subscribe_for_identity(&prepared, ReadOpts::default(), alice())).unwrap();
    assert_eq!(observed_rows(&mut stream, 0), 1);
    block_on(server.delete("support", RowUuid::from_bytes([6; 16]), Default::default())).unwrap();
    assert_eq!(
        block_on(selected.all_for_identity(&prepared, ReadOpts::default(), alice()))
            .unwrap()
            .len(),
        0
    );
    assert_eq!(
        observed_rows(&mut stream, 1),
        0,
        "executing support revocation must be watched"
    );
}

/// Alice's deferred LocalFirst read is re-admitted through the relay's actual
/// session pin when its upstream becomes unavailable; owner fallback is forbidden.
#[test]
fn deferred_first_load_retains_the_authenticated_session_pin() {
    use jazz::db::{FirstLoad, ReadOpts, RemoteLinkHint, duplex};
    for allowed in [false, true] {
        let authority = open(9, AuthorSubject::SYSTEM, !allowed, true);
        seed(&authority);
        let relay = open(8, AuthorSubject::SYSTEM, !allowed, false);
        let (up, down) = duplex();
        let upstream = block_on(relay.connect_upstream(up));
        authority
            .accept_subscriber(down, AuthorSubject::SYSTEM)
            .unwrap();
        relay.set_remote_link_hint(RemoteLinkHint::Live);
        let warm = relay
            .prepare_query(&Query::from("notes").filter(jazz::query::eq(
                jazz::query::col("body"),
                jazz::query::lit(jazz::records::Value::String("secret".into())),
            )))
            .unwrap();
        let mut warm_stream = block_on(relay.subscribe(&warm, ReadOpts::default())).unwrap();
        for _ in 0..8 {
            block_on(relay.tick()).unwrap();
            block_on(authority.tick()).unwrap();
        }
        block_on(relay.tick()).unwrap();
        assert_eq!(
            observed_rows(&mut warm_stream, 0),
            1,
            "relay has an admitted, warm upstream cache"
        );
        let bundle = block_on(relay.publish_bundle(&schema(allowed))).unwrap();
        // Do not tick the authority while the new, different query waits.

        let client = open(1, alice(), true, false);
        client.set_non_durable_client();
        let (up, down) = duplex();
        let _client_link = block_on(client.connect_upstream(up));
        relay
            .accept_pinned_subscriber(down, alice(), None, Some(bundle))
            .unwrap();
        client.set_remote_link_hint(RemoteLinkHint::Live);
        let query = client.prepare_query(&Query::from("notes")).unwrap();
        let mut stream = block_on(client.subscribe(
            &query,
            ReadOpts {
                first_load: FirstLoad::WaitForRemote { timeout_ms: 60_000 },
                ..ReadOpts::default()
            },
        ))
        .unwrap();
        for _ in 0..5 {
            block_on(client.tick()).unwrap();
            block_on(relay.tick()).unwrap();
        }
        assert!(
            stream.try_next_event().is_none(),
            "opening must remain deferred while upstream may answer"
        );
        assert!(relay.detach_connection(&upstream));
        relay.set_remote_link_hint(RemoteLinkHint::Failed);
        for _ in 0..8 {
            block_on(relay.tick()).unwrap();
            block_on(client.tick()).unwrap();
        }
        let local = relay
            .pinned_view(
                compile_schema(&schema(!allowed)).version_id(),
                Some(&bundle),
            )
            .unwrap()
            .unwrap();
        let prepared = local.prepare_query(&Query::from("notes")).unwrap();
        let locally_visible =
            block_on(local.all_for_identity(&prepared, ReadOpts::default(), alice()))
                .unwrap()
                .len();
        assert_eq!(
            locally_visible,
            usize::from(allowed),
            "relay cache pin={allowed}"
        );
        let cached = block_on(client.all(&query, ReadOpts::default()))
            .unwrap()
            .len();
        assert_eq!(
            cached,
            usize::from(allowed),
            "relay delivered governed cache, pin={allowed}"
        );
        let observed = observed_rows(&mut stream, 0);
        assert_eq!(
            observed,
            usize::from(allowed),
            "deferred pin={allowed}, local={locally_visible}"
        );
        drop(stream);
        block_on(client.tick()).unwrap();
        block_on(relay.tick()).unwrap();
    }
}
