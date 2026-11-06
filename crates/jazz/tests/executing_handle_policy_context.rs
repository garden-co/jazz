//! Ordinary reads execute under the handle's current authenticated governance.
use crate::common::{compile_schema, read_and_allow_all_writes};
use jazz::db::{Db, DbConfig, DbIdentity, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::public_schema::Schema as PublicSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TableSchemaBuilder};
use std::collections::BTreeMap;

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([17; 16])
}

fn schema(read: bool) -> PublicSchema {
    SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("notes")
                .column("body", ColumnType::Text)
                .policies(read_and_allow_all_writes(if read {
                    PolicyExpr::True
                } else {
                    PolicyExpr::False
                })),
        )
        .build()
}

fn open(node: u8, author: AuthorSubject, read: bool, authority: bool) -> Db {
    let db = block_on(Db::open(DbConfig::new(
        compile_schema(&schema(read)),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )))
    .unwrap();
    if authority {
        db.set_authority();
    } else {
        db.become_replica();
    }
    db
}

fn seed(server: &Db) {
    server
        .seed_settled_mergeable_for_bootstrap(
            "notes",
            RowUuid::from_bytes([4; 16]),
            AuthorSubject::SYSTEM,
            BTreeMap::from([("body".into(), Value::String("secret".into()))]),
        )
        .unwrap();
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

/// Alice's root and registered-name handles both observe owner policy changes;
/// a retained schema supplies names, never an old authorization decision.
#[test]
fn ordinary_retained_reads_follow_current_owner() {
    for allowed in [false, true] {
        let db = open(9, AuthorSubject::SYSTEM, allowed, true);
        seed(&db);
        let retained = block_on(db.register_schema_view(compile_schema(&schema(allowed)))).unwrap();
        let prepared = retained.prepare_query(&Query::from("notes")).unwrap();
        block_on(db.activate_schema(compile_schema(&schema(!allowed)))).unwrap();
        for executing in [&db, &retained] {
            let rows = block_on(executing.all_for_identity(&prepared, Default::default(), alice()))
                .unwrap();
            assert_eq!(rows.len(), usize::from(!allowed), "old policy={allowed}");
        }
    }
}

/// Alice's insert through retained names is checked by the current owner's
/// policy in both directions; an old registered view cannot keep a grant.
#[test]
fn retained_writes_use_current_owner_permissions() {
    use jazz::db::{InsertOptions, WriteIdentity};
    let write_schema = |allowed| {
        SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .policies(
                        jazz::tools::TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(if allowed {
                                PolicyExpr::True
                            } else {
                                PolicyExpr::False
                            }),
                    ),
            )
            .build()
    };
    for allowed in [false, true] {
        let initial = write_schema(allowed);
        let db = block_on(Db::open(DbConfig::new(
            compile_schema(&initial),
            DbIdentity {
                node: NodeUuid::from_bytes([9; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .unwrap();
        let retained = block_on(db.register_schema_view(compile_schema(&initial))).unwrap();
        block_on(db.activate_schema(compile_schema(&write_schema(!allowed)))).unwrap();
        let written = block_on(retained.insert(
            "notes",
            BTreeMap::from([("body".into(), Value::String("secret".into()))]),
            InsertOptions {
                identity: WriteIdentity::Session(alice()),
                ..Default::default()
            },
        ));
        assert_eq!(
            written.is_ok(),
            !allowed,
            "old insert policy={allowed}: {written:?}"
        );
        let query = db.prepare_query(&Query::from("notes")).unwrap();
        assert_eq!(
            block_on(db.all(&query, Default::default())).unwrap().len(),
            usize::from(!allowed)
        );
    }
}

/// Alice's physical text read retains old column names after Bob renames the
/// owner schema, while current owner policy still revokes the same handle.
#[test]
fn retained_large_cell_reads_keep_names_and_current_policy() {
    use jazz::protocol::{LensOp, MigrationLens, TableLens};
    let db = open(9, alice(), true, true);
    let text = "large-cell-é😀".repeat(10_000);
    db.seed_settled_mergeable_for_bootstrap(
        "notes",
        RowUuid::from_bytes([7; 16]),
        AuthorSubject::SYSTEM,
        BTreeMap::from([("body".into(), Value::String(text.clone()))]),
    )
    .unwrap();
    let retained = block_on(db.register_schema_view(compile_schema(&schema(true)))).unwrap();
    let renamed = |allowed| {
        SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("tasks")
                    .column("title", ColumnType::Text)
                    .policies(read_and_allow_all_writes(if allowed {
                        PolicyExpr::True
                    } else {
                        PolicyExpr::False
                    })),
            )
            .build()
    };
    let new = compile_schema(&renamed(true));
    let lens = MigrationLens::new(
        compile_schema(&schema(true)).version_id(),
        new.version_id(),
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
    block_on(db.activate_schema_with_lenses(new, &[lens])).unwrap();
    assert_eq!(
        block_on(retained.read_text_utf16_range(
            "notes",
            RowUuid::from_bytes([7; 16]),
            "body",
            0..10
        ))
        .unwrap(),
        "large-cell"
    );
    block_on(db.activate_schema(compile_schema(&renamed(false)))).unwrap();
    assert!(
        block_on(retained.read_text_utf16_range(
            "notes",
            RowUuid::from_bytes([7; 16]),
            "body",
            0..10
        ))
        .is_err(),
        "current policy must revoke retained physical reads"
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
        // Compare with a fresh authenticated subscriber of the same warm relay.
        // Main has no public pinned-view constructor; the normal serving route
        // is the independent oracle for the selected cache policy.
        let fresh = open(2, alice(), true, false);
        let (up, down) = duplex();
        let _fresh_link = block_on(fresh.connect_upstream(up));
        relay
            .accept_pinned_subscriber(down, alice(), None, Some(bundle))
            .unwrap();
        let fresh_query = fresh.prepare_query(&Query::from("notes").limit(2)).unwrap();
        let mut fresh_stream =
            block_on(fresh.subscribe(&fresh_query, ReadOpts::default())).unwrap();
        for _ in 0..8 {
            block_on(fresh.tick()).unwrap();
            block_on(relay.tick()).unwrap();
        }
        block_on(fresh.tick()).unwrap();
        let locally_visible = observed_rows(&mut fresh_stream, 0);
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

/// Alice reconnects the same client and retained subscription through freshly
/// authenticated admissions. A prior open pin cannot survive a closed admission.
#[test]
fn reconnect_readmission_uses_the_new_authenticated_pin() {
    use jazz::db::{ReadOpts, duplex};
    let server = open(9, AuthorSubject::SYSTEM, true, true);
    seed(&server);
    let closed = block_on(server.publish_bundle(&schema(false))).unwrap();
    let opened = block_on(server.publish_bundle(&schema(true))).unwrap();
    let client = open(1, alice(), true, false);
    let retained = block_on(client.register_schema_view(compile_schema(&schema(true)))).unwrap();
    let query = retained.prepare_query(&Query::from("notes")).unwrap();
    let mut stream = block_on(retained.subscribe(&query, ReadOpts::default())).unwrap();
    let mut count = observed_rows(&mut stream, 0);
    for allowed in [false, true, false, true] {
        let (up, down) = duplex();
        let link = block_on(client.connect_upstream(up));
        let subscriber = server
            .accept_pinned_subscriber(
                down,
                alice(),
                None,
                Some(if allowed { opened } else { closed }),
            )
            .unwrap();
        for _ in 0..8 {
            block_on(client.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
        block_on(client.tick()).unwrap();
        count = observed_rows(&mut stream, count);
        assert_eq!(count, usize::from(allowed), "reconnected pin={allowed}");
        assert_eq!(
            block_on(retained.all(&query, ReadOpts::default()))
                .unwrap()
                .len(),
            count
        );
        assert!(client.detach_connection(&link));
        server.disconnect_subscriber(subscriber);
        block_on(server.tick()).unwrap();
        assert_eq!(server.open_subscribers(), 0);
    }
    drop(stream);
    block_on(client.tick()).unwrap();
}

/// Alice's retained client reads and stages a write in an ordinary serializable
/// transaction. Bob's authority validates the read with Alice's actual bundle,
/// even when the current owner allows the opposite set of rows.
/// Alice -> pinned session -> read + staged insert -> authority Global receipt.
#[test]
fn serializable_retained_reads_and_staged_writes_use_the_admitted_bundle() {
    use jazz::db::{ReadOpts, ReadTier, duplex};
    use jazz::tools::OpenTransactionId;
    use jazz::tx::DurabilityTier;
    use std::future::Future;
    fn drive<T>(server: &Db, client: &Db, future: impl Future<Output = T>) -> T {
        let mut future = std::pin::pin!(future);
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        for _ in 0..64 {
            if let std::task::Poll::Ready(value) = future.as_mut().poll(&mut cx) {
                return value;
            }
            block_on(client.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
        panic!("the serializable operation did not complete");
    }
    for allowed in [false, true] {
        let server = open(9, AuthorSubject::SYSTEM, !allowed, true);
        seed(&server);
        let bundle = block_on(server.publish_bundle(&schema(allowed))).unwrap();
        let client = open(1, alice(), true, false);
        let retained =
            block_on(client.register_schema_view(compile_schema(&schema(true)))).unwrap();
        let (up, down) = duplex();
        let _link = block_on(client.connect_upstream(up));
        let _subscriber = server
            .accept_pinned_subscriber(down, alice(), None, Some(bundle))
            .unwrap();
        let query = retained.prepare_query(&Query::from("notes")).unwrap();
        let _stream = block_on(retained.subscribe(&query, ReadOpts::default())).unwrap();
        for _ in 0..8 {
            block_on(client.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
        let transaction = OpenTransactionId::new();
        block_on(retained.begin_serializable(transaction)).unwrap();
        let rows = drive(
            &server,
            &client,
            retained
                .tx_ref(transaction)
                .all_prepared_at(&query, false, None, ReadTier::Remote),
        )
        .unwrap();
        assert_eq!(
            rows.len(),
            usize::from(allowed),
            "the read uses the admitted bundle"
        );
        drive(
            &server,
            &client,
            retained.tx_ref(transaction).insert(
                "notes",
                BTreeMap::from([("body".into(), Value::String("new note".into()))]),
                Default::default(),
            ),
        )
        .unwrap();
        let receipt = drive(
            &server,
            &client,
            retained.commit_serializable_handle(transaction),
        )
        .unwrap();
        drive(
            &server,
            &client,
            retained.wait_for_transaction(receipt.0, DurabilityTier::Global),
        )
        .expect("the authority validates unchanged predicates under the admitted bundle");
        let all = server.prepare_query(&Query::from("notes")).unwrap();
        assert_eq!(
            block_on(server.all(&all, ReadOpts::default()))
                .unwrap()
                .len(),
            2,
            "the staged write was accepted and published exactly once"
        );
        if allowed {
            let conflicting = OpenTransactionId::new();
            block_on(retained.begin_serializable(conflicting)).unwrap();
            assert_eq!(
                drive(
                    &server,
                    &client,
                    retained.tx_ref(conflicting).all_prepared_at(
                        &query,
                        false,
                        None,
                        ReadTier::Remote
                    )
                )
                .unwrap()
                .len(),
                2
            );
            drive(
                &server,
                &client,
                retained.tx_ref(conflicting).insert(
                    "notes",
                    BTreeMap::from([("body".into(), Value::String("must not publish".into()))]),
                    jazz::db::InsertOptions {
                        row_id: Some(RowUuid::from_bytes([8; 16])),
                        ..Default::default()
                    },
                ),
            )
            .unwrap();
            block_on(server.update(
                "notes",
                RowUuid::from_bytes([4; 16]),
                BTreeMap::from([("body".into(), Value::String("changed after read".into()))]),
                Default::default(),
            ))
            .unwrap();
            match drive(
                &server,
                &client,
                retained.commit_serializable_handle(conflicting),
            ) {
                Err(error) => assert_eq!(error.code, jazz::db::ErrorCode::TransactionConflict),
                Ok(receipt) => {
                    let error = drive(
                        &server,
                        &client,
                        retained.wait_for_transaction(receipt.0, DurabilityTier::Global),
                    )
                    .unwrap_err();
                    assert_eq!(error.code, jazz::db::ErrorCode::WriteRejected);
                    assert_eq!(error.reason, Some(jazz::db::Reason::SerializationConflict));
                }
            }
            let rows = block_on(server.all(&all, ReadOpts::default())).unwrap();
            assert_eq!(rows.len(), 2, "the refused staged write was not published");
            assert!(
                rows.iter()
                    .all(|row| row.row_uuid() != RowUuid::from_bytes([8; 16]))
            );
        }
    }
}
