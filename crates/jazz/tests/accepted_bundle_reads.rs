//! Accepted SELECTs obey the policy bundle selected by authenticated admission.
use crate::accepted_rpc_reads::{alice, drive};
use crate::common::{compile_schema, read_and_allow_all_writes};
use jazz::db::{AcceptedReadScope, Db, DbConfig, DbIdentity, ErrorCode, block_on, duplex};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::public_schema::Schema as PublicSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TableSchemaBuilder};
use std::collections::BTreeMap;

pub(super) fn schema(read: bool) -> PublicSchema {
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
pub(super) fn open(node: u8, author: AuthorSubject, read: bool, authority: bool) -> Db {
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
pub(super) fn seed(server: &Db) {
    server
        .seed_settled_mergeable_for_bootstrap(
            "notes",
            RowUuid::from_bytes([4; 16]),
            AuthorSubject::SYSTEM,
            BTreeMap::from([("body".into(), Value::String("secret".into()))]),
        )
        .unwrap();
}

/// Alice uses the same names and claims on two authenticated pins. Neither
/// the owner's policy nor a later bundle publication changes those pins.
#[test]
fn accepted_reads_use_the_authenticated_bundle_in_both_policy_directions() {
    for owner in [false, true] {
        let server = open(9, AuthorSubject::SYSTEM, owner, true);
        seed(&server);
        let mut clients = Vec::new();
        for (node, allowed) in [(1, false), (2, true)] {
            let bundle = block_on(server.publish_bundle(&schema(allowed))).unwrap();
            let client = open(node, alice(), true, false);
            let (up, down) = duplex();
            let link = block_on(client.connect_upstream(up));
            server
                .accept_pinned_subscriber(down, alice(), Some(BTreeMap::new()), Some(bundle))
                .unwrap();
            clients.push((client, link, allowed));
        }
        for later_publication in [false, true] {
            if later_publication {
                block_on(server.publish_bundle(&schema(!owner))).unwrap();
            }
            for (client, _, allowed) in &clients {
                let query = client.prepare_query(&Query::from("notes")).unwrap();
                // An ordinary caller cannot replace authenticated Alice with
                // SYSTEM, even though it can encode a delegated scope.
                let spoof = AcceptedReadScope {
                    author: AuthorSubject::SYSTEM,
                    claims: BTreeMap::new(),
                };
                let read = drive(
                    &[client, &server],
                    client.accepted_query(&query, None, false, Some(spoof)),
                )
                .unwrap();
                assert_eq!(read.scope.author, alice());
                assert_eq!(read.evidence.len(), usize::from(*allowed));
            }
        }
    }
}

/// A trusted backend delegates Alice's identity within its authenticated
/// bundle; delegation cannot replace a closed bundle with owner policies.
#[test]
fn delegated_accepted_reads_stay_inside_the_authenticated_pin() {
    for allowed in [false, true] {
        let server = open(9, AuthorSubject::SYSTEM, !allowed, true);
        seed(&server);
        let bundle = block_on(server.publish_bundle(&schema(allowed))).unwrap();
        let backend = open(1, AuthorSubject::SYSTEM, true, false);
        let (up, down) = duplex();
        let _link = block_on(backend.connect_upstream(up));
        server
            .accept_pinned_subscriber(down, AuthorSubject::SYSTEM, None, Some(bundle))
            .unwrap();
        let query = backend.prepare_query(&Query::from("notes")).unwrap();
        let scope = AcceptedReadScope {
            author: alice(),
            claims: BTreeMap::new(),
        };
        let read = drive(
            &[&backend, &server],
            backend.accepted_query(&query, None, false, Some(scope)),
        )
        .unwrap();
        assert_eq!(read.scope.author, alice());
        assert_eq!(read.evidence.len(), usize::from(allowed));
    }
}

/// Alice's relay cannot authenticate an equivalent pin at its upstream.
/// It refuses before forwarding, including after its upstream reconnects.
#[test]
fn pinned_relays_refuse_accepted_reads_without_losing_the_pin() {
    let server = open(9, AuthorSubject::SYSTEM, true, true);
    seed(&server);
    let relay = open(8, AuthorSubject::SYSTEM, true, false);
    let bundle = block_on(relay.publish_bundle(&schema(true))).unwrap();
    let client = open(1, alice(), true, false);
    let (up, down) = duplex();
    let _client_link = block_on(client.connect_upstream(up));
    relay
        .accept_pinned_subscriber(down, alice(), None, Some(bundle))
        .unwrap();
    let query = client.prepare_query(&Query::from("notes")).unwrap();
    for connected in [false, true, true] {
        let link = if connected {
            let (up, down) = duplex();
            let link = block_on(relay.connect_upstream(up));
            server
                .accept_subscriber(down, AuthorSubject::SYSTEM)
                .unwrap();
            Some(link)
        } else {
            None
        };
        let error = drive(
            &[&client, &relay, &server],
            client.accepted_query(&query, None, false, None),
        )
        .unwrap_err();
        assert_eq!(error.code, ErrorCode::Query);
        assert!(
            error
                .message
                .contains("accepted reads through a pinned relay session is not supported yet")
        );
        assert_eq!(relay.pending_once_state_for_test(), (0, 0, 0));
        if let Some(link) = link {
            assert!(relay.detach_connection(&link));
        }
    }
}

/// Alice keeps the old table/column names across Bob's schema rename; her
/// authenticated bundle still controls those names after the owner changes.
#[test]
fn accepted_bundle_reads_preserve_retained_names_through_a_rename() {
    use jazz::protocol::{LensOp, MigrationLens, TableLens};
    for allowed in [false, true] {
        let server = open(9, AuthorSubject::SYSTEM, true, true);
        seed(&server);
        let bundle = block_on(server.publish_bundle(&schema(allowed))).unwrap();
        let client = open(1, alice(), true, false);
        let retained =
            block_on(client.register_schema_view(compile_schema(&schema(true)))).unwrap();
        let query = retained.prepare_query(&Query::from("notes")).unwrap();
        let renamed = compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("tasks")
                        .column("title", ColumnType::Text)
                        .policies(read_and_allow_all_writes(PolicyExpr::True)),
                )
                .build(),
        );
        let lens = MigrationLens::new(
            compile_schema(&schema(true)).version_id(),
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
        block_on(server.activate_schema_with_lenses(renamed, &[lens])).unwrap();
        let (up, down) = duplex();
        let _link = block_on(client.connect_upstream(up));
        server
            .accept_pinned_subscriber(down, alice(), None, Some(bundle))
            .unwrap();
        let read = drive(
            &[&client, &server],
            retained.accepted_query(&query, None, false, None),
        )
        .unwrap();
        assert_eq!(
            read.schema,
            *compile_schema(&schema(true)).version_id().as_bytes()
        );
        assert_eq!(read.evidence.len(), usize::from(allowed));
        let rows = jazz::binding_codec::decode_rows(&read.rows).unwrap();
        for row in rows.iter().flat_map(|batch| &batch.rows) {
            assert_eq!(row.cells.get("body"), Some(&Value::String("secret".into())));
            assert!(!row.cells.contains_key("title"));
        }
    }
}

/// Alice's accepted transaction reads and commits under one authenticated
/// pin, even when the owner's policy would return the opposite result.
#[test]
fn accepted_transactions_validate_under_each_authenticated_pin() {
    use jazz::tools::OpenTransactionId;
    use jazz::tx::DurabilityTier;
    for owner in [false, true] {
        for (node, allowed) in [(1, false), (2, true)] {
            let server = open(9, AuthorSubject::SYSTEM, owner, true);
            seed(&server);
            let bundle = block_on(server.publish_bundle(&schema(allowed))).unwrap();
            let client = open(node, alice(), true, false);
            let (up, down) = duplex();
            let _link = block_on(client.connect_upstream(up));
            server
                .accept_pinned_subscriber(down, alice(), Some(BTreeMap::new()), Some(bundle))
                .unwrap();
            let id = OpenTransactionId::new();
            block_on(client.begin_serializable_for_identity(id, alice())).unwrap();
            let query = client.prepare_query(&Query::from("notes")).unwrap();
            let read = drive(
                &[&client, &server],
                client
                    .accepted_transaction_query(id, query, false, 1000)
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(read.evidence.len(), usize::from(allowed));
            // Publication cannot silently replace either the read pin or
            // the policy used to validate its (possibly empty) predicate.
            block_on(server.publish_bundle(&schema(!allowed))).unwrap();
            drive(
                &[&client, &server],
                client.tx_ref(id).insert(
                    "notes",
                    BTreeMap::from([("body".into(), Value::String("new note".into()))]),
                    Default::default(),
                ),
            )
            .unwrap();
            let receipt = block_on(client.commit_serializable_handle(id)).unwrap();
            drive(
                &[&client, &server],
                client.wait_for_transaction(receipt.0, DurabilityTier::Global),
            )
            .unwrap();
        }
    }
}

/// Alice's accepted commit waits for Bob's durable part. After release an
/// unpinned retained handle refreshes owner policies; either pinned handle
/// keeps its admitted bundle, even when the owner changes during the wait.
#[test]
fn held_accepted_commits_reselect_applicable_policies_after_the_wait() {
    use jazz::db::InsertOptions;
    use jazz::db::shard::{CommitRef, Held, HeldRow};
    use jazz::tools::OpenTransactionId;
    use std::task::{Context, Poll, Waker};
    for pin in [None, Some(false), Some(true)] {
        for retained in [0, 1, 2] {
            if pin.is_some() && retained != 0 {
                continue;
            }
            for drift in [false, true] {
                let owner = pin.map_or(true, |allowed| !allowed);
                let server = open(9, AuthorSubject::SYSTEM, owner, true);
                seed(&server);
                let version = compile_schema(&schema(true)).version_id();
                let selected = match pin {
                    Some(allowed) => {
                        let bundle = block_on(server.publish_bundle(&schema(allowed))).unwrap();
                        server.check_session_pin(alice(), Some(&bundle)).unwrap();
                        Some(server.pinned_view(version, Some(&bundle)).unwrap().unwrap())
                    }
                    None if retained == 1 => Some(
                        block_on(server.register_schema_view(compile_schema(&schema(true))))
                            .unwrap(),
                    ),
                    None if retained == 2 => Some(server.governed_view(version).unwrap()),
                    None => None,
                };
                let view = selected.as_ref().unwrap_or(&server);
                let id = OpenTransactionId::new();
                block_on(view.begin_serializable_for_identity(id, alice())).unwrap();
                let prepared = view.prepare_query(&Query::from("notes")).unwrap();
                let read = block_on(
                    view.accepted_transaction_query(id, prepared, false, 1000)
                        .unwrap(),
                )
                .unwrap();
                assert_eq!(read.evidence.len(), usize::from(pin.unwrap_or(owner)));
                let row = RowUuid::from_bytes([77; 16]);
                block_on(view.tx_ref(id).insert(
                    "notes",
                    BTreeMap::from([("body".into(), Value::String("after wait".into()))]),
                    InsertOptions {
                        row_id: Some(row),
                        ..Default::default()
                    },
                ))
                .unwrap();
                let part = CommitRef {
                    node: [55; 16],
                    tx: 1,
                };
                assert!(matches!(
                    server
                        .shard()
                        .hold(
                            part,
                            &[HeldRow {
                                table: "notes".into(),
                                row: [77; 16]
                            }],
                            b"held part"
                        )
                        .unwrap(),
                    Held::At(_)
                ));
                let mut commit = Box::pin(view.commit_serializable_handle(id));
                let mut cx = Context::from_waker(Waker::noop());
                assert!(std::future::Future::poll(commit.as_mut(), &mut cx).is_pending());
                if drift {
                    server.set_identity_claims(
                        alice(),
                        BTreeMap::from([("changed".into(), Value::Bool(true))]),
                    );
                }
                block_on(server.activate_schema(compile_schema(&schema(!owner)))).unwrap();
                server.shard().release(part).unwrap();
                let Poll::Ready(result) = std::future::Future::poll(commit.as_mut(), &mut cx)
                else {
                    panic!("released commit stayed pending")
                };
                if drift {
                    let error = result.unwrap_err();
                    assert_eq!(error.code, ErrorCode::WriteRejected);
                    assert!(error.message.contains("claims changed"));
                } else if pin.is_some() {
                    result.expect("the authenticated pin survives owner activation");
                } else {
                    assert_eq!(result.unwrap_err().code, ErrorCode::TransactionConflict);
                }
                let all = server.prepare_query(&Query::from("notes")).unwrap();
                let rows = block_on(server.all(&all, Default::default())).unwrap();
                assert_eq!(
                    rows.iter().any(|r| r.row_uuid() == row),
                    pin.is_some() && !drift,
                    "rejected commit must publish nothing"
                );
            }
        }
    }
}

/// Alice admits an accepted read before Bob changes owner policies. Polling
/// later resolves unpinned root/registered/governed handles against the new
/// owner, while an explicitly admitted pin keeps its selected policies.
#[test]
fn deferred_accepted_evaluation_uses_explicit_governance_provenance() {
    use jazz::tools::OpenTransactionId;
    for owner in [false, true] {
        for kind in 0..5 {
            let server = open(9, AuthorSubject::SYSTEM, owner, true);
            seed(&server);
            let version = compile_schema(&schema(true)).version_id();
            let selected = match kind {
                0 => None,
                1 => Some(
                    block_on(server.register_schema_view(compile_schema(&schema(owner)))).unwrap(),
                ),
                2 => Some(server.pinned_view(version, None).unwrap().unwrap()),
                _ => {
                    let bundle = block_on(server.publish_bundle(&schema(kind == 4))).unwrap();
                    server.check_session_pin(alice(), Some(&bundle)).unwrap();
                    Some(server.pinned_view(version, Some(&bundle)).unwrap().unwrap())
                }
            };
            let db = selected.as_ref().unwrap_or(&server);
            let id = OpenTransactionId::new();
            block_on(db.begin_serializable_for_identity(id, alice())).unwrap();
            let query = db.prepare_query(&Query::from("notes")).unwrap();
            let pending = db
                .accepted_transaction_query(id, query, false, 1000)
                .unwrap();
            block_on(server.activate_schema(compile_schema(&schema(!owner)))).unwrap();
            let read = block_on(pending).unwrap();
            assert_eq!(
                read.evidence.len(),
                usize::from(if kind < 3 { !owner } else { kind == 4 }),
                "owner={owner}, kind={kind}"
            );
            db.abandon_transaction_handle(id).unwrap();
        }
    }
}
