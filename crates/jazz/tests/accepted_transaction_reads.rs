//! Accepted capability on ordinary identity-bound transaction lifetimes.
use crate::accepted_rpc_reads::{alice, bodies, claims, drive, open, schema, seed, tick};
use jazz::db::{Db, ErrorCode, ReadTier, block_on, duplex};
use jazz::ids::{AuthorSubject, RowUuid};
use jazz::query::{Query, col, eq, lit};
use jazz::records::Value;
use jazz::tools::OpenTransactionId;
use jazz::tx::DurabilityTier;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

fn begin(db: &Db) -> OpenTransactionId {
    let id = OpenTransactionId::new();
    block_on(db.begin_serializable_for_identity(id, alice())).unwrap();
    id
}
fn pair() -> (Db, Db) {
    let server = open(9, AuthorSubject::SYSTEM, true, false);
    let client = open(7, alice(), false, false);
    let (up, down) = duplex();
    block_on(client.connect_upstream(up));
    server.accept_subscriber(down, alice()).unwrap();
    (server, client)
}
fn poll<F: Future>(future: Pin<&mut F>) -> Poll<F::Output> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}
fn query(db: &Db) -> jazz::db::PreparedQuery {
    db.prepare_query(&Query::from("notes")).unwrap()
}

/// Alice must opt into a bound session; refusing the unsupported capability
/// leaves her ordinary unbound batch usable. Her bound read commits normally.
#[test]
fn bound_admission_preserves_ordinary_unbound_transactions() {
    let server = open(9, AuthorSubject::SYSTEM, true, false);
    seed(&server, 1, alice(), "a");
    let id = OpenTransactionId::new();
    block_on(server.begin_serializable(id)).unwrap();
    assert_eq!(
        server
            .accepted_transaction_query(id, query(&server), false, 1000)
            .err()
            .unwrap()
            .code,
        ErrorCode::Query
    );
    assert_eq!(
        block_on(server.tx_ref(id).all_prepared(&query(&server)))
            .unwrap()
            .len(),
        1
    );
    block_on(server.commit_serializable_handle(id)).unwrap();
    let id = begin(&server);
    assert_eq!(
        bodies(
            &block_on(
                server
                    .accepted_transaction_query(id, query(&server), false, 1000)
                    .unwrap()
            )
            .unwrap()
        ),
        ["note1"]
    );
    block_on(server.commit_serializable_handle(id)).unwrap();
}

/// Alice's cold cache is empty; her accepted read is certified by Bob's
/// authority and its read-only commit obtains a genuine Global result.
#[test]
fn cold_replica_predicates_validate_at_the_authority() {
    let (server, client) = pair();
    seed(&server, 1, alice(), "a");
    let id = begin(&client);
    let read = drive(
        &[&client, &server],
        client
            .accepted_transaction_query(id, query(&client), false, 1000)
            .unwrap(),
    )
    .unwrap();
    assert_eq!(bodies(&read), ["note1"]);
    assert!(
        block_on(client.all(
            &query(&client),
            jazz::db::ReadOpts::for_read_tier(ReadTier::LocalOnly)
        ))
        .unwrap()
        .is_empty()
    );
    let receipt = block_on(client.commit_serializable_handle(id)).unwrap();
    drive(
        &[&client, &server],
        client.wait_for_transaction(receipt.0, DurabilityTier::Global),
    )
    .unwrap();
}

/// Alice observes no matching note. Bob's insertion changes that predicate,
/// so even a read-only accepted commit is rejected at the authority.
#[test]
fn empty_accepted_predicate_rejects_an_authority_phantom() {
    let (server, client) = pair();
    let id = begin(&client);
    let read = drive(
        &[&client, &server],
        client
            .accepted_transaction_query(id, query(&client), false, 1000)
            .unwrap(),
    )
    .unwrap();
    assert!(read.evidence.is_empty());
    seed(&server, 1, alice(), "a");
    let receipt = block_on(client.commit_serializable_handle(id)).unwrap();
    crate::common::assert_serialization_conflict(
        &drive(
            &[&client, &server],
            client.wait_for_transaction(receipt.0, DurabilityTier::Global),
        )
        .unwrap_err(),
    );
}

/// Alice admits two reads before polling. Bob changes a row after the first
/// response; both reads retain the first cut and the commit detects the change.
#[test]
fn admitted_reads_share_the_existing_queue_and_cut() {
    let (server, client) = pair();
    seed(&server, 1, alice(), "a");
    let id = begin(&client);
    let first = client
        .accepted_transaction_query(id, query(&client), false, 1000)
        .unwrap();
    let second = client
        .accepted_transaction_query(id, query(&client), false, 1000)
        .unwrap();
    let first = drive(&[&client, &server], first).unwrap();
    block_on(server.update(
        "notes",
        RowUuid::from_bytes([1; 16]),
        [("body".into(), Value::String("later".into()))].into(),
        Default::default(),
    ))
    .unwrap();
    let second = drive(&[&client, &server], second).unwrap();
    assert_eq!(first, second);
    let receipt = block_on(client.commit_serializable_handle(id)).unwrap();
    crate::common::assert_serialization_conflict(
        &drive(
            &[&client, &server],
            client.wait_for_transaction(receipt.0, DurabilityTier::Global),
        )
        .unwrap_err(),
    );
}

/// Alice's premature commit consumes its handle before publishing anything;
/// neither an unpolled nor an issued accepted read can revive that batch.
#[test]
fn premature_commit_consumes_and_refuses_unfinished_reads() {
    for issued in [false, true] {
        let (server, client) = pair();
        let id = begin(&client);
        let mut read = Box::pin(
            client
                .accepted_transaction_query(id, query(&client), false, 1000)
                .unwrap(),
        );
        if issued {
            assert!(poll(read.as_mut()).is_pending());
            tick(&[&client]);
        }
        assert_eq!(
            block_on(client.commit_serializable_handle(id))
                .unwrap_err()
                .code,
            ErrorCode::NotObserved
        );
        assert!(block_on(client.commit_serializable_handle(id)).is_err());
        assert!(drive(&[&client, &server], read).is_err());
        tick(&[&client, &server]);
        assert_eq!(client.pending_once_state_for_test(), (0, 0, 0));
    }
}

/// Alice's cancelled accepted evidence invalidates only its own batch. Bob's
/// ordinary batch on the same database still reads and commits successfully.
#[test]
fn accepted_cancellation_does_not_poison_another_ordinary_batch() {
    let server = open(9, AuthorSubject::SYSTEM, true, false);
    seed(&server, 1, alice(), "a");
    let accepted = begin(&server);
    drop(
        server
            .accepted_transaction_query(accepted, query(&server), false, 1000)
            .unwrap(),
    );
    let ordinary = OpenTransactionId::new();
    block_on(server.begin_serializable(ordinary)).unwrap();
    assert_eq!(
        block_on(server.tx_ref(ordinary).all_prepared(&query(&server)))
            .unwrap()
            .len(),
        1
    );
    block_on(server.commit_serializable_handle(ordinary)).unwrap();
    assert_eq!(
        block_on(server.commit_serializable_handle(accepted))
            .unwrap_err()
            .code,
        ErrorCode::NotObserved
    );
}

/// Alice rolls back with queued or issued evidence, then reuses the numeric
/// handle. Bob's old answer cannot attach to the replacement read lifetime.
#[test]
fn rollback_and_reused_ids_do_not_adopt_old_reads() {
    for issued in [false, true] {
        let (server, client) = pair();
        seed(&server, 1, alice(), "a");
        let id = begin(&client);
        let mut old = Box::pin(
            client
                .accepted_transaction_query(id, query(&client), false, 1000)
                .unwrap(),
        );
        if issued {
            assert!(poll(old.as_mut()).is_pending());
            tick(&[&client]);
        }
        client.abandon_transaction_handle(id).unwrap();
        block_on(client.begin_serializable_for_identity(id, alice())).unwrap();
        assert!(drive(&[&client, &server], old).is_err());
        let result = drive(
            &[&client, &server],
            client
                .accepted_transaction_query(id, query(&client), false, 1000)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(bodies(&result), ["note1"]);
        let receipt = block_on(client.commit_serializable_handle(id)).unwrap();
        drive(
            &[&client, &server],
            client.wait_for_transaction(receipt.0, DurabilityTier::Global),
        )
        .unwrap();
    }
}

/// Alice's claim change cannot relabel already admitted evidence or preserve
/// an old credential at commit, even when the resulting rows would be equal.
#[test]
fn claim_drift_is_refused_at_admission_and_commit() {
    for before_read in [false, true] {
        let server = open(9, AuthorSubject::SYSTEM, true, false);
        server.set_identity_claims(alice(), claims("before"));
        let id = begin(&server);
        let read = server
            .accepted_transaction_query(id, query(&server), false, 1000)
            .unwrap();
        if before_read {
            server.set_identity_claims(alice(), claims("after"));
            assert!(block_on(read).is_err());
        } else {
            block_on(read).unwrap();
            server.set_identity_claims(alice(), claims("after"));
        }
        assert!(block_on(server.commit_serializable_handle(id)).is_err());
    }
}

/// Alice can mix ordinary and accepted reads under one schema/scope and cut.
/// Changing claims on the ordinary predicate refuses the mixed transaction.
#[test]
fn mixed_reads_share_scope_and_keep_ordinary_validation() {
    for accepted_first in [false, true] {
        let server = open(9, AuthorSubject::SYSTEM, true, false);
        seed(&server, 1, alice(), "a");
        let id = begin(&server);
        if accepted_first {
            block_on(
                server
                    .accepted_transaction_query(id, query(&server), false, 1000)
                    .unwrap(),
            )
            .unwrap();
        }
        assert_eq!(
            block_on(server.tx_ref(id).all_prepared(&query(&server)))
                .unwrap()
                .len(),
            1
        );
        if !accepted_first {
            block_on(
                server
                    .accepted_transaction_query(id, query(&server), false, 1000)
                    .unwrap(),
            )
            .unwrap();
        }
        block_on(server.commit_serializable_handle(id)).unwrap();
        let id = begin(&server);
        block_on(
            server
                .accepted_transaction_query(id, query(&server), false, 1000)
                .unwrap(),
        )
        .unwrap();
        let changed = query(&server).with_identity_claims(alice(), claims("different"));
        assert!(block_on(server.tx_ref(id).all_prepared(&changed)).is_err());
        assert!(block_on(server.commit_serializable_handle(id)).is_err());
    }
}

/// Alice cannot mistake already staged content for accepted evidence. The
/// admitted failure also prevents committing that transaction's staged write.
#[test]
fn staged_content_cannot_substitute_for_accepted_rows() {
    let server = open(9, AuthorSubject::SYSTEM, true, false);
    let id = begin(&server);
    block_on(
        server.tx_ref(id).insert(
            "notes",
            [
                ("body".into(), Value::String("staged".into())),
                ("owner".into(), Value::Uuid(alice().account_id().unwrap().0)),
                ("token".into(), Value::String("a".into())),
            ]
            .into(),
            Default::default(),
        ),
    )
    .unwrap();
    assert_eq!(
        server
            .accepted_transaction_query(id, query(&server), false, 1000)
            .err()
            .unwrap()
            .code,
        ErrorCode::NotObserved
    );
    assert!(block_on(server.commit_serializable_handle(id)).is_err());
    assert!(
        block_on(server.all(&query(&server), Default::default()))
            .unwrap()
            .is_empty()
    );
}

#[derive(Default)]
struct TimerHost(std::cell::RefCell<Vec<u64>>);
impl jazz::db::TickScheduler for TimerHost {
    fn schedule_tick(&self, _: jazz::db::TickUrgency) {}
    fn schedule_tick_after(&self, delay: u64) {
        self.0.borrow_mut().push(delay);
    }
}
#[derive(Default)]
struct WakeCount(std::sync::atomic::AtomicUsize);
impl std::task::Wake for WakeCount {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// Alice's accepted read waits behind an ordinary read with no upstream. Its
/// admission-time deadline wakes and expires in the queue; a different
/// ordinary batch still commits. No traffic is needed for that deadline.
#[test]
fn queued_expiry_uses_its_admission_budget_and_wakes_waiters() {
    let client = open(7, alice(), false, false);
    let host = std::rc::Rc::new(TimerHost::default());
    client.set_tick_scheduler(Some(host.clone()));
    let id = begin(&client);
    let prepared = query(&client);
    let transaction = client.tx_ref(id);
    let mut first = Box::pin(transaction.all_prepared_at(&prepared, false, None, ReadTier::Remote));
    assert!(poll(first.as_mut()).is_pending());
    let mut queued = Box::pin(
        client
            .accepted_transaction_query(id, query(&client), false, 200)
            .unwrap(),
    );
    let count = std::sync::Arc::new(WakeCount::default());
    let waker = Waker::from(count.clone());
    assert!(
        queued
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let delay = *host.0.borrow().last().expect("admission armed a timer");
    std::thread::sleep(std::time::Duration::from_millis(delay + 5));
    assert_eq!(count.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    tick(&[&client]);
    assert!(count.0.load(std::sync::atomic::Ordering::SeqCst) > 0);
    let Poll::Ready(Err(error)) = queued.as_mut().poll(&mut Context::from_waker(&waker)) else {
        panic!("queued read did not expire")
    };
    assert_eq!(error.code, ErrorCode::NotObserved);
    drop(queued);
    drop(first);
    assert_eq!(client.pending_once_state_for_test(), (0, 0, 0));
    assert_eq!(
        block_on(client.commit_serializable_handle(id))
            .unwrap_err()
            .code,
        ErrorCode::NotObserved
    );
    let other = OpenTransactionId::new();
    block_on(client.begin_serializable(other)).unwrap();
    block_on(client.tx_ref(other).all_prepared_at(
        &query(&client),
        false,
        None,
        ReadTier::LocalOnly,
    ))
    .unwrap();
    block_on(client.commit_serializable_handle(other)).unwrap();
}

/// Alice's ordinary predicate remains part of validation when her accepted
/// predicate names a different row. Bob changing only that ordinary row must
/// still invalidate the transaction.
#[test]
fn ordinary_predicates_are_not_skipped_in_a_mixed_batch() {
    let server = open(9, AuthorSubject::SYSTEM, true, false);
    seed(&server, 1, alice(), "a");
    seed(&server, 2, alice(), "a");
    let id = begin(&server);
    let by_id = |n| {
        server
            .prepare_query(&Query::from("notes").filter(eq(
                col("id"),
                lit(Value::Uuid(RowUuid::from_bytes([n; 16]).0)),
            )))
            .unwrap()
    };
    block_on(
        server
            .accepted_transaction_query(id, by_id(1), false, 1000)
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        block_on(server.tx_ref(id).all_prepared(&by_id(2)))
            .unwrap()
            .len(),
        1
    );
    block_on(server.update(
        "notes",
        RowUuid::from_bytes([2; 16]),
        [("body".into(), Value::String("changed".into()))].into(),
        Default::default(),
    ))
    .unwrap();
    assert_eq!(
        block_on(server.commit_serializable_handle(id))
            .unwrap_err()
            .code,
        ErrorCode::TransactionConflict
    );
}

/// Alice's ordinary snapshot includes her pending write. It cannot later be
/// relabelled accepted by adding a remote evidence read to the same batch.
#[test]
fn ordinary_pending_rows_cannot_be_relabelled_as_accepted() {
    let client = open(7, alice(), false, false);
    seed(&client, 1, alice(), "a");
    let id = begin(&client);
    assert_eq!(
        block_on(client.tx_ref(id).all_prepared_at(
            &query(&client),
            false,
            None,
            ReadTier::LocalOnly
        ))
        .unwrap()
        .len(),
        1
    );
    assert_eq!(
        block_on(
            client
                .accepted_transaction_query(id, query(&client), false, 1000)
                .unwrap()
        )
        .unwrap_err()
        .code,
        ErrorCode::NotObserved
    );
    assert!(block_on(client.commit_serializable_handle(id)).is_err());
}

/// Alice retains her schema handle across Bob's rename and can still commit.
/// Mixing the renamed handle into one accepted batch is explicitly refused.
#[test]
fn retained_schema_is_stable_and_schema_drift_is_refused() {
    use crate::common::{allow_all_policies, compile_schema};
    use jazz::protocol::{LensOp, MigrationLens, TableLens};
    use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
    let server = open(9, AuthorSubject::SYSTEM, true, false);
    seed(&server, 1, alice(), "a");
    let old = block_on(server.register_schema_view(schema(false))).unwrap();
    let v2 = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("tasks")
                    .column("title", ColumnType::Text)
                    .column("owner", ColumnType::Uuid)
                    .column("token", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let lens = MigrationLens::new(
        schema(false).version_id(),
        v2.version_id(),
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
    block_on(server.activate_schema_with_lenses(v2, &[lens])).unwrap();
    let id = begin(&old);
    assert_eq!(
        bodies(
            &block_on(
                old.accepted_transaction_query(id, query(&old), false, 1000)
                    .unwrap()
            )
            .unwrap()
        ),
        ["note1"]
    );
    block_on(old.commit_serializable_handle(id)).unwrap();
    let id = begin(&old);
    block_on(
        old.accepted_transaction_query(id, query(&old), false, 1000)
            .unwrap(),
    )
    .unwrap();
    assert!(
        server
            .accepted_transaction_query(
                id,
                server.prepare_query(&Query::from("tasks")).unwrap(),
                false,
                1000
            )
            .is_err()
    );
    assert!(block_on(old.commit_serializable_handle(id)).is_err());
}

/// Bob revokes Alice's support row after her accepted read. Re-evaluation
/// checks current support at commit, both locally and at a remote authority.
#[test]
fn current_permission_support_is_rechecked_at_commit() {
    use crate::common::{allow_all_policies, compile_schema, exists, session_eq};
    use jazz::db::{DbConfig, DbIdentity};
    use jazz::ids::NodeUuid;
    use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
    let permitted = exists("grants", vec![session_eq("owner", &["user", "account"])]);
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("grants")
                    .column("owner", ColumnType::Uuid)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .policies(
                        allow_all_policies()
                            .with_select(permitted.clone())
                            .with_insert(permitted),
                    ),
            )
            .build(),
    );
    for remote in [false, true] {
        for empty in [false, true] {
            let make = |node, author| {
                block_on(Db::open(DbConfig::new(
                    schema.clone(),
                    DbIdentity {
                        node: NodeUuid::from_bytes([node; 16]),
                        author,
                    },
                )))
                .unwrap()
            };
            let server = make(9, AuthorSubject::SYSTEM);
            server.set_authority();
            let grant = block_on(server.insert(
                "grants",
                [("owner".into(), Value::Uuid(alice().account_id().unwrap().0))].into(),
                Default::default(),
            ))
            .unwrap()
            .row_uuid();
            block_on(server.insert(
                "notes",
                [("body".into(), Value::String("visible".into()))].into(),
                Default::default(),
            ))
            .unwrap();
            let client = make(7, alice());
            client.become_replica();
            let (up, down) = duplex();
            block_on(client.connect_upstream(up));
            server.accept_subscriber(down, alice()).unwrap();
            let reader = if remote { &client } else { &server };
            let id = begin(reader);
            let q = if empty {
                Query::from("notes").limit(0)
            } else {
                Query::from("notes")
            };
            let read = drive(
                &[&client, &server],
                reader
                    .accepted_transaction_query(id, reader.prepare_query(&q).unwrap(), false, 1000)
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(read.evidence.len(), usize::from(!empty));
            if empty {
                // Empty evidence stays empty after revocation: only normal
                // commit-time write permissions can refuse this staged insert.
                if remote {
                    // Hydrate policy support through the ordinary read route.
                    drive(
                        &[&client, &server],
                        client.all_serialized_query(
                            &postcard::to_allocvec(&Query::from("grants")).unwrap(),
                            jazz::db::ReadOpts::for_read_tier(ReadTier::Remote),
                            None,
                            None,
                            None,
                        ),
                    )
                    .unwrap();
                }
                block_on(reader.tx_ref(id).insert(
                    "notes",
                    [("body".into(), Value::String("must refuse".into()))].into(),
                    Default::default(),
                ))
                .unwrap();
            }
            block_on(server.delete("grants", grant, Default::default())).unwrap();
            let result = block_on(reader.commit_serializable_handle(id));
            if remote {
                let receipt = result.unwrap();
                let error = drive(
                    &[&client, &server],
                    reader.wait_for_transaction(receipt.0, DurabilityTier::Global),
                )
                .unwrap_err();
                assert_eq!(error.code, ErrorCode::WriteRejected);
                if !empty {
                    crate::common::assert_serialization_conflict(&error);
                }
            } else {
                assert_eq!(
                    result.unwrap_err().code,
                    if empty {
                        ErrorCode::WriteRejected
                    } else {
                        ErrorCode::TransactionConflict
                    }
                );
            }
            assert_eq!(
                block_on(server.all(
                    &server.prepare_query(&Query::from("notes")).unwrap(),
                    Default::default()
                ))
                .unwrap()
                .len(),
                1
            );
        }
    }
}

/// Alice drops accepted evidence while queued behind an ordinary read. The
/// shared guard wakes the predecessor, closes this batch's evidence path and
/// leaves no request/answer state behind when that predecessor finishes.
#[test]
fn queued_cancellation_wakes_the_shared_read_lifetime() {
    let client = open(7, alice(), false, false);
    let id = begin(&client);
    let transaction = client.tx_ref(id);
    let prepared = query(&client);
    let count = std::sync::Arc::new(WakeCount::default());
    let waker = Waker::from(count.clone());
    let mut first = Box::pin(transaction.all_prepared_at(&prepared, false, None, ReadTier::Remote));
    assert!(
        first
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let queued = client
        .accepted_transaction_query(id, query(&client), false, 1000)
        .unwrap();
    assert_eq!(count.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    drop(queued);
    assert!(count.0.load(std::sync::atomic::Ordering::SeqCst) > 0);
    let Poll::Ready(Err(error)) = first.as_mut().poll(&mut Context::from_waker(&waker)) else {
        panic!("predecessor did not observe the failed accepted capability")
    };
    assert_eq!(error.code, ErrorCode::NotObserved);
    assert_eq!(client.pending_once_state_for_test(), (0, 0, 0));
    assert!(block_on(client.commit_serializable_handle(id)).is_err());
}

/// Alice reads accepted rows before staging another row. Her later ordinary
/// read still sees that admitted staging prefix and commits with the usual
/// baseline predicates, without relabelling staged data as accepted evidence.
#[test]
fn ordinary_reads_after_accepted_evidence_keep_staged_visibility() {
    let server = open(9, AuthorSubject::SYSTEM, true, false);
    seed(&server, 1, alice(), "a");
    let id = begin(&server);
    block_on(
        server
            .accepted_transaction_query(id, query(&server), false, 1000)
            .unwrap(),
    )
    .unwrap();
    block_on(
        server.tx_ref(id).insert(
            "notes",
            [
                ("body".into(), Value::String("staged".into())),
                ("owner".into(), Value::Uuid(alice().account_id().unwrap().0)),
                ("token".into(), Value::String("a".into())),
            ]
            .into(),
            Default::default(),
        ),
    )
    .unwrap();
    assert_eq!(
        block_on(server.tx_ref(id).all_prepared(&query(&server)))
            .unwrap()
            .len(),
        2
    );
    block_on(server.commit_serializable_handle(id)).unwrap();
    assert_eq!(
        block_on(server.all(&query(&server), Default::default()))
            .unwrap()
            .len(),
        2
    );
}

/// Alice's earlier ordinary read used different explicit claims. Adding
/// accepted mode later cannot silently relabel that already observed scope.
#[test]
fn ordinary_claims_are_checked_when_accepted_mode_is_added() {
    let server = open(9, AuthorSubject::SYSTEM, true, false);
    let id = begin(&server);
    let changed = query(&server).with_identity_claims(alice(), claims("prior"));
    block_on(server.tx_ref(id).all_prepared(&changed)).unwrap();
    assert!(
        block_on(
            server
                .accepted_transaction_query(id, query(&server), false, 1000)
                .unwrap()
        )
        .is_err()
    );
    assert!(block_on(server.commit_serializable_handle(id)).is_err());
}

/// Bob's same-version policy revision applies before Alice first polls, even
/// when Alice admitted her query through a retained schema handle.
#[test]
fn policy_revision_before_first_poll_uses_current_authorization() {
    for retained in [false, true] {
        let server = open(9, AuthorSubject::SYSTEM, true, false);
        seed(&server, 1, alice(), "a");
        let view = block_on(server.register_schema_view(schema(false))).unwrap();
        let db = if retained { &view } else { &server };
        let id = begin(db);
        let pending = db
            .accepted_transaction_query(id, query(db), false, 1000)
            .unwrap();
        block_on(server.activate_schema(schema(true))).unwrap();
        let read = block_on(pending).unwrap();
        assert!(
            read.evidence.is_empty(),
            "revoked SELECT returned Alice's note (retained={retained})"
        );
        db.abandon_transaction_handle(id).unwrap();
    }
}

/// Bob revokes SELECT after Alice reads. Her accepted predicate must conflict
/// at commit under current policies, including through a retained schema handle.
#[test]
fn policy_revision_before_commit_invalidates_accepted_predicate() {
    for retained in [false, true] {
        let server = open(9, AuthorSubject::SYSTEM, true, false);
        seed(&server, 1, alice(), "a");
        let view = block_on(server.register_schema_view(schema(false))).unwrap();
        let db = if retained { &view } else { &server };
        let id = begin(db);
        let read = block_on(
            db.accepted_transaction_query(id, query(db), false, 1000)
                .unwrap(),
        )
        .unwrap();
        assert_eq!(read.evidence.len(), 1);
        block_on(server.activate_schema(schema(true))).unwrap();
        assert_eq!(
            block_on(db.commit_serializable_handle(id))
                .unwrap_err()
                .code,
            ErrorCode::TransactionConflict
        );
    }
}

/// Alice's rejected mixed-scope read wakes her pending accepted task immediately.
/// The waker re-enters a public API to catch notifications under an engine borrow.
#[test]
fn mixed_scope_failure_wakes_pending_read_without_traffic() {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::task::Wake;
    thread_local! {
        static REENTER: std::cell::RefCell<Option<std::rc::Rc<Db>>> = const { std::cell::RefCell::new(None) };
    }
    #[derive(Default)]
    struct ReenterWake(AtomicUsize);
    impl Wake for ReenterWake {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
            REENTER.with(|db| {
                db.borrow()
                    .as_ref()
                    .unwrap()
                    .prepare_query(&Query::from("notes"))
                    .unwrap();
            });
        }
    }
    let client = std::rc::Rc::new(open(7, alice(), false, false));
    let id = begin(&client);
    let mut pending = Box::pin(
        client
            .accepted_transaction_query(id, query(&client), false, 60_000)
            .unwrap(),
    );
    let count = Arc::new(ReenterWake::default());
    let waker = Waker::from(count.clone());
    assert!(
        pending
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    REENTER.with(|db| *db.borrow_mut() = Some(client.clone()));
    let changed = query(&client).with_identity_claims(alice(), claims("different"));
    let error = block_on(client.tx_ref(id).all_prepared_at(
        &changed,
        false,
        None,
        ReadTier::LocalOnly,
    ))
    .unwrap_err();
    assert_eq!(error.code, ErrorCode::WriteRejected);
    assert!(
        count.0.load(Ordering::SeqCst) > 0,
        "failed read left accepted task asleep"
    );
    let Poll::Ready(Err(error)) = pending.as_mut().poll(&mut Context::from_waker(&waker)) else {
        panic!("failed accepted task did not promptly refuse")
    };
    assert_eq!(error.code, ErrorCode::WriteRejected);
    REENTER.with(|db| *db.borrow_mut() = None);
}

/// Alice’s replica-local SELECT is not a delegated session SELECT. Keep the actual
/// public-begin pending-own snapshot, without relabelling it as authority data.
#[test]
fn client_evidence_keeps_the_ordinary_begin_snapshot_and_refuses_pending_own_relabelling() {
    let client = open(7, alice(), false, true);
    seed(&client, 1, alice(), "a");
    // The existing bound-session contract is unchanged: absent claims deny.
    let bound = begin(&client);
    assert!(
        block_on(client.tx_ref(bound).all_prepared_at(
            &query(&client),
            false,
            None,
            ReadTier::LocalOnly,
        ))
        .unwrap()
        .is_empty()
    );
    client.abandon_transaction_handle(bound).unwrap();

    let id = OpenTransactionId::new();
    block_on(client.begin_client_evidence_transaction(id)).unwrap();
    seed(&client, 2, alice(), "a");
    assert_eq!(
        block_on(client.tx_ref(id).all_prepared_at(
            &query(&client),
            false,
            None,
            ReadTier::LocalOnly,
        ))
        .unwrap()
        .len(),
        1,
        "only the own write committed before begin is visible"
    );
    assert_eq!(
        block_on(
            client
                .accepted_transaction_query(id, query(&client), false, 1000,)
                .unwrap()
        )
        .unwrap_err()
        .code,
        ErrorCode::NotObserved
    );
    assert!(block_on(client.commit_serializable_handle(id)).is_err());
}

/// Alice’s same-batch accepted predicates remain genuine authority reads. A point
/// read can also see this batch's own staged row without local session policy.
#[test]
fn client_evidence_stages_and_reads_ordinary_rows_after_an_accepted_prefix() {
    let (server, client) = pair();
    seed(&server, 1, alice(), "a");
    let id = OpenTransactionId::new();
    block_on(client.begin_client_evidence_transaction(id)).unwrap();
    let accepted = drive(
        &[&client, &server],
        client
            .accepted_transaction_query(id, query(&client), false, 1000)
            .unwrap(),
    )
    .unwrap();
    assert_eq!(bodies(&accepted), ["note1"]);
    let row = RowUuid::from_bytes([42; 16]);
    block_on(
        client.tx_ref(id).insert(
            "notes",
            [
                ("body".into(), Value::String("staged".into())),
                ("owner".into(), Value::Uuid(alice().account_id().unwrap().0)),
                ("token".into(), Value::String("a".into())),
            ]
            .into(),
            jazz::db::InsertOptions {
                row_id: Some(row),
                ..Default::default()
            },
        ),
    )
    .unwrap();
    let point = client
        .prepare_query(&Query::from("notes").filter(eq(col("id"), lit(Value::Uuid(row.0)))))
        .unwrap();
    assert_eq!(
        block_on(
            client
                .tx_ref(id)
                .all_prepared_at(&point, false, None, ReadTier::LocalOnly,)
        )
        .unwrap()
        .len(),
        1
    );
    let receipt = block_on(client.commit_serializable_handle(id)).unwrap();
    drive(
        &[&client, &server],
        client.wait_for_transaction(receipt.0, DurabilityTier::Global),
    )
    .unwrap();
    assert_eq!(
        block_on(server.all(&query(&server), Default::default()))
            .unwrap()
            .len(),
        2
    );
}

/// Alice cannot use client mode as an authority/backend escape or upgrade an
/// already opened handle. Existing ordinary/explicit session modes are intact.
#[test]
fn client_evidence_refuses_authorities_and_late_upgrade() {
    for author in [alice(), AuthorSubject::SYSTEM] {
        let authority = open(9, author, true, false);
        let id = OpenTransactionId::new();
        assert!(block_on(authority.begin_client_evidence_transaction(id)).is_err());
        assert!(block_on(authority.commit_serializable_handle(id)).is_err());
    }
    let client = open(7, alice(), false, false);
    let id = OpenTransactionId::new();
    block_on(client.begin_serializable(id)).unwrap();
    assert!(block_on(client.begin_client_evidence_transaction(id)).is_err());
    assert!(
        client
            .accepted_transaction_query(id, query(&client), false, 1000)
            .is_err()
    );
    client.abandon_transaction_handle(id).unwrap();
}

/// Alice’s live scope checks still reject credential drift; the implicit client mode
/// cannot smuggle a selected reader or cached explicit claim scope into it.
#[test]
fn client_evidence_refuses_session_selection_and_live_claim_drift() {
    let (server, client) = pair();
    let id = OpenTransactionId::new();
    block_on(client.begin_client_evidence_transaction(id)).unwrap();
    let explicit = query(&client).with_identity_claims(alice(), claims("frozen"));
    assert!(
        client
            .accepted_transaction_query(id, explicit, false, 1000)
            .is_err()
    );
    assert!(
        block_on(client.tx_ref(id).all_prepared_at(
            &query(&client),
            false,
            Some(alice()),
            ReadTier::LocalOnly,
        ))
        .is_err()
    );
    let accepted = drive(
        &[&client, &server],
        client
            .accepted_transaction_query(id, query(&client), false, 1000)
            .unwrap(),
    )
    .unwrap();
    assert!(accepted.evidence.is_empty());
    client.set_identity_claims(alice(), claims("changed"));
    assert!(block_on(client.commit_serializable_handle(id)).is_err());
}

/// Alice’s replica-local view never supplies completeness. Empty accepted
/// predicates still reach the authority and catch a later inserted phantom.
#[test]
fn client_evidence_empty_predicate_still_refuses_an_authority_phantom() {
    let (server, client) = pair();
    let id = OpenTransactionId::new();
    block_on(client.begin_client_evidence_transaction(id)).unwrap();
    let accepted = drive(
        &[&client, &server],
        client
            .accepted_transaction_query(id, query(&client), false, 1000)
            .unwrap(),
    )
    .unwrap();
    assert!(accepted.evidence.is_empty());
    seed(&server, 1, alice(), "a");
    let receipt = block_on(client.commit_serializable_handle(id)).unwrap();
    crate::common::assert_serialization_conflict(
        &drive(
            &[&client, &server],
            client.wait_for_transaction(receipt.0, DurabilityTier::Global),
        )
        .unwrap_err(),
    );
}

/// Bob’s authority rechecks SELECT under its current policies for Alice too;
/// an old accepted reply cannot preserve a revoked policy at Global commit.
#[test]
fn client_evidence_does_not_preserve_revoked_authority_select() {
    let (server, client) = pair();
    seed(&server, 1, alice(), "a");
    let id = OpenTransactionId::new();
    block_on(client.begin_client_evidence_transaction(id)).unwrap();
    let accepted = drive(
        &[&client, &server],
        client
            .accepted_transaction_query(id, query(&client), false, 1000)
            .unwrap(),
    )
    .unwrap();
    assert_eq!(bodies(&accepted), ["note1"]);
    block_on(server.activate_schema(schema(true))).unwrap();
    let receipt = block_on(client.commit_serializable_handle(id)).unwrap();
    crate::common::assert_serialization_conflict(
        &drive(
            &[&client, &server],
            client.wait_for_transaction(receipt.0, DurabilityTier::Global),
        )
        .unwrap_err(),
    );
}

/// Changing Alice’s actual owner role invalidates this capability before any
/// subsequent read, write or commit can use its earlier replica admission.
#[test]
fn client_evidence_refuses_an_owner_role_change() {
    let client = open(7, alice(), false, false);
    let id = OpenTransactionId::new();
    block_on(client.begin_client_evidence_transaction(id)).unwrap();
    client.set_authority();
    assert!(block_on(client.tx_ref(id).all_prepared(&query(&client))).is_err());
    assert!(
        block_on(
            client.tx_ref(id).insert(
                "notes",
                [
                    ("body".into(), Value::String("must not publish".into())),
                    ("owner".into(), Value::Uuid(alice().account_id().unwrap().0)),
                    ("token".into(), Value::String("a".into())),
                ]
                .into(),
                Default::default()
            )
        )
        .is_err()
    );
    assert!(block_on(client.commit_serializable_handle(id)).is_err());
    assert!(
        block_on(client.all(&query(&client), Default::default()))
            .unwrap()
            .is_empty()
    );
}
