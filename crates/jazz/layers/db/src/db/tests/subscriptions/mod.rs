//! Subscription tests grouped by the same lifecycle boundaries as the implementation.

use super::*;
use groove::storage::TestStorage;

mod authorization;
mod coverage;
mod materialization;
mod publication;
mod structured;

/// Internal diagnostic: the native trace shows subscriptions refreshing in a
/// different order from Groove ID allocation after a full catalogue rebuild.
/// Public schemas/streams are used; only the rebuild boundary and registry
/// order are controlled to reproduce that lifecycle without private stores.
#[test]
fn full_rebuild_must_not_unsubscribe_another_replacement() {
    let db = block_on(doctest_support::open_todos_db()).expect("open fixture");
    let prepared = db.prepare_query(&db.table("todos")).expect("prepare");
    let mut first = block_on(db.subscribe(&prepared, ReadOpts::default())).expect("first");
    let mut second = block_on(db.subscribe(&prepared, ReadOpts::default())).expect("second");
    let _ = block_on(first.next_raw()).expect("first opening");
    let _ = block_on(second.next_raw()).expect("second opening");
    db.node.subscriptions.borrow_mut().reverse();
    block_on(async {
        db.node
            .node
            .lock()
            .await
            .rebuild_groove_runtime_for_test()
            .await
            .expect("rebuild");
    });
    for _ in 0..4 {
        block_on(db.node.refresh_subscriptions()).expect("replacement streams must remain live");
    }
}

/// Alice invalidates prepared plans without replacing Groove. The old handles
/// must still be retired, so each refresh leaves exactly two live subscriptions.
/// Internal because only the node owner can distinguish these two boundaries.
#[test]
fn plan_invalidation_retires_same_runtime_subscriptions() {
    let db = block_on(doctest_support::open_todos_db()).expect("open fixture");
    let prepared = db.prepare_query(&db.table("todos")).expect("prepare");
    let mut first = block_on(db.subscribe(&prepared, ReadOpts::default())).expect("first");
    let mut second = block_on(db.subscribe(&prepared, ReadOpts::default())).expect("second");
    block_on(first.next_raw()).expect("first opening");
    block_on(second.next_raw()).expect("second opening");
    for _ in 0..3 {
        block_on(async {
            db.node
                .node
                .lock()
                .await
                .invalidate_groove_runtime_for_test();
        });
        block_on(db.node.refresh_subscriptions()).expect("refresh");
        assert_eq!(db.active_groove_subscriptions_for_test(), 2);
    }
}

/// Internal because the test-only refresh rendezvous owns thread-local state.
/// Dropping its caller handle must clear that state without recursively
/// borrowing the registry, so a later owner refresh remains unblocked.
#[test]
fn dropping_subscription_refresh_pause_clears_its_registry() {
    let pause = crate::db::node_runtime::pause_subscription_refresh_after_detach_for_test();
    drop(pause);
    let db = block_on(doctest_support::open_todos_db()).expect("open refresh fixture");
    assert_eq!(
        block_on(db.node.refresh_subscriptions()).expect("refresh after pause drop"),
        0
    );
}

/// Internal because the public cancellation contract crosses the runtime
/// owner's detached-maintained-view boundary. A test-only rendezvous is needed
/// to order that handoff deterministically; the observable assertion is that
/// Alice's replacement stream remains live after cancelling its predecessor.
///
/// alice old stream ──refresh detaches──► owner wait
/// alice cancel ──finalize old id───────► replacement remains live
#[test]
fn cancelling_a_detached_subscription_does_not_reject_its_replacement() {
    let db = block_on(doctest_support::open_todos_db()).expect("open subscription fixture");
    let prepared = db
        .prepare_query(&db.table("todos"))
        .expect("prepare replacement query");
    let cancelled =
        block_on(db.subscribe(&prepared, ReadOpts::default())).expect("open cancelled stream");
    let mut replacement =
        block_on(db.subscribe(&prepared, ReadOpts::default())).expect("open replacement stream");
    let _ = block_on(replacement.next_raw()).expect("receive replacement opening");
    let pause = crate::db::node_runtime::pause_subscription_refresh_after_detach_for_test();
    let mut refresh = Box::pin(db.node.refresh_subscriptions());
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    assert!(matches!(refresh.as_mut().poll(&mut context), Poll::Pending));
    assert!(pause.entered(), "refresh detached the cancelled stream");

    drop(cancelled);
    block_on(db.node.drain_subscription_finalizations())
        .expect("retire the cancelled stream before refresh resumes");
    pause.release();
    assert!(matches!(
        refresh.as_mut().poll(&mut context),
        Poll::Ready(Ok(_))
    ));
    assert!(
        replacement.try_next_event().is_none(),
        "replacement was not rejected"
    );
    block_on(db.insert(
        "todos",
        doctest_support::todo_cells("after cancellation", false),
        Default::default(),
    ))
    .expect("write after cancellation");
    let SubscriptionEvent::Delta { added, .. } =
        block_on(replacement.next_raw()).expect("replacement receives later write")
    else {
        panic!("expected replacement delta after cancellation");
    };
    assert_eq!(added.len(), 1, "replacement stays live for later writes");
}

/// Internal because deterministic cancellation needs to pause a public
/// `subscribe` between node-mutex acquisition and maintained-view ownership.
/// A close that is then cancelled must reject the stranded opener and retire
/// the local Groove subscription it created after the close snapshot.
#[test]
fn cancelled_close_retires_a_subscription_opener_that_was_waiting_for_the_node() {
    let db = block_on(doctest_support::open_todos_db()).unwrap();
    let prepared = db
        .prepare_query(&db.table("todos"))
        .expect("prepare controlled subscription query");

    block_on(async {
        let node_owner = db.node.node.lock().await;
        let mut opening = Box::pin(db.subscribe(&prepared, ReadOpts::default()));
        let waker = Waker::noop();
        let mut context = Context::from_waker(waker);
        assert!(matches!(opening.as_mut().poll(&mut context), Poll::Pending));

        let mut closing = Box::pin(db.close());
        assert!(matches!(closing.as_mut().poll(&mut context), Poll::Pending));
        drop(closing);
        drop(node_owner);

        let error = match opening.await {
            Ok(_) => panic!("closed admission rejects the opener"),
            Err(error) => error,
        };
        assert_eq!(error.code, ErrorCode::Protocol);
        assert!(error.message.contains("subscription admission is closed"));
        db.tick()
            .await
            .expect("retire opening cleanup after cancellation");
        assert_eq!(db.active_groove_subscriptions_for_test(), 0);
        assert!(
            db.node
                .pending_subscription_finalizations
                .borrow()
                .is_empty()
        );
    });
}

/// Alice drops a local subscription while Bob owns the async node mutex. This
/// lower-level test is necessary because the contract is specifically that
/// `Drop` does not acquire that mutex; the public effect is verified after the
/// next owner tick drains the queued command.
///
/// bob ──hold node mutex──► cold-capable node owner
/// alice ──drop stream────► finalization queue ──tick──► Groove unsubscribe
#[test]
fn dropping_subscription_while_node_mutex_is_held_queues_finalization() {
    let schema = schema();
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let db = block_on(Db::open(DbConfig {
        schema,
        storage: TestStorage::new(&refs),
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x42; 16]),
            author: AuthorSubject::for_test_bytes([0x43; 16]),
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0x4243))),
    }))
    .expect("open controlled subscription db");
    let prepared = db
        .prepare_query(&db.table("todos"))
        .expect("prepare todos query");
    let subscription =
        block_on(db.subscribe(&prepared, ReadOpts::default())).expect("open local subscription");
    assert_eq!(db.active_groove_subscriptions_for_test(), 1);

    let node_owner = block_on(db.node.node.lock());
    drop(subscription);
    assert_eq!(node_owner.runtime_stats_for_test().active_subscriptions, 1);
    drop(node_owner);

    block_on(db.tick()).expect("drain finalization command");
    assert_eq!(db.active_groove_subscriptions_for_test(), 0);
    assert!(db.node.upstream_coverage_refcounts.borrow().is_empty());
    assert!(db.node.latest_coverage_subscriptions.borrow().is_empty());
    assert!(db.node.upstream_subscription_owners.borrow().is_empty());
    assert!(
        db.node
            .upstream_subscriptions
            .borrow()
            .iter()
            .any(|command| matches!(command, PendingUpstreamCommand::Unsubscribe(_)))
    );
}

fn existing_subscription_rejections() -> Vec<SubscribeRejectReason> {
    let mut reasons = [
        SubscribeServerFailureCode::TableNotFound,
        SubscribeServerFailureCode::SchemaResolution,
        SubscribeServerFailureCode::QueryValidation,
        SubscribeServerFailureCode::QueryLowering,
        SubscribeServerFailureCode::PolicyEvaluation,
        SubscribeServerFailureCode::Internal,
    ]
    .into_iter()
    .map(|code| SubscribeRejectReason::ServerFailure { code })
    .collect::<Vec<_>>();
    reasons.extend([
        SubscribeRejectReason::UnsupportedShapeCapability {
            detail: "unsupported shape".to_owned(),
        },
        SubscribeRejectReason::ShapeRegistrationPendingCatalogueAdmission,
        SubscribeRejectReason::InvalidAuthoritySourceClosure {
            transition: "invalid closure".to_owned(),
        },
    ]);
    reasons
}

/// Alice's low-level stream keeps publishing local rows after every existing
/// rejection reason from Bob. The private delivery seam injects exact reasons
/// without depending on unrelated server compilation or policy failures.
#[test]
fn existing_rejections_keep_low_level_subscription_live() {
    for reason in existing_subscription_rejections() {
        for poll in [false, true] {
            let db = block_on(doctest_support::open_todos_db()).unwrap();
            let prepared = db.prepare_query(&db.table("todos")).unwrap();
            let mut stream = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
            block_on(stream.next_raw()).expect("Alice's local opening");
            stream
                ._state
                .borrow()
                .send_rejection(SubscriptionEvent::Rejected {
                    reason: reason.clone(),
                })
                .unwrap();
            let event = if poll {
                let mut cx = Context::from_waker(Waker::noop());
                match Pin::new(&mut stream).poll_next(&mut cx) {
                    Poll::Ready(event) => event,
                    Poll::Pending => panic!("queued rejection must be ready"),
                }
            } else {
                stream.try_next_event()
            };
            assert!(
                matches!(event, Some(SubscriptionEvent::Rejected { reason: delivered }) if delivered == reason)
            );
            block_on(db.insert(
                "todos",
                doctest_support::todo_cells("after rejection", false),
                Default::default(),
            ))
            .unwrap();
            block_on(db.tick()).unwrap();
            assert!(
                matches!(stream.try_next_event(), Some(SubscriptionEvent::Delta { added, .. }) if added.len() == 1),
                "existing rejection {reason:?} must leave Alice's stream live"
            );
        }
    }
}

/// Alice's cached window survives Bob's existing rejection reasons; only a
/// result-protocol rejection ends it. Injecting rejection at the private seam
/// isolates fallback lifetime from server query validation.
///
/// alice: window waits -> link fails -> cached [b] -> Bob rejects -> insert aa
///        existing reason: cached [aa]; QueryResultProtocol: stream ends.
#[test]
fn window_fallback_survives_existing_rejections_and_terminates_on_result_protocol() {
    let mut reasons = existing_subscription_rejections();
    reasons.push(SubscribeRejectReason::ServerFailure {
        code: SubscribeServerFailureCode::QueryResultProtocol,
    });
    for reason in reasons {
        let terminal = matches!(
            reason,
            SubscribeRejectReason::ServerFailure {
                code: SubscribeServerFailureCode::QueryResultProtocol
            }
        );
        let db = block_on(doctest_support::open_todos_db()).unwrap();
        for title in ["a", "b", "c"] {
            block_on(db.insert(
                "todos",
                doctest_support::todo_cells(title, false),
                Default::default(),
            ))
            .unwrap();
        }
        db.set_remote_link_hint(RemoteLinkHint::Live);
        let query = db
            .table("todos")
            .order_by("title", OrderDirection::Asc)
            .offset(1)
            .limit(1);
        let prepared = db.prepare_query(&query).unwrap();
        let mut stream = block_on(db.subscribe(
            &prepared,
            ReadOpts {
                first_load: FirstLoad::WaitForRemote { timeout_ms: 60_000 },
                ..ReadOpts::default()
            },
        ))
        .unwrap();
        assert!(
            stream.try_next_event().is_none(),
            "remote opening is withheld"
        );
        db.set_remote_link_hint(RemoteLinkHint::Failed);
        assert!(
            matches!(stream.try_next_event(), Some(SubscriptionEvent::Delta { reset: true, added, .. }) if added.len() == 1)
        );
        stream
            ._state
            .borrow()
            .send_rejection(SubscriptionEvent::Rejected {
                reason: reason.clone(),
            })
            .unwrap();
        assert!(
            matches!(stream.try_next_event(), Some(SubscriptionEvent::Rejected { reason: delivered }) if delivered == reason)
        );
        block_on(db.insert(
            "todos",
            doctest_support::todo_cells("aa", false),
            Default::default(),
        ))
        .unwrap();
        block_on(db.tick()).unwrap();
        let event = stream.try_next_event();
        if terminal {
            assert!(event.is_none());
            let mut cx = Context::from_waker(Waker::noop());
            assert!(matches!(
                Pin::new(&mut stream).poll_next(&mut cx),
                Poll::Ready(None)
            ));
        } else {
            assert!(
                matches!(event, Some(SubscriptionEvent::Delta { added, .. }) if added.len() == 1),
                "existing rejection {reason:?} must preserve the cached window"
            );
        }
    }
}

/// Bob's closure rejection releases Alice's current WaitForRemote opening
/// before the rejection, and Alice can keep reading subsequent local writes.
/// The private rejection seam makes this first-load ordering deterministic.
#[test]
fn closure_rejection_releases_wait_for_remote_opening_and_keeps_stream_live() {
    let db = block_on(doctest_support::open_todos_db()).unwrap();
    db.set_remote_link_hint(RemoteLinkHint::Live);
    let prepared = db.prepare_query(&db.table("todos")).unwrap();
    let mut stream = block_on(db.subscribe(
        &prepared,
        ReadOpts {
            first_load: FirstLoad::WaitForRemote { timeout_ms: 60_000 },
            ..ReadOpts::default()
        },
    ))
    .unwrap();
    assert!(
        stream.try_next_event().is_none(),
        "Alice's opening is withheld"
    );
    stream
        ._state
        .borrow()
        .send_rejection(SubscriptionEvent::Rejected {
            reason: SubscribeRejectReason::InvalidAuthoritySourceClosure {
                transition: "invalid closure".to_owned(),
            },
        })
        .unwrap();
    assert!(matches!(
        stream.try_next_event(),
        Some(SubscriptionEvent::Delta { reset: true, .. })
    ));
    assert!(matches!(
        stream.try_next_event(),
        Some(SubscriptionEvent::Rejected {
            reason: SubscribeRejectReason::InvalidAuthoritySourceClosure { .. }
        })
    ));
    block_on(db.insert(
        "todos",
        doctest_support::todo_cells("after closure rejection", false),
        Default::default(),
    ))
    .unwrap();
    block_on(db.tick()).unwrap();
    assert!(
        matches!(stream.try_next_event(), Some(SubscriptionEvent::Delta { added, .. }) if added.len() == 1)
    );
}
