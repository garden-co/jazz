//! Subscription tests grouped by the same lifecycle boundaries as the implementation.

use super::*;
use groove::storage::TestStorage;

mod authorization;
mod coverage;
mod materialization;
mod structured;

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
