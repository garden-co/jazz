//! Internal executor-contract tests: public server tests cannot observe a
//! lost wake independently of their own polling loop, or hold the node mutex
//! at the precise coverage boundary. Schemas/queries still use public builders.
use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct ReadWakes(AtomicUsize);
impl futures::task::ArcWake for ReadWakes {
    fn wake_by_ref(this: &Arc<Self>) {
        this.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn pending_read<'a>(
    alice: &'a Db<RocksDbStorage>,
    title: &'a str,
    expired: Rc<Cell<bool>>,
) -> Pin<Box<dyn Future<Output = Result<SerializedReadResult, Error>> + 'a>> {
    Box::pin(async move {
        let query =
            postcard::to_allocvec(&alice.table("todos").filter(eq(col("title"), lit(title))))
                .unwrap();
        alice
            .all_serialized_query(
                &query,
                ReadOpts {
                    tier: DurabilityTier::Global,
                    ..ReadOpts::default()
                },
                None,
                None,
                None,
                true,
                || expired.get(),
                |attachment| alice.detach_query(attachment),
            )
            .await
    })
}

/// Alice's concurrent reads sleep until Bob returns their accepted receipts.
/// Neither read is polled again unless its own registered waker fires.
/// alice: read A + read B -> sleep -> bob: receipts -> alice: both resolve
#[test]
fn serialized_coverage_wakes_concurrent_reads_without_polling() {
    let alice = open_db(0xa1, AuthorSubject::SYSTEM, &schema());
    let bob = open_core(0xa2, AuthorSubject::SYSTEM, &schema());
    let (left, right) = duplex();
    let _upstream = block_on(alice.connect_upstream(left));
    let _subscriber = bob.accept_subscriber(right, AuthorSubject::SYSTEM);
    let mut reads = [
        pending_read(&alice, "first", Rc::new(Cell::new(false))),
        pending_read(&alice, "second", Rc::new(Cell::new(false))),
    ];
    let wakes = [
        Arc::new(ReadWakes::default()),
        Arc::new(ReadWakes::default()),
    ];
    let wakers = wakes
        .each_ref()
        .map(|wake| futures::task::waker(Arc::clone(wake)));
    let mut observed = [0, 0];
    let mut done = [false, false];
    for index in 0..2 {
        assert!(
            reads[index]
                .as_mut()
                .poll(&mut Context::from_waker(&wakers[index]))
                .is_pending()
        );
    }
    for _ in 0..50 {
        block_on(alice.tick()).unwrap();
        bob.tick().unwrap();
        block_on(alice.tick()).unwrap();
        for index in 0..2 {
            if done[index] || wakes[index].0.load(Ordering::SeqCst) == observed[index] {
                continue;
            }
            observed[index] = wakes[index].0.load(Ordering::SeqCst);
            if let Poll::Ready(result) = reads[index]
                .as_mut()
                .poll(&mut Context::from_waker(&wakers[index]))
            {
                assert!(
                    matches!(result.unwrap(), SerializedReadResult::Rows(rows) if rows.is_empty())
                );
                done[index] = true;
            }
        }
        if done.iter().all(|done| *done) {
            break;
        }
    }
    assert_eq!(
        done,
        [true, true],
        "receipt delivery must wake each usage-site read"
    );
    assert_eq!(alice.query_coverage_attachment_counts_for_test(), (0, 0));
}

/// Bob never responds. Alice cancels one read and expires another while her
/// owner is busy. Both paths detach coverage and release their stored wakers.
/// alice: wait -> [cancel | deadline with owner held] -> no live attachments
#[test]
fn sleeping_coverage_cancellation_and_deadline_release_waiters() {
    let alice = open_db(0xa3, AuthorSubject::SYSTEM, &schema());
    let (transport, _bob) = duplex();
    let _upstream = block_on(alice.connect_upstream(transport));
    let expired = Rc::new(Cell::new(false));
    let wakes = Arc::new(ReadWakes::default());
    let waker = futures::task::waker(Arc::clone(&wakes));
    let mut cx = Context::from_waker(&waker);
    let mut cancelled = pending_read(&alice, "cancel", Rc::clone(&expired));
    assert!(cancelled.as_mut().poll(&mut cx).is_pending());
    drop(cancelled);
    block_on(alice.tick()).unwrap();
    assert_eq!(alice.query_coverage_attachment_counts_for_test(), (0, 0));

    let mut timed_out = pending_read(&alice, "expire", Rc::clone(&expired));
    assert!(timed_out.as_mut().poll(&mut cx).is_pending());
    let owner = block_on(alice.node.node.lock());
    expired.set(true);
    match timed_out.as_mut().poll(&mut cx) {
        Poll::Ready(Err(error)) => assert_eq!(error.code, ErrorCode::NotObserved),
        _ => panic!("deadline must remain observable while coverage cannot acquire the owner"),
    }
    drop(owner);
    drop(timed_out);
    block_on(alice.tick()).unwrap();
    assert_eq!(alice.query_coverage_attachment_counts_for_test(), (0, 0));
    assert_eq!(
        Arc::strong_count(&wakes),
        2,
        "no cancelled read retains the caller's waker"
    );
}

/// Alice already received Bob's receipt, but another operation owns her node.
/// Releasing that owner must wake the read without another network message.
/// bob: receipt -> alice: owner held -> read sleeps -> unlock -> ready
#[test]
fn covered_read_wakes_when_busy_owner_is_released() {
    let alice = open_db(0xa4, AuthorSubject::SYSTEM, &schema());
    let bob = open_core(0xa5, AuthorSubject::SYSTEM, &schema());
    let (left, right) = duplex();
    let _upstream = block_on(alice.connect_upstream(left));
    let _subscriber = bob.accept_subscriber(right, AuthorSubject::SYSTEM);
    let prepared = block_on(alice.prepare_query_async(&alice.table("todos"))).unwrap();
    let attachment = block_on(alice.attach_query_with_opts_async(
        &prepared,
        ReadOpts {
            tier: DurabilityTier::Global,
            ..ReadOpts::default()
        },
        None,
        None,
    ))
    .unwrap();
    for _ in 0..20 {
        block_on(alice.tick()).unwrap();
        bob.tick().unwrap();
        block_on(alice.tick()).unwrap();
        if alice.query_attachment_is_covered(&attachment) {
            break;
        }
    }
    assert!(alice.query_attachment_is_covered(&attachment));
    let wakes = Arc::new(ReadWakes::default());
    let waker = futures::task::waker(Arc::clone(&wakes));
    let mut cx = Context::from_waker(&waker);
    let owner = block_on(alice.node.node.lock());
    let mut covered = Box::pin(alice.await_query_attachment_coverage(&attachment));
    assert!(covered.as_mut().poll(&mut cx).is_pending());
    assert_eq!(wakes.0.load(Ordering::SeqCst), 0);
    drop(owner);
    assert!(wakes.0.load(Ordering::SeqCst) > 0);
    assert!(covered.as_mut().poll(&mut cx).is_ready());
    drop(covered);
    alice.detach_query(attachment);
}
