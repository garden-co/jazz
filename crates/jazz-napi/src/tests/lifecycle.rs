//! White-box tests are needed here to inject bounded cleanup errors and inspect
//! weak ownership. Public Node tests separately cover real upload and close IO.
use super::*;
use crate::tests::host_fixture::{fixture, fixture_schema};

/// Alice's primary close failure retains its code; cleanup failure is secondary.
#[test]
fn cleanup_failure_is_secondary_to_close_failure() {
    let mut failures = CleanupFailures::default();
    failures.record(4, "cleanup".into());
    let error = with_cleanup(
        HostError::Engine(CoreError::new(
            jazz::db::ErrorCode::WriteRejected,
            "injected close failure",
        )),
        &failures,
    );
    let HostError::Engine(error) = error else {
        panic!("lost primary error")
    };
    assert_eq!(error.code, jazz::db::ErrorCode::WriteRejected);
    assert_eq!(
        error.message,
        "injected close failure; secondary: streaming cleanup failed at sequence 4: cleanup"
    );
    assert_eq!(
        failures.message().as_deref(),
        Some("streaming cleanup failed at sequence 4: cleanup")
    );
}

/// Alice's diagnostics retain the earliest two failures and bound the rest.
#[test]
fn streaming_cleanup_errors_keep_first_sequence_and_bound_secondaries() {
    let mut failures = CleanupFailures::default();
    for (seq, text) in [(4, "first"), (9, "second"), (12, "third")] {
        failures.record(seq, text.into());
    }
    assert_eq!(
        failures.message().as_deref(),
        Some(
            "streaming cleanup failed at sequence 4: first; secondary: second; 1 additional cleanup failure(s)"
        )
    );
    assert_eq!(failures.entries.len(), 2);
}

/// Alice's view closes consume only their own diagnostics; the owner aggregates them once.
#[test]
fn cleanup_failures_are_consumed_by_their_view_close() {
    let db = fixture();
    let first = db.register_schema(fixture_schema().into()).unwrap();
    let second = db.register_schema(fixture_schema().into()).unwrap();
    let inner = db.open_inner().unwrap();
    let a = first.open_inner().unwrap();
    let b = second.open_inner().unwrap();
    a.view.failures.borrow_mut().record(4, "view A".into());
    b.view.failures.borrow_mut().record(9, "view B".into());
    assert_eq!(
        block_on(close(a.clone(), false)).unwrap_err().to_string(),
        "streaming cleanup failed at sequence 4: view A"
    );
    assert!(a.view.failures.borrow().message().is_none());
    assert!(b.view.failures.borrow().message().is_some());
    assert_eq!(
        block_on(close(b.clone(), false)).unwrap_err().to_string(),
        "streaming cleanup failed at sequence 9: view B"
    );
    assert_eq!(
        inner.host.cleanup_failures.borrow().message().as_deref(),
        Some("streaming cleanup failed at sequence 4: view A; secondary: view B")
    );
    assert!(block_on(close(inner.clone(), true)).is_err());
    assert!(inner.host.cleanup_failures.borrow().message().is_none());
}

/// Alice's per-view failures merge by global sequence, independent of close order.
#[test]
fn cleanup_failure_merge_preserves_global_sequence_order() {
    let mut owner = CleanupFailures::default();
    let mut view = CleanupFailures::default();
    owner.record(2, "second".into());
    owner.record(3, "third".into());
    view.record(1, "first".into());
    owner.merge(view);
    assert_eq!(
        owner.message().as_deref(),
        Some(
            "streaming cleanup failed at sequence 1: first; secondary: second; 1 additional cleanup failure(s)"
        )
    );
}

/// Dropping Alice's view leaves no per-view state and preserves its failure at the owner.
#[test]
fn orphaned_view_failures_fold_into_bounded_owner_accounting() {
    let db = fixture();
    let inner = db.open_inner().unwrap();
    let view = View::new(&inner.host);
    let id = view.id;
    view.failures.borrow_mut().record(4, "orphaned".into());
    drop(view);
    assert!(!inner.host.views.borrow().contains_key(&id));
    assert_eq!(
        inner.host.cleanup_failures.borrow().message().as_deref(),
        Some("streaming cleanup failed at sequence 4: orphaned")
    );
}

/// Alice's cancelled close leaves no strong cycle and releases its orphaned view.
#[test]
fn orphaned_closing_view_marker_is_reaped_after_completion() {
    let db = fixture();
    let inner = db.open_inner().unwrap();
    let facade = db.register_schema(fixture_schema().into()).unwrap();
    let other = facade.open_inner().unwrap();
    let id = other.view.id;
    let weak = Rc::downgrade(&other.view);
    let guard = block_on(inner.host.cleanup_drain.lock());
    let mut closing = close(other, false);
    assert!(
        closing
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(facade);
    drop(closing);
    drop(guard);
    assert!(weak.upgrade().is_none());
    assert!(!inner.host.views.borrow().contains_key(&id));
}
