//! Hosts receive bounded wake deadlines for ordinary one-shot reads.

use crate::common::{allow_all_policies, compile_schema};
use jazz::db::{
    Db, DbConfig, DbIdentity, FirstLoad, ReadOpts, ReadTier, RemoteLinkHint, SerializedReadResult,
    SyncMessage, TickScheduler, TickUrgency, Transport, TransportError, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use std::cell::{Cell, RefCell};
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

#[derive(Clone, Default)]
struct Gate {
    blocked: Rc<Cell<bool>>,
    sent: Rc<RefCell<Vec<u64>>>,
}
struct GatedTransport {
    inner: Box<dyn Transport>,
    gate: Gate,
}
impl Transport for GatedTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if let SyncMessage::Fetch { id, .. } = &message {
            if self.gate.blocked.get() {
                return Err(TransportError::Backpressure);
            }
            self.gate.sent.borrow_mut().push(*id);
        }
        self.inner.send(message)
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inner.try_recv()
    }
}
fn fixture() -> (Db, Db, Gate) {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let open = |node, author| {
        block_on(Db::open(DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([node; 16]),
                author,
            },
        )))
        .unwrap()
    };
    let alice = open(1, AuthorSubject::for_test_bytes([1; 16]));
    let bob = open(2, AuthorSubject::SYSTEM);
    bob.set_authority();
    block_on(bob.insert(
        "notes",
        BTreeMap::from([("body".into(), Value::String("hello".into()))]),
        Default::default(),
    ))
    .unwrap();
    let gate = Gate::default();
    reconnect(&alice, &bob, &gate);
    for _ in 0..3 {
        tick(&alice, &bob);
    }
    gate.blocked.set(true);
    (alice, bob, gate)
}
fn reconnect(alice: &Db, bob: &Db, gate: &Gate) {
    let (up, down) = duplex();
    block_on(alice.connect_upstream(Box::new(GatedTransport {
        inner: up,
        gate: gate.clone(),
    })));
    bob.accept_subscriber(down, AuthorSubject::for_test_bytes([1; 16]));
    alice.set_remote_link_hint(RemoteLinkHint::Live);
}
fn poll<T>(future: Pin<&mut impl Future<Output = T>>) -> Poll<T> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}
fn tick(alice: &Db, bob: &Db) {
    block_on(alice.tick()).unwrap();
    block_on(bob.tick()).unwrap();
}
fn finish<T>(alice: &Db, bob: &Db, mut future: Pin<&mut impl Future<Output = T>>) -> T {
    for _ in 0..32 {
        if let Poll::Ready(value) = poll(future.as_mut()) {
            return value;
        }
        tick(alice, bob);
    }
    panic!("read did not settle");
}
fn assert_rows(result: SerializedReadResult) {
    let SerializedReadResult::Rows(rows) = result else {
        panic!("ordinary read")
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].cell("body"), Some(Value::String("hello".into())));
}

#[derive(Default)]
struct Scheduler {
    immediate: Cell<usize>,
    after: RefCell<Vec<u64>>,
    inspect: RefCell<Option<Box<dyn Fn()>>>,
}
impl Scheduler {
    fn clear(&self) {
        self.immediate.set(0);
        self.after.borrow_mut().clear();
    }
    fn earliest(&self) -> u64 {
        *self
            .after
            .borrow()
            .last()
            .expect("a deadline should be armed")
    }
}
impl TickScheduler for Scheduler {
    fn schedule_tick(&self, urgency: TickUrgency) {
        assert_eq!(urgency, TickUrgency::Immediate);
        self.immediate.set(self.immediate.get() + 1);
        if let Some(inspect) = self.inspect.borrow().as_ref() {
            inspect();
        }
    }
    fn schedule_tick_after(&self, delay_ms: u64) {
        assert!(delay_ms > 0, "deadline must round up to a later timer turn");
        self.after.borrow_mut().push(delay_ms);
        if let Some(inspect) = self.inspect.borrow().as_ref() {
            inspect();
        }
    }
}
fn waiting(timeout_ms: u64) -> ReadOpts {
    ReadOpts {
        tier: ReadTier::LocalFirst,
        first_load: FirstLoad::WaitForRemote { timeout_ms },
        ..Default::default()
    }
}

/// Alice's waiting read tells her host to send work now and wake at the
/// first-load deadline. Bob's answer releases that deadline even while Alice
/// retains the completed future. The host callback records the public contract.
#[test]
fn ordinary_read_schedules_progress_and_releases_completed_deadline() {
    let (alice, bob, gate) = fixture();
    let scheduler = Rc::new(Scheduler::default());
    alice.set_tick_scheduler(Some(scheduler.clone()));
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let mut read = Box::pin(alice.all_serialized_query(&query, waiting(600_000), None, None, None));
    assert!(poll(read.as_mut()).is_pending());
    assert_eq!(scheduler.immediate.get(), 1);
    assert!((550_000..=600_000).contains(&scheduler.earliest()));
    gate.blocked.set(false);
    assert_rows(finish(&alice, &bob, read.as_mut()).unwrap());
    scheduler.clear();
    block_on(alice.tick()).unwrap();
    assert!(
        scheduler.after.borrow().is_empty(),
        "completed read kept re-arming timers"
    );
    assert_eq!(scheduler.immediate.get(), 0);
}

/// Alice cancels her earliest read while another awaits Bob. The host's next
/// drive turn re-arms the remaining deadline; dropping both leaves no live timer.
#[test]
fn cancelling_earliest_read_rearms_the_surviving_deadline() {
    let (alice, _, _) = fixture();
    let scheduler = Rc::new(Scheduler::default());
    alice.set_tick_scheduler(Some(scheduler.clone()));
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let mut first =
        Box::pin(alice.all_serialized_query(&query, waiting(600_000), None, None, None));
    let mut second =
        Box::pin(alice.all_serialized_query(&query, waiting(1_200_000), None, None, None));
    assert!(poll(first.as_mut()).is_pending());
    assert!(poll(second.as_mut()).is_pending());
    assert!((550_000..=600_000).contains(&scheduler.earliest()));
    drop(first);
    scheduler.clear();
    block_on(alice.tick()).unwrap();
    assert!((1_150_000..=1_200_000).contains(&scheduler.earliest()));
    drop(second);
    scheduler.clear();
    block_on(alice.tick()).unwrap();
    assert!(scheduler.after.borrow().is_empty());
}

/// Alice's host installs or replaces its scheduler after a read has begun.
/// Each new hook receives the outstanding deadline for Bob's pending answer.
#[test]
fn scheduler_installation_and_replacement_arm_live_deadlines() {
    let (alice, _, _) = fixture();
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let mut read = Box::pin(alice.all_serialized_query(&query, waiting(600_000), None, None, None));
    assert!(poll(read.as_mut()).is_pending());
    let first = Rc::new(Scheduler::default());
    alice.set_tick_scheduler(Some(first.clone()));
    assert!((550_000..=600_000).contains(&first.earliest()));
    first.clear();
    let second = Rc::new(Scheduler::default());
    alice.set_tick_scheduler(Some(second.clone()));
    assert!((550_000..=600_000).contains(&second.earliest()));
    assert!(first.after.borrow().is_empty());
    alice.set_tick_scheduler(None);
    second.clear();
    block_on(alice.tick()).unwrap();
    assert!(second.after.borrow().is_empty());
}

/// Alice's deadline expires before her host installs a scheduler. Installing
/// the hook must request a drive turn so her read can fall back without another
/// event from Bob. Replacing the hook must preserve that pending wake too.
#[test]
fn scheduler_installation_wakes_reads_whose_deadline_already_expired() {
    let (alice, _, _) = fixture();
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let mut read = Box::pin(alice.all_serialized_query(&query, waiting(100), None, None, None));
    assert!(poll(read.as_mut()).is_pending());
    std::thread::sleep(std::time::Duration::from_millis(120));

    let first = Rc::new(Scheduler::default());
    alice.set_tick_scheduler(Some(first.clone()));
    assert_eq!(first.immediate.get(), 1);
    let replacement = Rc::new(Scheduler::default());
    alice.set_tick_scheduler(Some(replacement.clone()));
    assert_eq!(replacement.immediate.get(), 1);

    block_on(alice.tick()).unwrap();
    assert!(matches!(poll(read.as_mut()), Poll::Ready(Ok(_))));
    let completed = Rc::new(Scheduler::default());
    alice.set_tick_scheduler(Some(completed.clone()));
    assert_eq!(completed.immediate.get(), 0);
    assert!(completed.after.borrow().is_empty());
}

/// Alice waits while her host attempts to reconnect to Bob. Its five-second
/// attempt window wakes earlier than the read's longer first-load budget.
/// Reporting failure schedules immediate progress so the read can fall back.
#[test]
fn attempt_window_and_failed_link_request_host_progress() {
    let (alice, _, _) = fixture();
    alice.set_remote_link_hint(RemoteLinkHint::Attempting);
    let scheduler = Rc::new(Scheduler::default());
    alice.set_tick_scheduler(Some(scheduler.clone()));
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let mut read = Box::pin(alice.all_serialized_query(&query, waiting(600_000), None, None, None));
    assert!(poll(read.as_mut()).is_pending());
    assert!((1..=5_000).contains(&scheduler.earliest()));
    scheduler.clear();
    alice.set_remote_link_hint(RemoteLinkHint::Failed);
    assert_eq!(scheduler.immediate.get(), 1);
    let Poll::Ready(Ok(SerializedReadResult::Rows(rows))) = poll(read.as_mut()) else {
        panic!("failed link should release local fallback")
    };
    assert!(rows.is_empty());
    scheduler.clear();
    block_on(alice.tick()).unwrap();
    assert!(scheduler.after.borrow().is_empty());
}

/// Alice's host callback may inspect her database while scheduling Bob's
/// pending work. Re-entering this public diagnostic exposes a held store borrow.
#[test]
fn host_callbacks_run_after_database_borrows_are_released() {
    let (alice, bob, gate) = fixture();
    let alice = Rc::new(alice);
    let scheduler = Rc::new(Scheduler::default());
    let inspect_db = alice.clone();
    *scheduler.inspect.borrow_mut() = Some(Box::new(move || {
        let _ = inspect_db.sync_debug();
    }));
    alice.set_tick_scheduler(Some(scheduler.clone()));
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let mut read = Box::pin(alice.all_serialized_query(&query, waiting(600_000), None, None, None));
    assert!(poll(read.as_mut()).is_pending());
    assert!(!scheduler.after.borrow().is_empty());
    gate.blocked.set(false);
    alice.set_remote_link_hint(RemoteLinkHint::Live);
    assert_rows(finish(&alice, &bob, read.as_mut()).unwrap());
    alice.set_tick_scheduler(None);
    scheduler.inspect.borrow_mut().take();
}
