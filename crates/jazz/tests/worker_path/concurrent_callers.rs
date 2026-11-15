//! Finding 1 (#5342 review): `Store` holds its `RefCell` borrow across SQL
//! awaits, so every entry point must queue behind the operation lease while
//! another operation is suspended in the worker, instead of panicking with
//! "RefCell already borrowed".

use std::collections::BTreeMap;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::task::{Context, Waker};

use jazz::db::{Db, DbConfig, DbIdentity, InsertOptions, block_on};
use jazz::host::StoreFile;
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use jazz_db::test_worker::SuspendingWorker;

use crate::common::{allow_all_policies, compile_schema};

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("title", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn config() -> DbConfig {
    DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([0x5e; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}

fn title(text: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("title".to_owned(), Value::String(text.to_owned()))])
}

fn open() -> (SuspendingWorker, Db) {
    let worker = SuspendingWorker::memory();
    let file = block_on(StoreFile::worker(worker.clone())).expect("worker admitted");
    let db = block_on(Db::open_file(config(), file)).expect("Db opens over a worker");
    (worker, db)
}

fn panic_message(panic: Box<dyn std::any::Any + Send>) -> String {
    panic
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "<non-string panic>".to_owned())
}

/// Alice's insert is suspended in the worker (its first statement's reply is
/// held) while she owns the store. Bob calls `probe` meanwhile. Returns a
/// failure description, or `None` if Bob queued and then completed.
///
/// alice: insert -> SQL reply held ...................... release -> done
/// bob:              probe -> Pending (queued) ............. -> completes
type Probe<'a> = Pin<Box<dyn Future<Output = ()> + 'a>>;

/// An async entry point call, its result discarded.
macro_rules! probe {
    (|$db:ident| $call:expr) => {
        higher_ranked(|$db| {
            Box::pin(async move {
                let _ = $call.await;
            })
        })
    };
}

fn higher_ranked<F: for<'a> FnOnce(&'a Db) -> Probe<'a>>(probe: F) -> F {
    probe
}

fn queues_behind_suspended_insert(
    name: &str,
    probe: impl for<'a> FnOnce(&'a Db) -> Probe<'a>,
) -> Option<String> {
    let (worker, db) = open();
    let mut cx = Context::from_waker(Waker::noop());
    worker.hold_next();
    let mut alice = Box::pin(db.insert("notes", title("alice"), InsertOptions::default()));
    assert!(alice.as_mut().poll(&mut cx).is_pending());
    assert!(
        worker.holding(),
        "Alice's insert is suspended in the worker"
    );

    let polled = catch_unwind(AssertUnwindSafe(|| {
        let mut bob = probe(&db);
        let first = bob.as_mut().poll(&mut cx);
        (bob, first.is_pending())
    }));
    let failure = match polled {
        Err(panic) => Some(format!("{name}: panicked: {}", panic_message(panic))),
        Ok((_, false)) => Some(format!(
            "{name}: completed while Alice's operation owned the store"
        )),
        Ok((bob, true)) => {
            worker.release();
            block_on(alice.as_mut()).expect("Alice's insert completes");
            match catch_unwind(AssertUnwindSafe(|| block_on(bob))) {
                Ok(_) => None,
                Err(panic) => Some(format!(
                    "{name}: panicked after queueing: {}",
                    panic_message(panic)
                )),
            }
        }
    };
    worker.release();
    if failure.is_some() {
        let _ = catch_unwind(AssertUnwindSafe(|| block_on(alice)));
    }
    failure
}

/// The synchronous metadata read `probe` must not panic while Alice's insert
/// is suspended in the worker.
fn reads_beside_suspended_insert<T>(name: &str, probe: impl FnOnce(&Db) -> T) -> Option<String> {
    let (worker, db) = open();
    let mut cx = Context::from_waker(Waker::noop());
    worker.hold_next();
    let mut alice = Box::pin(db.insert("notes", title("alice"), InsertOptions::default()));
    assert!(alice.as_mut().poll(&mut cx).is_pending());
    assert!(
        worker.holding(),
        "Alice's insert is suspended in the worker"
    );
    let failure = catch_unwind(AssertUnwindSafe(|| {
        let _ = probe(&db);
    }))
    .err()
    .map(|panic| format!("{name}: panicked: {}", panic_message(panic)));
    worker.release();
    block_on(alice).expect("Alice's insert completes");
    failure
}

fn assert_no_failures(failures: Vec<Option<String>>) {
    let failures: Vec<String> = failures.into_iter().flatten().collect();
    assert!(
        failures.is_empty(),
        "{} entry points did not wait for the suspended operation:\n  {}",
        failures.len(),
        failures.join("\n  ")
    );
}

/// Control: the entry points that take the operation lease already queue
/// behind a suspended worker operation. This passes on 7ba9076e1 and shows the
/// harness suspends a real `Db` operation.
#[test]
fn leased_entry_points_queue_behind_a_suspended_worker_operation() {
    assert_no_failures(vec![
        queues_behind_suspended_insert(
            "insert",
            probe!(|db| db.insert("notes", title("bob"), InsertOptions::default())),
        ),
        queues_behind_suspended_insert("catalogue", probe!(|db| db.catalogue())),
        queues_behind_suspended_insert(
            "rejected_transactions",
            probe!(|db| db.rejected_transactions()),
        ),
    ]);
}

/// Every async entry point queues behind a suspended worker operation.
#[test]
fn async_entry_points_queue_behind_a_suspended_worker_operation() {
    assert_no_failures(vec![
        queues_behind_suspended_insert("tick", probe!(|db| db.tick())),
        queues_behind_suspended_insert("close", probe!(|db| db.close())),
        queues_behind_suspended_insert("set_authority", probe!(|db| db.set_authority())),
        queues_behind_suspended_insert("table_identity", probe!(|db| db.table_identity("notes"))),
        queues_behind_suspended_insert("take_chunk_io", probe!(|db| db.take_chunk_io())),
        queues_behind_suspended_insert(
            "wait_for_pending_writes",
            probe!(|db| db.wait_for_pending_writes(DurabilityTier::Local)),
        ),
        queues_behind_suspended_insert("capture_snapshot", probe!(|db| db.capture_snapshot())),
    ]);
}

/// Synchronous metadata reads cannot queue; they must not panic while a
/// worker operation holds the store.
#[test]
fn sync_metadata_reads_do_not_panic_beside_a_suspended_worker_operation() {
    assert_no_failures(vec![
        reads_beside_suspended_insert("prepare_query", |db| {
            db.prepare_query(&Query::from("notes")).map(|_| ())
        }),
        reads_beside_suspended_insert("current_schema", |db| db.current_schema()),
        reads_beside_suspended_insert("next_row_id", |db| db.next_row_id()),
        reads_beside_suspended_insert("seq_high_water", |db| db.seq_high_water()),
        reads_beside_suspended_insert("is_authority", |db| db.is_authority()),
        reads_beside_suspended_insert("take_upstream_rejections", |db| {
            db.take_upstream_rejections().len()
        }),
        reads_beside_suspended_insert("fanout_stats", |db| {
            let _ = db.fanout_stats();
        }),
        reads_beside_suspended_insert("take_slow_queries", |db| db.take_slow_queries().1),
    ]);
}
