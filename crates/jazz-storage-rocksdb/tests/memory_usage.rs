//! RocksDB's process-wide memory report, in its own test process so that no
//! other test's stores are open while it measures.

use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};

use groove::storage::OrderedKvStorage;
use jazz_storage_rocksdb::{RocksDbStorage, process_memory_usage};

/// Memtable bytes written to an open store show up in the report, and leave
/// it with the store. Collecting the report concurrently never keeps a
/// closed store's database open, so the same path can be reopened at once.
#[test]
fn reports_open_stores_and_releases_closed_ones() {
    let idle = process_memory_usage().expect("RocksDB reports memory");
    assert_eq!(idle.memtable_bytes, 0, "no store is open yet: {idle:?}");

    let dir = tempfile::tempdir().unwrap();
    let storage = RocksDbStorage::open(dir.path(), &["records"]).unwrap();
    ready(storage.set("records".into(), b"key".to_vec(), vec![7; 256 * 1024])).unwrap();
    let open = process_memory_usage().expect("RocksDB reports memory");
    assert!(
        open.memtable_bytes >= 256 * 1024,
        "the write is in a memtable: {open:?}"
    );
    drop(storage);
    let closed = process_memory_usage().expect("RocksDB reports memory");
    assert_eq!(
        closed.memtable_bytes, 0,
        "the closed store left: {closed:?}"
    );

    let stop = Arc::new(AtomicBool::new(false));
    let reporter = {
        let stop = stop.clone();
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                process_memory_usage().expect("RocksDB reports memory");
            }
        })
    };
    for _ in 0..50 {
        let storage = RocksDbStorage::open(dir.path(), &["records"])
            .expect("a store closed while reporting can be reopened");
        drop(storage);
    }
    stop.store(true, Ordering::Relaxed);
    reporter.join().unwrap();
}

fn ready<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("RocksDB storage operation unexpectedly suspended"),
    }
}
