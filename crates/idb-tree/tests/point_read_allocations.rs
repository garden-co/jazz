//! Public point reads must copy their result, not resident sibling payloads.
//! Allocation instrumentation is needed because returned rows alone cannot
//! distinguish a borrowed lookup from cloning the complete resident leaf.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
};

use futures::{FutureExt, executor::block_on};
use idb_tree::{IdbTree, MemoryPageStore, Options};

thread_local! {
    static COUNT_PAYLOADS: Cell<bool> = const { Cell::new(false) };
    static PAYLOAD_COPIES: Cell<usize> = const { Cell::new(0) };
}

struct CountingAllocator;
// Only this integration-test binary uses the allocator. Counts are thread-local
// and enabled around a warm public read, excluding setup and assertion work.
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if layout.size() == 1000 && COUNT_PAYLOADS.try_with(Cell::get).unwrap_or(false) {
            let _ = PAYLOAD_COPIES.try_with(|count| count.set(count.get() + 1));
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn payload_copies<T>(read: impl FnOnce() -> T) -> (T, usize) {
    PAYLOAD_COPIES.set(0);
    COUNT_PAYLOADS.set(true);
    let result = read();
    COUNT_PAYLOADS.set(false);
    (result, PAYLOAD_COPIES.get())
}

#[test]
fn resident_point_reads_copy_only_the_requested_payload() {
    block_on(async {
        let store = MemoryPageStore::default();
        let tree = IdbTree::open(store.clone(), Options::default())
            .await
            .unwrap();
        for key in 0..10_u8 {
            tree.put(vec![key], vec![key; 1000]).await.unwrap();
        }
        tree.put(vec![20], vec![20; 40_000]).await.unwrap();
        tree.flush().await.unwrap();
        drop(tree);
        let tree = IdbTree::open(store, Options::default()).await.unwrap();
        assert_eq!(tree.get(&[3]).await.unwrap(), Some(vec![3; 1000]));
        let (result, copies) =
            payload_copies(|| tree.get(&[3]).now_or_never().expect("resident read"));
        assert_eq!(result.unwrap(), Some(vec![3; 1000]));
        assert_eq!(copies, 1, "only the returned inline payload may be copied");
        let (result, copies) =
            payload_copies(|| tree.get(&[11]).now_or_never().expect("resident miss"));
        assert_eq!(result.unwrap(), None);
        assert_eq!(copies, 0, "missing keys must not copy sibling payloads");
        // Cold overflow hydration and subsequent cached reads retain exact bytes.
        assert_eq!(tree.get(&[20]).await.unwrap(), Some(vec![20; 40_000]));
        assert_eq!(tree.get(&[20]).await.unwrap(), Some(vec![20; 40_000]));
        tree.put(vec![3], vec![33; 1000]).await.unwrap();
        tree.delete(&[4]).await.unwrap();
        assert_eq!(tree.get(&[3]).await.unwrap(), Some(vec![33; 1000]));
        assert_eq!(tree.get(&[4]).await.unwrap(), None);
        assert_eq!(tree.get(&[5]).await.unwrap(), Some(vec![5; 1000]));
        assert_eq!(tree.get(&[20]).await.unwrap(), Some(vec![20; 40_000]));
    });
}
