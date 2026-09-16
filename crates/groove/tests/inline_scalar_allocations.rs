//! Borrowing inline bytes/string payloads must not allocate owned copies.
use groove::large_values::{
    LargeValueKind, StoredScalar, encode_stored_scalar, inline_scalar_bytes,
};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::hint::black_box;

struct CountingAllocator;
thread_local! {
    static ACTIVE: Cell<bool> = const { Cell::new(false) };
    static ALLOCATIONS: Cell<usize> = const { Cell::new(0) };
}
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;
fn record_allocation() {
    if ACTIVE.try_with(Cell::get).unwrap_or(false) {
        let _ = ALLOCATIONS.try_with(|count| count.set(count.get() + 1));
    }
}
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record_allocation();
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record_allocation();
        unsafe { System.realloc(ptr, layout, size) }
    }
}
#[test]
fn inline_scalar_borrowing_does_not_allocate() {
    for kind in [LargeValueKind::Bytes, LargeValueKind::String] {
        for size in [0, 32, 65536] {
            let encoded =
                encode_stored_scalar(kind, &StoredScalar::Primitive(vec![b'a'; size])).unwrap();
            inline_scalar_bytes(kind, &encoded).unwrap();
            ALLOCATIONS.with(|count| count.set(0));
            ACTIVE.with(|active| active.set(true));
            let result = black_box(inline_scalar_bytes(black_box(kind), black_box(&encoded)));
            ACTIVE.with(|active| active.set(false));
            let borrowed = result.unwrap();
            assert_eq!(borrowed.len(), size);
            assert_eq!(borrowed.as_ptr(), encoded[1..].as_ptr());
            assert_eq!(ALLOCATIONS.with(Cell::get), 0, "kind={kind:?}, size={size}");
        }
    }
}
