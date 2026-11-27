use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct InspectingAllocator;

#[global_allocator]
static ALLOCATOR: InspectingAllocator = InspectingAllocator;

thread_local! {
    static WATCHED: Cell<Option<(usize, usize)>> = const { Cell::new(None) };
    static WIPED: Cell<Option<bool>> = const { Cell::new(None) };
}

// SAFETY: The allocator forwards the original arguments to System. It reads
// only the watched string's initialized bytes before freeing its allocation.
unsafe impl GlobalAlloc for InspectingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let _ = WATCHED.try_with(|watched| {
            if let Some((address, len)) = watched.get()
                && address == ptr.addr()
            {
                watched.set(None);
                // SAFETY: The caller supplied a live allocation. The tracked
                // prefix was initialized when the string's ownership moved.
                let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
                let _ =
                    WIPED.try_with(|wiped| wiped.set(Some(bytes.iter().all(|byte| *byte == 0))));
            }
        });
        unsafe { System.dealloc(ptr, layout) }
    }
}

/// Inspect the input's allocation at deallocation, before the allocator can
/// overwrite or reuse it. The operation consumes the string without resizing it.
pub fn assert_zeroized<T>(text: String, consume: impl FnOnce(String) -> T) -> T {
    assert!(!text.is_empty() && text.as_bytes().iter().any(|byte| *byte != 0));
    WIPED.set(None);
    WATCHED.set(Some((text.as_ptr().addr(), text.len())));
    let result = consume(text);
    WATCHED.set(None);
    assert_eq!(
        WIPED.get(),
        Some(true),
        "the owned secret must be wiped before deallocation"
    );
    result
}
