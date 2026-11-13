//! The process-wide SQLite memory budget.
//!
//! SQLite recycles page-cache memory to stay under its soft heap limit, so
//! lowering the limit trades storage reads for memory continuously, and a
//! store never fails a request for lack of cache.

/// Set the budget; `None` is unbounded.
#[cfg(feature = "sqlite")]
pub fn set_budget(bytes: Option<usize>) {
    let limit = bytes.map_or(0, |bytes| i64::try_from(bytes).unwrap_or(i64::MAX));
    // SAFETY: a plain setter on SQLite's global allocator state; it is
    // thread-safe in SQLite's default (serialized) threading mode.
    unsafe {
        rusqlite::ffi::sqlite3_soft_heap_limit64(limit);
    }
}

/// Memory SQLite currently holds.
#[cfg(feature = "sqlite")]
pub fn resident_bytes() -> usize {
    // SAFETY: reads a counter SQLite maintains under its own mutex.
    let used = unsafe { rusqlite::ffi::sqlite3_memory_used() };
    usize::try_from(used).unwrap_or(0)
}

/// The current budget, or `None` when unbounded.
#[cfg(feature = "sqlite")]
pub fn budget() -> Option<usize> {
    // SAFETY: a negative argument only reads the current limit.
    let limit = unsafe { rusqlite::ffi::sqlite3_soft_heap_limit64(-1) };
    (limit > 0).then(|| usize::try_from(limit).unwrap_or(usize::MAX))
}

#[cfg(not(feature = "sqlite"))]
pub fn set_budget(bytes: Option<usize>) {
    if bytes.is_some() {
        tracing::warn!("Process-global SQLite cache budgets are unavailable with worker storage");
    }
}
#[cfg(not(feature = "sqlite"))]
pub fn resident_bytes() -> usize {
    0
}
#[cfg(not(feature = "sqlite"))]
pub fn budget() -> Option<usize> {
    None
}
