//! Memory tracking utilities for benchmarks.
//!
//! Provides a tracking allocator and utilities for measuring memory overhead.
//! Only used by memory_benchmark.rs - other benchmarks don't use these utilities.

#![allow(dead_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Global counters for allocation tracking.
static ALLOCATED: AtomicUsize = AtomicUsize::new(0);
static DEALLOCATED: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

/// A tracking allocator that wraps the system allocator.
pub struct TrackingAllocator;

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: We're wrapping the system allocator
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let size = layout.size();
            let prev = ALLOCATED.fetch_add(size, Ordering::Relaxed);
            let current = prev + size - DEALLOCATED.load(Ordering::Relaxed);

            // Update peak if current exceeds it
            let mut peak = PEAK.load(Ordering::Relaxed);
            while current > peak {
                match PEAK.compare_exchange_weak(
                    peak,
                    current,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(p) => peak = p,
                }
            }
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        DEALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
        // SAFETY: We're wrapping the system allocator
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: We're wrapping the system allocator
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            let old_size = layout.size();
            if new_size > old_size {
                let diff = new_size - old_size;
                let prev = ALLOCATED.fetch_add(diff, Ordering::Relaxed);
                let current = prev + diff - DEALLOCATED.load(Ordering::Relaxed);

                let mut peak = PEAK.load(Ordering::Relaxed);
                while current > peak {
                    match PEAK.compare_exchange_weak(
                        peak,
                        current,
                        Ordering::Relaxed,
                        Ordering::Relaxed,
                    ) {
                        Ok(_) => break,
                        Err(p) => peak = p,
                    }
                }
            } else {
                DEALLOCATED.fetch_add(old_size - new_size, Ordering::Relaxed);
            }
        }
        new_ptr
    }
}

/// Memory statistics snapshot.
#[derive(Debug, Clone, Copy)]
#[allow(dead_code)]
pub struct MemoryStats {
    /// Total bytes allocated since start/reset.
    pub allocated: usize,
    /// Total bytes deallocated since start/reset.
    pub deallocated: usize,
    /// Peak memory usage (high water mark).
    pub peak: usize,
}

#[allow(dead_code)]
impl MemoryStats {
    /// Current memory in use.
    pub fn current(&self) -> usize {
        self.allocated.saturating_sub(self.deallocated)
    }

    /// Format as human-readable string.
    pub fn format(&self) -> String {
        format!(
            "current: {}, peak: {}, total alloc: {}",
            format_bytes(self.current()),
            format_bytes(self.peak),
            format_bytes(self.allocated)
        )
    }

    /// Calculate memory multiple relative to data size.
    pub fn multiple_of(&self, data_bytes: usize) -> MemoryMultiple {
        MemoryMultiple {
            current: self.current() as f64 / data_bytes as f64,
            peak: self.peak as f64 / data_bytes as f64,
            data_bytes,
            stats: *self,
        }
    }
}

/// Memory usage expressed as a multiple of data size.
#[derive(Debug)]
#[allow(dead_code)]
pub struct MemoryMultiple {
    /// Current memory as multiple of data.
    pub current: f64,
    /// Peak memory as multiple of data.
    pub peak: f64,
    /// Raw data size in bytes.
    pub data_bytes: usize,
    /// Raw stats.
    pub stats: MemoryStats,
}

impl std::fmt::Display for MemoryMultiple {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Data: {} | Current: {} ({:.1}x) | Peak: {} ({:.1}x)",
            format_bytes(self.data_bytes),
            format_bytes(self.stats.current()),
            self.current,
            format_bytes(self.stats.peak),
            self.peak
        )
    }
}

/// Get current memory statistics.
pub fn get_stats() -> MemoryStats {
    MemoryStats {
        allocated: ALLOCATED.load(Ordering::Relaxed),
        deallocated: DEALLOCATED.load(Ordering::Relaxed),
        peak: PEAK.load(Ordering::Relaxed),
    }
}

/// Reset all counters to zero.
pub fn reset_stats() {
    ALLOCATED.store(0, Ordering::Relaxed);
    DEALLOCATED.store(0, Ordering::Relaxed);
    PEAK.store(0, Ordering::Relaxed);
}

/// Reset and return a baseline for delta measurements.
#[allow(dead_code)]
pub fn baseline() -> MemoryStats {
    // Don't reset - just capture current state for delta
    get_stats()
}

/// Get delta from a baseline.
#[allow(dead_code)]
pub fn delta_from(baseline: &MemoryStats) -> MemoryStats {
    let current = get_stats();
    MemoryStats {
        allocated: current.allocated.saturating_sub(baseline.allocated),
        deallocated: current.deallocated.saturating_sub(baseline.deallocated),
        peak: current.peak.saturating_sub(
            baseline
                .peak
                .min(current.peak - current.current() + baseline.current()),
        ),
    }
}

/// Format bytes as human-readable.
pub fn format_bytes(bytes: usize) -> String {
    const KB: usize = 1024;
    const MB: usize = KB * 1024;
    const GB: usize = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Calculate estimated plaintext data size for documents.
#[allow(dead_code)]
pub fn estimate_document_data_size(
    count: usize,
    avg_title_len: usize,
    avg_content_len: usize,
) -> usize {
    // Each document has: folder_id (16 bytes UUID), title, content, author_id (~20 bytes), timestamp (8 bytes)
    let per_doc = 16 + avg_title_len + avg_content_len + 20 + 8;
    count * per_doc
}

/// Calculate plaintext size of a single document.
pub fn document_plaintext_size(title: &str, content: &str, author_id: &str) -> usize {
    16 + // folder_id UUID
    title.len() +
    content.len() +
    author_id.len() +
    8 // timestamp
}

// ============================================================================
// Memory Breakdown Structures
// ============================================================================

/// Detailed memory breakdown for profiling.
#[derive(Debug, Clone, Default)]
pub struct MemoryBreakdown {
    /// SyncManager breakdown
    pub sync_manager: SyncManagerMemory,
    /// QueryManager breakdown
    pub query_manager: QueryManagerMemory,
    /// Total calculated
    pub total: usize,
}

impl MemoryBreakdown {
    /// Print a formatted breakdown.
    pub fn print(&self) {
        let sm = &self.sync_manager;
        let qm = &self.query_manager;

        println!("\nMemory breakdown:");
        println!(
            "  SyncManager: {} ({:.1}%)",
            format_bytes(sm.total),
            100.0 * sm.total as f64 / self.total.max(1) as f64
        );
        println!("    - catalogue: {}", format_bytes(sm.catalogue));
        println!("    - connections: {}", format_bytes(sm.connections));
        println!("    - subscriptions: {}", format_bytes(sm.subscriptions));
        println!("    - queues: {}", format_bytes(sm.queues));

        println!(
            "  QueryManager: {} ({:.1}%)",
            format_bytes(qm.total),
            100.0 * qm.total as f64 / self.total.max(1) as f64
        );
        println!("    - indices state: {}", format_bytes(qm.indices));
        println!("    - subscriptions: {}", format_bytes(qm.subscriptions));
        println!("    - policy checks: {}", format_bytes(qm.policy_checks));

        println!("  Total calculated: {}", format_bytes(self.total));
    }
}

/// Memory breakdown for SyncManager.
#[derive(Debug, Clone, Default)]
pub struct SyncManagerMemory {
    /// Size of catalogue state.
    pub catalogue: usize,
    /// Size of server/client connection state.
    pub connections: usize,
    /// Size of subscription and routing state.
    pub subscriptions: usize,
    /// Size of inbox/outbox and pending queues.
    pub queues: usize,
    /// Total
    pub total: usize,
}

/// Memory breakdown for QueryManager.
#[derive(Debug, Clone, Default)]
pub struct QueryManagerMemory {
    /// Size of index state (pending updates, etc.).
    pub indices: usize,
    /// Size of query subscriptions.
    pub subscriptions: usize,
    /// Size of active policy checks.
    pub policy_checks: usize,
    /// Total
    pub total: usize,
}
