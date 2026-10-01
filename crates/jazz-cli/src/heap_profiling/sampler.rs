//! A global allocator wrapper that samples live heap allocations.
//!
//! Like tcmalloc, it samples allocations as a Poisson process over allocated
//! bytes: each thread counts bytes down from an exponentially distributed
//! interval with mean [`sample_interval`] and records the allocation that
//! crosses zero, with its stack. A sampled allocation of `size` bytes stands
//! for `size / (1 - exp(-size / mean))` bytes, which keeps the in-use
//! estimate unbiased whatever the mix of allocation sizes.
//!
//! Unsampled calls pay one thread-local subtraction on allocation and one
//! load from a 128 KiB counting filter on deallocation; only samples and
//! frees that hit the filter take the lock on the table of live samples.

use std::alloc::{GlobalAlloc, Layout};
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU16, AtomicU64, Ordering};

/// Mean bytes allocated between samples unless [`set_sample_interval`]
/// changes it.
pub const DEFAULT_SAMPLE_INTERVAL: u64 = 512 * 1024;
const MAX_FRAMES: usize = 64;
const FILTER_SLOTS: usize = 1 << 16;

/// Forwards to `A` and samples what it allocates. Use it as the
/// `#[global_allocator]`.
pub struct SamplingAllocator<A> {
    inner: A,
}

impl<A> SamplingAllocator<A> {
    pub const fn new(inner: A) -> Self {
        Self { inner }
    }
}

static SAMPLE_INTERVAL: AtomicU64 = AtomicU64::new(DEFAULT_SAMPLE_INTERVAL);

/// Mean bytes allocated between samples.
pub fn sample_interval() -> u64 {
    SAMPLE_INTERVAL.load(Ordering::Relaxed)
}

/// Change the mean bytes allocated between samples. The calling thread and
/// threads that start later use it right away; other running threads pick
/// it up from their next sample on, so set it before spawning threads.
pub fn set_sample_interval(bytes: u64) {
    let mean = bytes.max(1);
    SAMPLE_INTERVAL.store(mean, Ordering::Relaxed);
    COUNTDOWN.set(next_interval(mean));
}

thread_local! {
    /// Bytes left until the next sample. Starts at zero so that the
    /// thread's first allocation draws its first interval.
    static COUNTDOWN: Cell<i64> = const { Cell::new(0) };
    /// Per-thread xorshift state; zero until the first interval is drawn.
    static RNG: Cell<u64> = const { Cell::new(0) };
    /// Set while this thread is inside the sampler, whose own allocations
    /// are never sampled and whose own frees never touch the table.
    static IN_SAMPLER: Cell<bool> = const { Cell::new(false) };
}

/// A sampled allocation that has not been freed.
pub struct LiveSample {
    /// Estimated bytes in use that this sample stands for.
    pub weight: f64,
    /// Instruction pointers of the allocating stack, innermost first: the
    /// sampler's own frame, then the allocator shim's, then the caller's.
    pub stack: Box<[usize]>,
}

static LIVE: Mutex<Option<HashMap<usize, LiveSample>>> = Mutex::new(None);

/// Live samples per hash slot, so that freeing unsampled memory skips the
/// table without locking. A slot that reaches `u16::MAX` stays there, so
/// frees hashing to it always check the table.
static FILTER: [AtomicU16; FILTER_SLOTS] = [const { AtomicU16::new(0) }; FILTER_SLOTS];

#[inline(always)]
fn filter_slot(ptr: usize) -> &'static AtomicU16 {
    let hash = (ptr as u64 >> 4).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    &FILTER[(hash >> (64 - FILTER_SLOTS.trailing_zeros())) as usize]
}

// SAFETY: every call is forwarded to `inner` unchanged; the bookkeeping
// around it never touches the memory.
unsafe impl<A: GlobalAlloc> GlobalAlloc for SamplingAllocator<A> {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.inner.alloc(layout) };
        after_alloc(ptr, layout.size());
        ptr
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.inner.alloc_zeroed(layout) };
        after_alloc(ptr, layout.size());
        ptr
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        before_dealloc(ptr);
        unsafe { self.inner.dealloc(ptr, layout) }
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // Forget `ptr` before it can be freed and handed to another thread.
        // If the realloc fails, the old block stays unsampled.
        before_dealloc(ptr);
        let new = unsafe { self.inner.realloc(ptr, layout, new_size) };
        after_alloc(new, new_size);
        new
    }
}

#[inline(always)]
fn after_alloc(ptr: *mut u8, size: usize) {
    if ptr.is_null() {
        return;
    }
    let left = COUNTDOWN.get().wrapping_sub(size as i64);
    COUNTDOWN.set(left);
    if left <= 0 {
        crossed_interval(ptr as usize, size);
    }
}

#[inline(always)]
fn before_dealloc(ptr: *mut u8) {
    if filter_slot(ptr as usize).load(Ordering::Relaxed) != 0 {
        forget(ptr as usize);
    }
}

#[cold]
#[inline(never)]
fn crossed_interval(ptr: usize, size: usize) {
    if IN_SAMPLER.get() {
        // One of the sampler's own allocations: sample a later one instead.
        return;
    }
    let first = RNG.get() == 0;
    let mean = sample_interval();
    COUNTDOWN.set(next_interval(mean));
    if first {
        return;
    }
    IN_SAMPLER.set(true);
    let mut frames = [0usize; MAX_FRAMES];
    let mut depth = 0;
    // Frames below this one's locals belong to the unwinder, which may live
    // in a shared library without symbols to recognise it by.
    let local = 0u8;
    let here = std::hint::black_box(&local) as *const u8 as usize;
    // SAFETY: unwinding neither allocates nor needs a lock this thread
    // holds, and no other thread's `trace` can reach this one's stack.
    unsafe {
        backtrace::trace_unsynchronized(|frame| {
            let frame_base = frame.sp() as usize;
            if frame_base == 0 || frame_base > here {
                frames[depth] = frame.ip() as usize;
                depth += 1;
            }
            depth < MAX_FRAMES
        });
    }
    let mean = mean as f64;
    let sample = LiveSample {
        weight: size as f64 / (1.0 - (-(size as f64) / mean).exp()),
        stack: frames[..depth].into(),
    };
    let mut live = LIVE.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    live.get_or_insert_with(HashMap::new).insert(ptr, sample);
    let _ = filter_slot(ptr).fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
        count.checked_add(1)
    });
    drop(live);
    IN_SAMPLER.set(false);
}

#[cold]
#[inline(never)]
fn forget(ptr: usize) {
    if IN_SAMPLER.get() {
        // The table freeing its own memory while this thread holds the
        // lock; that memory was never sampled.
        return;
    }
    IN_SAMPLER.set(true);
    let mut live = LIVE.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if live.as_mut().and_then(|live| live.remove(&ptr)).is_some() {
        let _ = filter_slot(ptr).fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
            (count != u16::MAX).then(|| count - 1)
        });
    }
    drop(live);
    IN_SAMPLER.set(false);
}

/// Exponentially distributed with the given mean, from this thread's
/// xorshift generator.
fn next_interval(mean: u64) -> i64 {
    let mut x = RNG.get();
    if x == 0 {
        // Distinct per thread (the address of its thread-local) and per run.
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos() as u64);
        let thread = RNG.with(|rng| rng as *const Cell<u64> as u64);
        x = thread ^ nanos | 1;
    }
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    RNG.set(x);
    // Uniform in (0, 1], so the logarithm stays finite.
    let uniform = ((x >> 11) + 1) as f64 / (1u64 << 53) as f64;
    (-uniform.ln() * mean as f64) as i64 + 1
}

/// Visit every live sample while holding the table. Allocations `visit`
/// makes are not sampled, and it must not free sampled memory.
pub fn for_each_live_sample(mut visit: impl FnMut(&LiveSample)) {
    let was_in_sampler = IN_SAMPLER.replace(true);
    let live = LIVE.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(live) = live.as_ref() {
        live.values().for_each(&mut visit);
    }
    drop(live);
    IN_SAMPLER.set(was_in_sampler);
}
