//! A global allocator wrapper that samples live heap allocations.
//!
//! Like tcmalloc, it samples allocations as a Poisson process over allocated
//! bytes: each thread counts bytes down from an exponentially distributed
//! interval with mean [`sample_interval`] and records the allocation that
//! crosses zero, with its stack. A sampled allocation of `size` bytes stands
//! for `size / (1 - exp(-size / mean))` bytes, which keeps the in-use
//! estimate unbiased whatever the mix of allocation sizes. Stacks come from
//! walking frame pointers, which `.cargo/config.toml` keeps in every Linux
//! build; unwinding through `.eh_frame` instead tripled the server's CPU per
//! write in the static musl build (#3940).
//!
//! Sampling is off until [`enable_sampling`] is called. While it is off, each
//! thread's first allocation parks its countdown at `i64::MAX`, so
//! allocations pay one thread-local subtraction and deallocations one load of
//! a flag. Once on, unsampled calls pay the subtraction on allocation and one
//! load from a 128 KiB counting filter on deallocation; only samples and
//! frees that hit the filter take the lock on the table of live samples.

use std::alloc::{GlobalAlloc, Layout};
use std::cell::Cell;
use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};

/// Mean bytes allocated between samples unless [`enable_sampling`] sets
/// another.
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
static ENABLED: AtomicBool = AtomicBool::new(false);

/// Mean bytes allocated between samples.
pub fn sample_interval() -> u64 {
    SAMPLE_INTERVAL.load(Ordering::Relaxed)
}

/// Whether [`enable_sampling`] has been called.
pub fn sampling_enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

/// Start sampling, about once per `mean_bytes` allocated. The calling thread
/// and threads that start later sample right away; threads that already
/// allocated never do, so call it before spawning threads.
pub fn enable_sampling(mean_bytes: u64) {
    let mean = mean_bytes.max(1);
    SAMPLE_INTERVAL.store(mean, Ordering::Relaxed);
    ENABLED.store(true, Ordering::Relaxed);
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
    /// This thread's stack bounds once a sample needed them, and whether
    /// they are final: only the main thread's stack grows.
    static STACK: Cell<(usize, usize, bool)> = const { Cell::new((0, 0, false)) };
}

/// A sampled allocation that has not been freed.
pub struct LiveSample {
    /// Estimated bytes in use that this sample stands for.
    pub weight: f64,
    /// Return addresses of the allocating stack, innermost first: into the
    /// sampler and the allocator shim, then into the code that allocated.
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
    if ENABLED.load(Ordering::Relaxed) && filter_slot(ptr as usize).load(Ordering::Relaxed) != 0 {
        forget(ptr as usize);
    }
}

#[cold]
#[inline(never)]
fn crossed_interval(ptr: usize, size: usize) {
    if !ENABLED.load(Ordering::Relaxed) {
        // Never cross again on this thread.
        COUNTDOWN.set(i64::MAX);
        return;
    }
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
    let depth = walk_frame_pointers(&mut frames);
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

/// Fill `frames` with the return addresses of the calling stack, innermost
/// first, by following its frame pointers, and return how many it found.
///
/// A frame record is the caller's frame pointer followed by the return
/// address, on x86-64 and AArch64 alike. Records on this thread's stack
/// between this function's locals and the stack's top are mapped and read
/// directly. Any other record, such as one on a stack segment that `stacker`
/// switched to, is read through `process_vm_readv`, which reports unmapped
/// memory instead of faulting. So a frame without a frame pointer ends the
/// stack early (or adds a wrong frame) but never crashes. Each record must
/// lie above the last, so a stack segment mapped above the stack it was
/// entered from would end the walk at the switch; thread entry points zero
/// the frame pointer, which ends it at the root.
#[inline(always)]
fn walk_frame_pointers(frames: &mut [usize; MAX_FRAMES]) -> usize {
    let local = 0u8;
    let here = std::hint::black_box(&local) as *const u8 as usize;
    let on_stack = stack_top(here).map_or(0..0, |high| here..high);
    let mut elsewhere = CopiedStack::default();
    let mut depth = 0;
    let mut frame = frame_pointer();
    while depth < MAX_FRAMES && frame.is_multiple_of(size_of::<usize>()) {
        let record = if on_stack.contains(&frame) && on_stack.end - frame >= RECORD {
            // SAFETY: both words lie between this function's locals and the
            // top of the current thread's stack, all of which is mapped.
            unsafe {
                let record = frame as *const usize;
                Some((record.read(), record.add(1).read()))
            }
        } else {
            elsewhere.record_at(frame)
        };
        let Some((caller_frame, return_address)) = record else {
            break;
        };
        if return_address == 0 {
            break;
        }
        frames[depth] = return_address;
        depth += 1;
        if caller_frame <= frame {
            break;
        }
        frame = caller_frame;
    }
    depth
}

/// Bytes in a frame record.
const RECORD: usize = 2 * size_of::<usize>();

/// Words of memory off this thread's stack, copied with `process_vm_readv`
/// a page or so at a time, since frame records sit close together.
struct CopiedStack {
    start: usize,
    len: usize,
    words: [usize; 512],
}

impl Default for CopiedStack {
    fn default() -> Self {
        Self {
            start: 0,
            len: 0,
            words: [0; 512],
        }
    }
}

impl CopiedStack {
    /// The frame record at `frame`, or `None` if it is not mapped.
    fn record_at(&mut self, frame: usize) -> Option<(usize, usize)> {
        let copied = frame >= self.start && frame - self.start + RECORD <= self.len;
        if !copied {
            let local = libc::iovec {
                iov_base: self.words.as_mut_ptr().cast(),
                iov_len: size_of_val(&self.words),
            };
            let remote = libc::iovec {
                iov_base: frame as *mut libc::c_void,
                iov_len: size_of_val(&self.words),
            };
            // SAFETY: `local` is this buffer; the kernel checks `remote`
            // and copies only what is mapped, up to the first gap.
            let read = unsafe { libc::process_vm_readv(libc::getpid(), &local, 1, &remote, 1, 0) };
            self.start = frame;
            self.len = usize::try_from(read).unwrap_or(0);
            if self.len < RECORD {
                return None;
            }
        }
        let index = (frame - self.start) / size_of::<usize>();
        Some((self.words[index], self.words[index + 1]))
    }
}

/// The frame pointer of the function this is inlined into.
#[inline(always)]
fn frame_pointer() -> usize {
    let frame: usize;
    // SAFETY: reads a register.
    unsafe {
        #[cfg(target_arch = "x86_64")]
        std::arch::asm!("mov {}, rbp", out(reg) frame, options(nomem, nostack, preserves_flags));
        #[cfg(target_arch = "aarch64")]
        std::arch::asm!("mov {}, x29", out(reg) frame, options(nomem, nostack, preserves_flags));
    }
    frame
}

/// One past the highest address of this thread's stack, which `here` is
/// on, or `None` when `here` is not on it (a `stacker` segment or a signal
/// stack) or the bounds can't be told.
fn stack_top(here: usize) -> Option<usize> {
    let (low, high, last) = STACK.get();
    if (low..high).contains(&here) {
        return Some(high);
    }
    if last {
        return None;
    }
    // First sample on this thread, or on the main thread, whose stack may
    // have grown since (musl reports only the part mapped so far).
    // SAFETY: `attr` is initialised by `pthread_getattr_np` before it is
    // read, and destroyed once.
    let (low, high) = unsafe {
        let mut attr = std::mem::zeroed::<libc::pthread_attr_t>();
        if libc::pthread_getattr_np(libc::pthread_self(), &mut attr) != 0 {
            return None;
        }
        let mut base = std::ptr::null_mut();
        let mut size = 0;
        let found = libc::pthread_attr_getstack(&attr, &mut base, &mut size) == 0;
        libc::pthread_attr_destroy(&mut attr);
        if !found {
            return None;
        }
        (base as usize, base as usize + size)
    };
    // SAFETY: plain syscalls.
    let main = unsafe { libc::syscall(libc::SYS_gettid) == libc::getpid() as libc::c_long };
    STACK.set((low, high, !main));
    (low..high).contains(&here).then_some(high)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[inline(never)]
    fn walk_at_depth(depth: usize) -> usize {
        if depth == 0 {
            let mut frames = [0usize; MAX_FRAMES];
            walk_frame_pointers(&mut frames)
        } else {
            std::hint::black_box(walk_at_depth(depth - 1))
        }
    }

    /// The walk reaches past every recursive frame to the thread's entry.
    #[test]
    fn walks_every_frame_up_to_the_thread_entry() {
        let shallow = walk_at_depth(0);
        assert!(
            shallow > 3,
            "a test thread's stack has more than {shallow} frames"
        );
        assert_eq!(walk_at_depth(20), (shallow + 20).min(MAX_FRAMES));
    }

    /// Bounds remembered from a shallower point, as musl reports for the
    /// main thread before its stack grows, are looked up again rather than
    /// cutting deeper stacks off.
    #[test]
    fn looks_stack_bounds_up_again_when_the_stack_outgrows_them() {
        let local = 0u8;
        let here = std::hint::black_box(&local) as *const u8 as usize;
        let high = stack_top(here).expect("a test thread knows its stack");
        STACK.set((here + 1, high, false));
        assert_eq!(stack_top(here), Some(high));
        assert!(walk_at_depth(10) > 10);
    }

    /// Code that polls on a `stacker` segment, as the server's database
    /// does, keeps its whole stack: the frames on the segment and those on
    /// the thread's stack below the switch.
    #[test]
    fn walks_from_a_stacker_segment_back_onto_the_thread_stack() {
        let on_thread = walk_at_depth(0);
        let on_segment = stacker::grow(1 << 20, || walk_at_depth(10));
        assert!(
            on_segment >= on_thread + 10,
            "{on_segment} frames on the segment, {on_thread} on the thread"
        );
    }
}
