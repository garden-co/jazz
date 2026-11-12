//! Heap receipt for the deep-room reads: how much memory each read needs while
//! it runs, how much an open view keeps, and how much stays after it closes.
//!
//! ```text
//! cargo run --release -p jazz-example-band-chat-benchmark --bin band-chat-memory-receipt -- 100000
//! ```
//!
//! The first argument is the deep room's size (default 100,000, the walltime
//! shape); a second argument `miniature` uses the test shape instead.
//!
//! Bytes are counted at the global allocator, so they are exact and per
//! operation, which process RSS cannot be: the allocator keeps freed pages, and
//! RSS includes everything else in the process. SQLite allocates with the
//! system malloc, past that allocator, so its own counters are read beside it:
//! its live bytes and its high-water mark. The counting slows every
//! allocation, so the times printed here are indicative only; the walltime
//! suite is the measurement of time.

use jazz_benchmark_guard::Allocator;
use jazz_example_band_chat_benchmark::deep_room::{DeepRoom, Shape};
use std::alloc::{GlobalAlloc, Layout};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

/// Live heap bytes and their high-water mark since the last [`Window`].
struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

fn grow(bytes: usize) {
    let live = LIVE.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK.fetch_max(live, Ordering::Relaxed);
}

fn shrink(bytes: usize) {
    LIVE.fetch_sub(bytes, Ordering::Relaxed);
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forward the caller's layout unchanged.
        let ptr = unsafe { Allocator.alloc(layout) };
        if !ptr.is_null() {
            grow(layout.size());
        }
        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        // SAFETY: forward the caller's layout unchanged.
        let ptr = unsafe { Allocator.alloc_zeroed(layout) };
        if !ptr.is_null() {
            grow(layout.size());
        }
        ptr
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // SAFETY: `ptr` came from `Allocator` with `layout`; forward both.
        let moved = unsafe { Allocator.realloc(ptr, layout, new_size) };
        if !moved.is_null() {
            if new_size >= layout.size() {
                grow(new_size - layout.size());
            } else {
                shrink(layout.size() - new_size);
            }
        }
        moved
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        shrink(layout.size());
        // SAFETY: `ptr` came from `Allocator` with `layout`.
        unsafe { Allocator.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// SQLite's live bytes, and its high-water mark (reset to the live bytes when
/// `reset`).
fn sqlite_bytes(reset: bool) -> (isize, isize) {
    // SAFETY: both are thread-safe status reads of the process-wide counters.
    unsafe {
        let peak = libsqlite3_sys::sqlite3_memory_highwater(reset.into()) as isize;
        (libsqlite3_sys::sqlite3_memory_used() as isize, peak)
    }
}

/// One measured stretch: the peak is relative to the live heap at its start.
struct Window {
    base: usize,
    sqlite_base: isize,
    started: Instant,
}

impl Window {
    fn open() -> Self {
        let base = LIVE.load(Ordering::Relaxed);
        PEAK.store(base, Ordering::Relaxed);
        let (sqlite_base, _) = sqlite_bytes(true);
        Self {
            base,
            sqlite_base,
            started: Instant::now(),
        }
    }

    fn peak(&self) -> isize {
        PEAK.load(Ordering::Relaxed) as isize - self.base as isize
    }

    fn held(&self) -> isize {
        LIVE.load(Ordering::Relaxed) as isize - self.base as isize
    }

    fn sqlite_peak(&self) -> isize {
        sqlite_bytes(false).1 - self.sqlite_base
    }

    fn sqlite_held(&self) -> isize {
        sqlite_bytes(false).0 - self.sqlite_base
    }
}

fn mb(bytes: isize) -> String {
    format!("{:.1}", bytes as f64 / (1024.0 * 1024.0))
}

/// A read that keeps a subscription open: peak while it settles, what the
/// open view holds, and what is left once the view is dropped and the engine
/// has run its pending work.
fn view<T>(room: &DeepRoom, name: &str, open: impl FnOnce() -> (T, usize)) {
    let window = Window::open();
    let (view, rows) = open();
    let elapsed = window.started.elapsed();
    let (peak, held) = (window.peak(), window.held());
    let (sqlite_peak, sqlite_held) = (window.sqlite_peak(), window.sqlite_held());
    drop(view);
    room.settle();
    room.settle();
    println!(
        "| {name} | {rows} | {} | {} | {} | {} | {} | {} | {} |",
        mb(peak),
        mb(held),
        mb(window.held()),
        mb(sqlite_peak),
        mb(sqlite_held),
        mb(window.sqlite_held()),
        elapsed.as_millis()
    );
}

/// A one-shot read: nothing stays open, so "held" is the same as "after".
fn once(room: &DeepRoom, name: &str, read: impl FnOnce() -> usize) {
    view(room, name, || ((), read()));
}

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    let deep: usize = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "100000".into())
        .parse()
        .expect("first argument: deep room size, a multiple of 50 and at least 8,000");
    let shape = match std::env::args().nth(2).as_deref() {
        Some("miniature") => Shape::miniature(deep),
        None => Shape::band(deep),
        Some(other) => panic!("second argument: `miniature` or nothing, not {other:?}"),
    };

    let seeding = Window::open();
    let room = DeepRoom::seeded(shape);
    println!(
        "fixture: deep room {deep}, {} messages in {} rooms; the seeded store holds {} MB \
         on the Rust heap and {} MB in SQLite (in-memory database, so this is the \
         database itself), peak while seeding {} MB and {} MB, {} s",
        shape.total_messages(),
        shape.rooms,
        mb(seeding.held()),
        mb(seeding.sqlite_held()),
        mb(seeding.peak()),
        mb(seeding.sqlite_peak()),
        seeding.started.elapsed().as_secs()
    );
    println!();
    println!(
        "| read | rows | peak MB | held open MB | after close MB | SQLite peak MB \
         | SQLite held open MB | SQLite after close MB | ms (indicative) |"
    );
    println!("|---|---|---|---|---|---|---|---|---|");
    view(&room, "open the room (newest page)", || {
        room.open_newest_page()
    });
    view(&room, "scroll back (older page)", || room.open_older_page());
    view(&room, "jump to a message (both halves)", || {
        let [older, newer] = room.jump_to_message();
        let rows = older.1 + newer.1;
        ([older.0, newer.0], rows)
    });
    once(&room, "search the room", || room.search_room());
    view(&room, "unread count", || room.open_unread_count());
    view(&room, "room list", || room.open_inbox());
    once(&room, "read by", || room.read_by_sheet());
    // The same view opened again: whether its cost was a one-time cache or is
    // paid on every open.
    view(&room, "open the room again", || room.open_newest_page());
}
