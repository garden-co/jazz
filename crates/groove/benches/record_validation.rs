//! Isolated nested record admission receipt. Run with
//! `cargo bench -p groove --bench record_validation --profile perf`.
use groove::records::{OwnedRecord, RecordDescriptor, Value, ValueType};
use std::alloc::{GlobalAlloc, Layout, System};
use std::hint::black_box;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Instant;

struct CountingAllocator;
static COUNT: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;
unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNT.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(layout.size(), Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if COUNT.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(size, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, size) }
    }
}
fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    let iterations: usize = std::env::var("GROOVE_RECORD_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000);
    for depth in [1, 3, 6] {
        let setup_start = Instant::now();
        let mut descriptor =
            RecordDescriptor::new([("id", ValueType::U64), ("text", ValueType::String)]);
        let mut raw = descriptor
            .create(&[Value::U64(7), Value::String("synthetic payload".repeat(8))])
            .unwrap();
        for _ in 1..depth {
            let value = Value::Record(OwnedRecord::new(raw, descriptor));
            descriptor =
                RecordDescriptor::new([("child", ValueType::Record(Box::new(descriptor)))]);
            raw = descriptor.create(&[value]).unwrap();
        }
        let values = [Value::Record(OwnedRecord::new(raw, descriptor))];
        let parent = RecordDescriptor::new([("child", ValueType::Record(Box::new(descriptor)))]);
        let setup_ns = setup_start.elapsed().as_nanos();
        for _ in 0..100 {
            black_box(parent.create(black_box(&values)).unwrap());
        }
        let start = Instant::now();
        for _ in 0..iterations {
            black_box(parent.create(black_box(&values)).unwrap());
        }
        let encode_ns = start.elapsed().as_nanos();
        ALLOCATIONS.store(0, Ordering::Relaxed);
        BYTES.store(0, Ordering::Relaxed);
        COUNT.store(true, Ordering::Relaxed);
        black_box(parent.create(black_box(&values)).unwrap());
        COUNT.store(false, Ordering::Relaxed);
        println!(
            "{{\"depth\":{depth},\"iterations\":{iterations},\"setup_ns\":{setup_ns},\"encode_ns\":{encode_ns},\"allocations_per_encode\":{},\"bytes_per_encode\":{}}}",
            ALLOCATIONS.load(Ordering::Relaxed),
            BYTES.load(Ordering::Relaxed)
        );
    }
}
