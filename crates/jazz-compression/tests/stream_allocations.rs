//! The public streaming contract includes bounded working memory. Allocation
//! instrumentation is necessary here: a byte-equality test cannot distinguish
//! incremental decoding from storing and replaying the entire channel history.
//! Native zstd C allocations are outside this Rust allocator receipt; its
//! window cap is enforced separately through the native decoder parameter.
#![cfg(any(feature = "lz4", feature = "zstd", feature = "ruzstd"))]
use jazz_compression::stream::{Codec, StreamDecoder};
use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::io::Write;

struct TrackingAllocator;
thread_local! {
    static TRACK: Cell<bool> = const { Cell::new(false) };
    static LIVE_DELTA: Cell<isize> = const { Cell::new(0) };
    static PEAK_DELTA: Cell<isize> = const { Cell::new(0) };
}
fn record(size: isize) {
    if TRACK.try_with(Cell::get).unwrap_or(false) {
        let _ = LIVE_DELTA.try_with(|value| {
            let live = value.get() + size;
            value.set(live);
            let _ = PEAK_DELTA.try_with(|peak| peak.set(peak.get().max(live)));
        });
    }
}
// SAFETY: All operations delegate unchanged pointers, layouts, and sizes to
// System. The thread-local counters neither allocate nor touch allocation data.
unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size() as isize);
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size() as isize);
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        record(-(layout.size() as isize));
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record(new_size as isize - layout.size() as isize);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}
#[global_allocator]
static ALLOCATOR: TrackingAllocator = TrackingAllocator;

fn decode_message(decoder: &mut StreamDecoder, input: &[u8], expected: &[u8]) {
    let mut input = input;
    let mut output = [0; 8192];
    let mut offset = 0;
    loop {
        let p = decoder.decode(input, &mut output).unwrap();
        assert_eq!(&output[..p.written], &expected[offset..offset + p.written]);
        offset += p.written;
        input = &input[p.consumed..];
        if input.is_empty() && p.written < output.len() {
            break;
        }
        assert!(p.consumed + p.written > 0);
    }
    assert_eq!(offset, expected.len());
}

#[test]
fn warmed_decoder_memory_does_not_grow_with_channel_history() {
    let mut state = 12345_u32;
    let data: Vec<u8> = (0..8192)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state as u8
        })
        .collect();
    let codecs = vec![
        #[cfg(feature = "lz4")]
        Codec::Lz4,
        #[cfg(any(feature = "zstd", feature = "ruzstd"))]
        Codec::Zstd,
    ];
    for codec in codecs {
        // Fixture generation and output ownership are outside the measured
        // decoder. The persistent fixture contains over 8MiB of logical data.
        let mut encoder: Box<dyn FixtureEncoder> = match codec {
            #[cfg(feature = "lz4")]
            Codec::Lz4 => Box::new(lz4_flex::frame::FrameEncoder::with_frame_info(
                lz4_flex::frame::FrameInfo::new()
                    .block_size(lz4_flex::frame::BlockSize::Max64KB)
                    .block_mode(lz4_flex::frame::BlockMode::Linked),
                Vec::new(),
            )),
            Codec::Zstd => {
                let mut encoder = zstd::stream::write::Encoder::new(Vec::new(), 3).unwrap();
                encoder.window_log(16).unwrap();
                Box::new(encoder)
            }
            #[allow(unreachable_patterns)]
            _ => unreachable!(),
        };
        let mut messages = Vec::new();
        for _ in 0..1024 {
            encoder.write_all(&data).unwrap();
            encoder.flush().unwrap();
            messages.push(encoder.take_bytes());
        }
        let mut decoder = StreamDecoder::new(codec).unwrap();
        for message in &messages[..16] {
            decode_message(&mut decoder, message, &data);
        }
        LIVE_DELTA.set(0);
        PEAK_DELTA.set(0);
        TRACK.set(true);
        for message in &messages[16..] {
            decode_message(&mut decoder, message, &data);
        }
        TRACK.set(false);
        assert_eq!(
            LIVE_DELTA.get(),
            0,
            "{codec:?} retains allocations as channel history grows"
        );
        assert!(
            PEAK_DELTA.get() <= 65536,
            "{codec:?} temporary decode allocation exceeded one block: {}",
            PEAK_DELTA.get()
        );
    }
}
trait FixtureEncoder: Write {
    fn take_bytes(&mut self) -> Vec<u8>;
}
impl FixtureEncoder for zstd::stream::write::Encoder<'_, Vec<u8>> {
    fn take_bytes(&mut self) -> Vec<u8> {
        std::mem::take(self.get_mut())
    }
}
#[cfg(feature = "lz4")]
impl FixtureEncoder for lz4_flex::frame::FrameEncoder<Vec<u8>> {
    fn take_bytes(&mut self) -> Vec<u8> {
        std::mem::take(self.get_mut())
    }
}
