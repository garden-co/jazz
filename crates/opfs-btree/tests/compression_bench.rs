//! Measurement for overflow compression. Run with:
//!   cargo test -p opfs-btree --test compression_bench -- --ignored --nocapture
//!
//! Reports on-disk size (the metric compression actually targets) and insert
//! time, with `compress_overflow` on vs off. Uses an in-memory file, so the
//! time column shows only compression's CPU cost — the real win is the byte
//! reduction, which on OPFS/disk also means proportionally less write I/O.

use std::time::{Duration, Instant};

use opfs_btree::{BTreeOptions, MemoryFile, OpfsBTree, SyncFile};

const ROWS: usize = 2_000;
const PAYLOAD_BYTES: usize = 20 * 1024;

fn key(i: usize) -> Vec<u8> {
    format!("row/{i:08}").into_bytes()
}

// Structured, repetitive text resembling a serialized row, with the row id
// woven in so values are not byte-identical.
fn compressible_payload(seed: usize) -> Vec<u8> {
    let base = "{\"title\":\"sync the radiant osprey dashboard\",\"done\":false,\
                \"owner\":\"user-0042\",\"note\":\"lorem ipsum dolor sit amet\"},";
    let mut out = format!("[row-{seed}]");
    while out.len() < PAYLOAD_BYTES {
        out.push_str(base);
    }
    out.truncate(PAYLOAD_BYTES);
    out.into_bytes()
}

fn incompressible_payload(seed: usize) -> Vec<u8> {
    let mut out = vec![0u8; PAYLOAD_BYTES];
    let mut state = (seed as u32).wrapping_mul(2_654_435_761).wrapping_add(1);
    for b in out.iter_mut() {
        state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        *b = (state >> 24) as u8;
    }
    out
}

fn run(compress: bool, payloads: &[Vec<u8>]) -> (u64, Duration) {
    let options = BTreeOptions {
        compress_overflow: compress,
        ..Default::default()
    };
    let file = MemoryFile::new();
    let mut tree = OpfsBTree::open(file.clone(), options).expect("open");
    let started = Instant::now();
    for (i, payload) in payloads.iter().enumerate() {
        tree.put(&key(i), payload).expect("put");
    }
    tree.checkpoint().expect("checkpoint");
    let elapsed = started.elapsed();
    (file.len().expect("len"), elapsed)
}

fn report(label: &str, payloads: &[Vec<u8>]) {
    let raw_total: usize = payloads.iter().map(Vec::len).sum();
    let (off_bytes, off_time) = run(false, payloads);
    let (on_bytes, on_time) = run(true, payloads);
    let pct = 100.0 * (1.0 - on_bytes as f64 / off_bytes as f64);
    println!(
        "\n{label}: {ROWS} rows x {PAYLOAD_BYTES} B (raw payload {} MiB)",
        raw_total >> 20
    );
    println!(
        "  compress off : {:>8.2} MiB on disk, {:>6.1?}",
        off_bytes as f64 / (1 << 20) as f64,
        off_time
    );
    println!(
        "  compress on  : {:>8.2} MiB on disk, {:>6.1?}",
        on_bytes as f64 / (1 << 20) as f64,
        on_time
    );
    println!("  -> {pct:.1}% smaller on disk");
}

#[test]
#[ignore = "measurement, run explicitly with --ignored --nocapture"]
fn measure_overflow_compression() {
    let compressible: Vec<Vec<u8>> = (0..ROWS).map(compressible_payload).collect();
    let incompressible: Vec<Vec<u8>> = (0..ROWS).map(incompressible_payload).collect();
    report("compressible (row-like text)", &compressible);
    report("incompressible (random bytes)", &incompressible);
}
