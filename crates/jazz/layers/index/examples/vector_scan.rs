//! Exact-scan cost of a vector search (SPEC 20.5, `VECTOR_EXACT_MAX`).
//!
//! Scores `N` stored `f32le/1` vectors of `D` dimensions against one query
//! and keeps the best `K`, the Rust half of phase 1 (SQLite reads excluded).
//! Run natively with `cargo run --release -p jazz-index --example
//! vector_scan`, or build for `wasm32-wasip1` and run under a WASI host to
//! approximate the browser. Arguments: `[n] [dimensions] [k] [rounds]`.

use std::time::Instant;

use jazz_index::vector::{Metric, TopK, decode_f32le, encode_f32le, prepare, score};

fn lcg(state: &mut u64) -> f32 {
    *state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
    ((*state >> 40) as f32 / (1u64 << 24) as f32) * 2.0 - 1.0
}

fn main() {
    let args: Vec<usize> = std::env::args()
        .skip(1)
        .map(|a| a.parse().unwrap())
        .collect();
    let n = args.first().copied().unwrap_or(50_000);
    let d = args.get(1).copied().unwrap_or(384);
    let k = args.get(2).copied().unwrap_or(10);
    let rounds = args.get(3).copied().unwrap_or(5);

    let mut seed = 42u64;
    let mut cells: Vec<Vec<u8>> = Vec::with_capacity(n);
    for _ in 0..n {
        let v: Vec<f32> = (0..d).map(|_| lcg(&mut seed)).collect();
        cells.push(encode_f32le(&prepare(Metric::Cosine, &v).unwrap()));
    }
    let q: Vec<f32> = (0..d).map(|_| lcg(&mut seed)).collect();
    let q = prepare(Metric::Cosine, &q).unwrap();

    for metric in [Metric::Cosine, Metric::L2] {
        let mut best = f64::INFINITY;
        let mut checksum = 0u64;
        for _ in 0..rounds {
            let start = Instant::now();
            let mut top = TopK::new(k);
            for (i, cell) in cells.iter().enumerate() {
                let v = decode_f32le(cell, d).unwrap();
                top.push(score(metric, &q, &v), i as u64, ());
            }
            let result = top.into_sorted();
            let ms = start.elapsed().as_secs_f64() * 1e3;
            best = best.min(ms);
            checksum = result
                .iter()
                .fold(0, |acc, (s, id, _)| acc ^ s.to_bits() ^ id);
        }
        println!(
            "{} n={n} d={d} k={k}: best of {rounds} = {best:.1} ms ({:.0} ns/vector), checksum {checksum:016x}",
            metric.as_str(),
            best * 1e6 / n as f64
        );
    }
}
