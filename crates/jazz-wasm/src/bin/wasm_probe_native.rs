#[cfg(not(feature = "bench-probes"))]
fn main() {
    eprintln!("run with --features bench-probes");
    std::process::exit(2);
}

#[cfg(feature = "bench-probes")]
fn main() {
    use std::time::Instant;

    let probes: &[(&str, u32, u32, fn(u32, u32) -> u64)] = &[
        (
            "arithmetic_hash",
            jazz_wasm::bench_probes::ARITHMETIC_ITERS,
            0,
            |iterations, _| jazz_wasm::bench_probes::arithmetic_hash(iterations),
        ),
        (
            "dyn_dispatch",
            jazz_wasm::bench_probes::DYN_DISPATCH_ITERS,
            0,
            |iterations, _| jazz_wasm::bench_probes::dyn_dispatch(iterations),
        ),
        (
            "refcell_borrow",
            jazz_wasm::bench_probes::REFCELL_ITERS,
            0,
            |iterations, _| jazz_wasm::bench_probes::refcell_borrow(iterations),
        ),
        (
            "alloc_churn",
            jazz_wasm::bench_probes::ALLOC_ITERS,
            0,
            |iterations, _| jazz_wasm::bench_probes::alloc_churn(iterations),
        ),
        (
            "random_access_memory",
            jazz_wasm::bench_probes::MEMORY_ITERS,
            jazz_wasm::bench_probes::MEMORY_ENTRIES,
            jazz_wasm::bench_probes::random_access_memory,
        ),
    ];

    println!("[");
    for (index, (name, iterations, entries, run)) in probes.iter().enumerate() {
        let started = Instant::now();
        let checksum = run(*iterations, *entries);
        let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        println!(
            "  {{\"shape\":\"{}\",\"iterations\":{},\"entries\":{},\"elapsedMs\":{:.3},\"checksum\":\"{}\"}}{}",
            name,
            iterations,
            entries,
            elapsed_ms,
            checksum,
            if index + 1 == probes.len() { "" } else { "," }
        );
    }
    println!("]");
}
