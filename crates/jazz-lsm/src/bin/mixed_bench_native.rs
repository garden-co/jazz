#[cfg(target_arch = "wasm32")]
fn main() {}

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use std::env;
    use std::path::{Path, PathBuf};
    use std::time::Instant;

    use jazz_lsm::{LsmOptions, LsmTree, StdFs, WriteDurability};
    use serde::Serialize;

    const DEFAULT_COUNT: usize = 5_000;
    const DEFAULT_VALUE_SIZES: [usize; 3] = [32, 256, 4096];

    #[derive(Debug, Clone, Copy)]
    struct MixedScenario {
        name: &'static str,
        read_pct: u8,
        write_pct: u8,
        update_pct: u8,
    }

    const MIXED_SCENARIOS: [MixedScenario; 3] = [
        MixedScenario {
            name: "mixed_random_70r_30w",
            read_pct: 70,
            write_pct: 30,
            update_pct: 80,
        },
        MixedScenario {
            name: "mixed_random_50r_50w_with_updates",
            read_pct: 50,
            write_pct: 50,
            update_pct: 90,
        },
        MixedScenario {
            name: "mixed_random_60r_20w_20d",
            read_pct: 60,
            write_pct: 20,
            update_pct: 80,
        },
    ];

    #[derive(Debug, Clone, Serialize)]
    struct BenchmarkResult {
        operation: String,
        value_size: u32,
        count: u32,
        elapsed_ms: f64,
        ops_per_sec: f64,
        p95_op_ms: f64,
        reads: u32,
        read_hits: u32,
        read_misses: u32,
        writes: u32,
        deletes: u32,
        checksum: u64,
    }

    #[derive(Debug, Clone, Copy)]
    enum OpChoice {
        Read,
        Write,
        Delete,
    }

    #[derive(Debug, Clone)]
    struct Args {
        count: usize,
        value_sizes: Vec<usize>,
        json: bool,
    }

    struct DeterministicRng {
        state: u64,
    }

    impl DeterministicRng {
        fn new(seed: u64) -> Self {
            Self { state: seed }
        }

        fn next_u64(&mut self) -> u64 {
            self.state = self.state.wrapping_mul(6364136223846793005).wrapping_add(1);
            self.state
        }

        fn next_u8(&mut self) -> u8 {
            (self.next_u64() >> 56) as u8
        }

        fn next_usize(&mut self, upper: usize) -> usize {
            if upper == 0 {
                return 0;
            }
            (self.next_u64() as usize) % upper
        }
    }

    fn bench_options() -> LsmOptions {
        LsmOptions {
            max_memtable_bytes: 512 * 1024,
            max_wal_bytes: 8 * 1024 * 1024,
            level0_file_limit: 4,
            level_fanout: 4,
            max_levels: 4,
            write_durability: WriteDurability::Buffered,
            ..Default::default()
        }
    }

    fn open_db(path: &Path) -> LsmTree<StdFs> {
        let fs = StdFs::new(path).expect("open std fs");
        LsmTree::open(fs, bench_options(), Vec::new()).expect("open lsm tree")
    }

    fn temp_db_dir(label: &str, value_size: usize, count: usize) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let pid = std::process::id();
        std::env::temp_dir().join(format!(
            "jazz-lsm-{label}-{value_size}-{count}-{pid}-{nanos}"
        ))
    }

    fn key(i: usize) -> Vec<u8> {
        format!("k{i:08}").into_bytes()
    }

    fn value(size: usize, seed: u8) -> Vec<u8> {
        let mut out = vec![0u8; size];
        for (i, byte) in out.iter_mut().enumerate() {
            *byte = seed.wrapping_add((i % 251) as u8);
        }
        out
    }

    fn choose_operation(s: &MixedScenario, roll: u8) -> OpChoice {
        if roll < s.read_pct {
            return OpChoice::Read;
        }
        if roll < s.read_pct.saturating_add(s.write_pct) {
            return OpChoice::Write;
        }
        OpChoice::Delete
    }

    fn percentile_ms(latencies_ns: &mut [u64], percentile: f64) -> f64 {
        if latencies_ns.is_empty() {
            return 0.0;
        }

        latencies_ns.sort_unstable();
        let pos = ((latencies_ns.len() as f64 * percentile).ceil() as usize)
            .saturating_sub(1)
            .min(latencies_ns.len() - 1);
        latencies_ns[pos] as f64 / 1_000_000.0
    }

    fn preload(db: &mut LsmTree<StdFs>, key_space: usize, value_size: usize) {
        for i in 0..key_space {
            let k = key(i);
            let v = value(value_size, (i % 251) as u8);
            db.put(&k, &v).expect("preload put");
        }
        db.flush().expect("preload flush");
    }

    fn run_mixed_scenario(
        scenario: MixedScenario,
        count: usize,
        value_size: usize,
    ) -> BenchmarkResult {
        let dir = temp_db_dir(scenario.name, value_size, count);
        std::fs::create_dir_all(&dir).expect("create temp db directory");
        let mut db = open_db(&dir);

        // Preload to ensure mixed workloads stress both reads and updates/deletes.
        let initial_key_space = count.max(1);
        preload(&mut db, initial_key_space, value_size);

        let mut rng = DeterministicRng::new(0xA5A5_A5A5_0123_4567 ^ (value_size as u64));
        let mut key_space = initial_key_space;
        let mut op_latencies_ns = Vec::with_capacity(count);

        let mut reads = 0u32;
        let mut read_hits = 0u32;
        let mut read_misses = 0u32;
        let mut writes = 0u32;
        let mut deletes = 0u32;
        let mut checksum = 0u64;

        let total_start = Instant::now();
        for step in 0..count {
            let op = choose_operation(&scenario, rng.next_u8() % 100);
            let op_start = Instant::now();

            match op {
                OpChoice::Read => {
                    reads += 1;
                    let idx = rng.next_usize(key_space.max(1));
                    let k = key(idx);
                    let maybe = db.get(&k).expect("read");
                    if let Some(v) = maybe {
                        read_hits += 1;
                        checksum = checksum.wrapping_add(v[0] as u64);
                    } else {
                        read_misses += 1;
                        checksum = checksum.wrapping_add(1);
                    }
                }
                OpChoice::Write => {
                    writes += 1;
                    let update = (rng.next_u8() % 100) < scenario.update_pct;
                    let idx = if update || key_space == 0 {
                        rng.next_usize(key_space.max(1))
                    } else {
                        let i = key_space;
                        key_space += 1;
                        i
                    };
                    let k = key(idx);
                    let v = value(value_size, ((step + idx) % 251) as u8);
                    checksum = checksum.wrapping_add(v[0] as u64);
                    db.put(&k, &v).expect("write");
                }
                OpChoice::Delete => {
                    deletes += 1;
                    let idx = rng.next_usize(key_space.max(1));
                    let k = key(idx);
                    db.delete(&k).expect("delete");
                    checksum = checksum.wrapping_add(idx as u64);
                }
            }

            op_latencies_ns.push(op_start.elapsed().as_nanos() as u64);
        }
        db.flush().expect("final flush");
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
        let elapsed = total_start.elapsed();

        BenchmarkResult {
            operation: scenario.name.to_string(),
            value_size: value_size as u32,
            count: count as u32,
            elapsed_ms: elapsed.as_secs_f64() * 1000.0,
            ops_per_sec: count as f64 / elapsed.as_secs_f64(),
            p95_op_ms: percentile_ms(&mut op_latencies_ns, 0.95),
            reads,
            read_hits,
            read_misses,
            writes,
            deletes,
            checksum,
        }
    }

    fn parse_args() -> Result<Args, String> {
        let mut out = Args {
            count: DEFAULT_COUNT,
            value_sizes: DEFAULT_VALUE_SIZES.to_vec(),
            json: false,
        };

        let argv = env::args().skip(1).collect::<Vec<_>>();
        let mut i = 0usize;
        while i < argv.len() {
            match argv[i].as_str() {
                "--count" => {
                    let next = argv
                        .get(i + 1)
                        .ok_or_else(|| "`--count` requires a value".to_string())?;
                    let parsed = next
                        .parse::<usize>()
                        .map_err(|_| "`--count` must be a positive integer".to_string())?;
                    if parsed == 0 {
                        return Err("`--count` must be a positive integer".to_string());
                    }
                    out.count = parsed;
                    i += 2;
                }
                "--value-sizes" => {
                    let next = argv
                        .get(i + 1)
                        .ok_or_else(|| "`--value-sizes` requires a value".to_string())?;
                    let parsed = next
                        .split(',')
                        .filter_map(|x| x.trim().parse::<usize>().ok())
                        .filter(|&n| n > 0)
                        .collect::<Vec<_>>();
                    if parsed.is_empty() {
                        return Err("`--value-sizes` must contain positive integers".to_string());
                    }
                    out.value_sizes = parsed;
                    i += 2;
                }
                "--json" => {
                    out.json = true;
                    i += 1;
                }
                "--help" | "-h" => {
                    return Err(help_text());
                }
                unknown => {
                    return Err(format!("Unknown argument: {unknown}\n\n{}", help_text()));
                }
            }
        }

        Ok(out)
    }

    fn help_text() -> String {
        [
            "Usage: cargo run -p jazz-lsm --bin mixed_bench_native -- [options]",
            "",
            "Options:",
            "  --count <n>           Number of mixed ops per scenario/value-size (default: 5000)",
            "  --value-sizes <list>  Comma-separated value sizes in bytes (default: 32,256,4096)",
            "  --json                Emit machine-readable JSON output",
        ]
        .join("\n")
    }

    fn print_table(results: &[BenchmarkResult]) {
        let headers = [
            "operation",
            "value_size",
            "count",
            "elapsed_ms",
            "ops_per_sec",
            "p95_op_ms",
            "reads",
            "read_hits",
            "read_misses",
            "writes",
            "deletes",
        ];

        let rows = results
            .iter()
            .map(|r| {
                vec![
                    r.operation.clone(),
                    r.value_size.to_string(),
                    r.count.to_string(),
                    format!("{:.3}", r.elapsed_ms),
                    format!("{:.2}", r.ops_per_sec),
                    format!("{:.4}", r.p95_op_ms),
                    r.reads.to_string(),
                    r.read_hits.to_string(),
                    r.read_misses.to_string(),
                    r.writes.to_string(),
                    r.deletes.to_string(),
                ]
            })
            .collect::<Vec<_>>();

        let widths = headers
            .iter()
            .enumerate()
            .map(|(idx, h)| {
                rows.iter()
                    .map(|r| r[idx].len())
                    .fold(h.len(), |acc, n| acc.max(n))
            })
            .collect::<Vec<_>>();

        let line = widths
            .iter()
            .map(|w| "-".repeat(*w))
            .collect::<Vec<_>>()
            .join("  ");

        let fmt_row = |row: &[String]| {
            row.iter()
                .enumerate()
                .map(|(idx, v)| format!("{:width$}", v, width = widths[idx]))
                .collect::<Vec<_>>()
                .join("  ")
        };

        println!(
            "{}",
            headers
                .iter()
                .enumerate()
                .map(|(idx, v)| format!("{:width$}", v, width = widths[idx]))
                .collect::<Vec<_>>()
                .join("  ")
        );
        println!("{line}");
        for row in &rows {
            println!("{}", fmt_row(row));
        }
    }

    pub fn run() {
        let args = match parse_args() {
            Ok(args) => args,
            Err(msg) => {
                eprintln!("{msg}");
                std::process::exit(2);
            }
        };

        let mut out = Vec::new();
        for &value_size in &args.value_sizes {
            for scenario in MIXED_SCENARIOS {
                let result = run_mixed_scenario(scenario, args.count, value_size);
                out.push(result);
            }
        }

        if args.json {
            println!(
                "{}",
                serde_json::to_string_pretty(&out).expect("serialize benchmark results")
            );
        } else {
            print_table(&out);
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn main() {
    native::run();
}
