#[cfg(target_arch = "wasm32")]
fn main() {}

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use std::env;
    use std::path::{Path, PathBuf};
    use std::time::Instant;

    #[cfg(feature = "compare-native")]
    use bf_tree::{BfTree, Config as BfConfig, LeafInsertResult, LeafReadResult};
    #[cfg(feature = "compare-native")]
    use fjall::{Config as FjallConfig, PartitionCreateOptions, PersistMode};
    use jazz_lsm::{LsmOptions, LsmTree, RuntimeStats, StdFs, WriteDurability};
    use opfs_btree::{BTreeOptions as OpfsBTreeOptions, OpfsBTree, StdFile as OpfsStdFile};
    #[cfg(feature = "compare-native")]
    use rocksdb::{Options as RocksOptions, WriteOptions};
    use serde::Serialize;
    #[cfg(feature = "compare-native")]
    use surrealkv::{
        Durability as SurrealDurability, Mode as SurrealMode, Transaction as SurrealTransaction,
        Tree as SurrealTree, TreeBuilder as SurrealTreeBuilder,
    };
    #[cfg(feature = "compare-native")]
    use tokio::runtime::{Builder as TokioRuntimeBuilder, Runtime as TokioRuntime};

    const DEFAULT_COUNT: usize = 5_000;
    const DEFAULT_VALUE_SIZES: [usize; 3] = [32, 256, 4096];
    const DEFAULT_BASE_SEED: u64 = 0xA5A5_A5A5_0123_4567;
    const DEFAULT_ENGINES: [&str; 2] = ["jazz_lsm", "opfs_btree"];
    #[cfg(feature = "compare-native")]
    const COMPARE_ENGINES: [&str; 4] = ["bf_tree", "rocksdb", "surrealkv", "fjall"];
    #[cfg(feature = "compare-native")]
    const BF_TREE_CACHE_BYTES: usize = 32 * 1024 * 1024;
    #[cfg(feature = "compare-native")]
    const BF_TREE_MAX_VALUE_SIZE: usize = 30 * 1024;

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
        engine: String,
        operation: String,
        value_size: u32,
        count: u32,
        seed: u64,
        elapsed_ms: f64,
        ops_per_sec: f64,
        p95_op_ms: f64,
        reads: u32,
        read_hits: u32,
        read_misses: u32,
        writes: u32,
        deletes: u32,
        checksum: u64,
        runtime_stats: RuntimeStats,
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
        seed: u64,
        engines: Vec<String>,
        include_cold_read: bool,
        json: bool,
    }

    trait BenchEngine {
        fn put(&mut self, key: &[u8], value: &[u8]);
        fn get(&mut self, key: &[u8]) -> Option<Vec<u8>>;
        fn delete(&mut self, key: &[u8]);
        fn finish(&mut self);

        fn runtime_stats(&self) -> RuntimeStats {
            RuntimeStats::default()
        }
    }

    struct LsmBenchEngine {
        db: LsmTree<StdFs>,
    }

    impl BenchEngine for LsmBenchEngine {
        fn put(&mut self, key: &[u8], value: &[u8]) {
            self.db.put(key, value).expect("lsm put");
        }

        fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
            self.db.get(key).expect("lsm get")
        }

        fn delete(&mut self, key: &[u8]) {
            self.db.delete(key).expect("lsm delete");
        }

        fn finish(&mut self) {
            self.db.flush().expect("lsm flush");
        }

        fn runtime_stats(&self) -> RuntimeStats {
            self.db.runtime_stats()
        }
    }

    struct OpfsBTreeBenchEngine {
        db: OpfsBTree<OpfsStdFile>,
    }

    impl BenchEngine for OpfsBTreeBenchEngine {
        fn put(&mut self, key: &[u8], value: &[u8]) {
            self.db.put(key, value).expect("opfs-btree put");
        }

        fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
            self.db.get(key).expect("opfs-btree get")
        }

        fn delete(&mut self, key: &[u8]) {
            self.db.delete(key).expect("opfs-btree delete");
        }

        fn finish(&mut self) {
            self.db.checkpoint().expect("opfs-btree checkpoint");
        }
    }

    #[cfg(feature = "compare-native")]
    struct BfTreeBenchEngine {
        tree: BfTree,
        read_buffer: Vec<u8>,
    }

    #[cfg(feature = "compare-native")]
    impl BenchEngine for BfTreeBenchEngine {
        fn put(&mut self, key: &[u8], value: &[u8]) {
            let result = self.tree.insert(key, value);
            assert!(
                matches!(result, LeafInsertResult::Success),
                "bf-tree insert failed: {:?}",
                result
            );
        }

        fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
            match self.tree.read(key, &mut self.read_buffer) {
                LeafReadResult::Found(len) => Some(self.read_buffer[..(len as usize)].to_vec()),
                LeafReadResult::Deleted | LeafReadResult::NotFound => None,
                LeafReadResult::InvalidKey => panic!("bf-tree invalid key"),
            }
        }

        fn delete(&mut self, key: &[u8]) {
            self.tree.delete(key);
        }

        fn finish(&mut self) {
            self.tree.snapshot();
        }
    }

    #[cfg(feature = "compare-native")]
    struct RocksDbBenchEngine {
        db: rocksdb::DB,
        write_options: WriteOptions,
    }

    #[cfg(feature = "compare-native")]
    impl BenchEngine for RocksDbBenchEngine {
        fn put(&mut self, key: &[u8], value: &[u8]) {
            self.db
                .put_opt(key, value, &self.write_options)
                .expect("rocksdb put");
        }

        fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
            self.db
                .get_pinned(key)
                .expect("rocksdb get")
                .map(|v| v.as_ref().to_vec())
        }

        fn delete(&mut self, key: &[u8]) {
            self.db
                .delete_opt(key, &self.write_options)
                .expect("rocksdb delete");
        }

        fn finish(&mut self) {
            self.db.flush().expect("rocksdb flush");
        }
    }

    #[cfg(feature = "compare-native")]
    struct FjallBenchEngine {
        keyspace: fjall::Keyspace,
        partition: fjall::PartitionHandle,
    }

    #[cfg(feature = "compare-native")]
    impl BenchEngine for FjallBenchEngine {
        fn put(&mut self, key: &[u8], value: &[u8]) {
            self.partition.insert(key, value).expect("fjall insert");
        }

        fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
            self.partition
                .get(key)
                .expect("fjall get")
                .map(|v| v.to_vec())
        }

        fn delete(&mut self, key: &[u8]) {
            self.partition.remove(key).expect("fjall remove");
        }

        fn finish(&mut self) {
            self.keyspace
                .persist(PersistMode::SyncData)
                .expect("fjall persist");
        }
    }

    #[cfg(feature = "compare-native")]
    struct SurrealKvBenchEngine {
        tree: SurrealTree,
        runtime: TokioRuntime,
        write_txn: Option<SurrealTransaction>,
        read_txn: Option<SurrealTransaction>,
    }

    #[cfg(feature = "compare-native")]
    impl SurrealKvBenchEngine {
        fn ensure_write_txn(&mut self) -> &mut SurrealTransaction {
            if self.write_txn.is_none() {
                let txn = {
                    let _guard = self.runtime.enter();
                    self.tree
                        .begin()
                        .expect("begin surrealkv write txn")
                        .with_durability(SurrealDurability::Eventual)
                };
                self.write_txn = Some(txn);
            }
            self.write_txn.as_mut().expect("surrealkv write txn")
        }

        fn ensure_read_txn(&mut self) -> &mut SurrealTransaction {
            if self.read_txn.is_none() {
                let txn = {
                    let _guard = self.runtime.enter();
                    self.tree
                        .begin_with_mode(SurrealMode::ReadOnly)
                        .expect("begin surrealkv read txn")
                };
                self.read_txn = Some(txn);
            }
            self.read_txn.as_mut().expect("surrealkv read txn")
        }
    }

    #[cfg(feature = "compare-native")]
    impl BenchEngine for SurrealKvBenchEngine {
        fn put(&mut self, key: &[u8], value: &[u8]) {
            self.read_txn = None;
            self.ensure_write_txn()
                .set(key, value)
                .expect("surrealkv set");
        }

        fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
            if self.write_txn.is_some() {
                self.finish();
            }
            self.ensure_read_txn().get(key).expect("surrealkv get")
        }

        fn delete(&mut self, key: &[u8]) {
            self.read_txn = None;
            self.ensure_write_txn()
                .delete(key)
                .expect("surrealkv delete");
        }

        fn finish(&mut self) {
            self.read_txn = None;
            if let Some(mut txn) = self.write_txn.take() {
                self.runtime
                    .block_on(async { txn.commit().await })
                    .expect("surrealkv commit");
            }
        }
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

    fn open_lsm_db(path: &Path) -> LsmTree<StdFs> {
        let fs = StdFs::new(path).expect("open std fs");
        LsmTree::open(fs, bench_options(), Vec::new()).expect("open lsm tree")
    }

    fn open_opfs_btree(path: &Path) -> OpfsBTree<OpfsStdFile> {
        let file = OpfsStdFile::open(path.join("opfs-btree.data")).expect("open opfs-btree file");
        let options = OpfsBTreeOptions {
            page_size: 16 * 1024,
            cache_bytes: 8 * 1024 * 1024,
            overflow_threshold: 8 * 1024,
        };
        OpfsBTree::open(file, options).expect("open opfs-btree")
    }

    #[cfg(feature = "compare-native")]
    fn open_bf_tree(path: &Path, max_value_size: usize) -> BfTreeBenchEngine {
        let mut config = BfConfig::new(path.join("bftree.index"), BF_TREE_CACHE_BYTES);
        config.cb_min_record_size(4);

        let target_record = max_value_size + 64;
        let mut leaf_page_size = 16 * 1024;
        while leaf_page_size < target_record * 2 {
            leaf_page_size *= 2;
        }
        let max_record_size = target_record.min((leaf_page_size / 2).saturating_sub(128));
        config.leaf_page_size(leaf_page_size);
        config.cb_max_record_size(max_record_size);

        let tree = BfTree::with_config(config, None).expect("open bf-tree");
        BfTreeBenchEngine {
            tree,
            read_buffer: vec![0u8; max_value_size.saturating_add(1024)],
        }
    }

    #[cfg(feature = "compare-native")]
    fn open_rocksdb(path: &Path) -> RocksDbBenchEngine {
        let mut options = RocksOptions::default();
        options.create_if_missing(true);
        options.set_use_fsync(false);

        let mut write_options = WriteOptions::default();
        write_options.set_sync(false);
        write_options.disable_wal(true);

        let db_path = path.join("rocksdb");
        let db = rocksdb::DB::open(&options, db_path).expect("open rocksdb");
        RocksDbBenchEngine { db, write_options }
    }

    #[cfg(feature = "compare-native")]
    fn open_fjall(path: &Path) -> FjallBenchEngine {
        let keyspace = FjallConfig::new(path.join("fjall"))
            .open()
            .expect("open fjall keyspace");
        let partition = keyspace
            .open_partition("bench", PartitionCreateOptions::default())
            .expect("open fjall partition");
        FjallBenchEngine {
            keyspace,
            partition,
        }
    }

    #[cfg(feature = "compare-native")]
    fn open_surrealkv(path: &Path) -> SurrealKvBenchEngine {
        let runtime = TokioRuntimeBuilder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("build surrealkv tokio runtime");
        let tree = {
            let _guard = runtime.enter();
            SurrealTreeBuilder::new()
                .with_path(path.join("surrealkv"))
                .with_level_count(4)
                .with_max_memtable_size(256 * 1024 * 1024)
                .without_compression()
                .build()
                .expect("open surrealkv")
        };

        SurrealKvBenchEngine {
            tree,
            runtime,
            write_txn: None,
            read_txn: None,
        }
    }

    fn supported_engines() -> Vec<&'static str> {
        #[cfg(feature = "compare-native")]
        {
            let mut out = DEFAULT_ENGINES.to_vec();
            out.extend(COMPARE_ENGINES);
            return out;
        }

        #[cfg(not(feature = "compare-native"))]
        {
            DEFAULT_ENGINES.to_vec()
        }
    }

    fn parse_engines(raw: &str) -> Result<Vec<String>, String> {
        let supported = supported_engines();
        if raw.trim().eq_ignore_ascii_case("all") {
            return Ok(supported.iter().map(|e| (*e).to_string()).collect());
        }

        let mut out = Vec::new();
        for token in raw.split(',') {
            let name = token.trim();
            if !supported.contains(&name) {
                return Err(format!(
                    "`--engines` contains unknown engine `{}` (supported: {})",
                    name,
                    supported.join(",")
                ));
            }
            if !out.iter().any(|existing| existing == name) {
                out.push(name.to_string());
            }
        }
        if out.is_empty() {
            return Err(format!(
                "`--engines` must include at least one of: {}",
                supported.join(",")
            ));
        }
        Ok(out)
    }

    fn engine_supports_value_size(engine_name: &str, value_size: usize) -> bool {
        let _ = engine_name;
        #[cfg(feature = "compare-native")]
        {
            if engine_name == "bf_tree" && value_size > BF_TREE_MAX_VALUE_SIZE {
                return false;
            }
        }
        let _ = value_size;
        true
    }

    fn open_engine(engine_name: &str, path: &Path, value_size: usize) -> Box<dyn BenchEngine> {
        let _ = value_size;
        match engine_name {
            "jazz_lsm" => Box::new(LsmBenchEngine {
                db: open_lsm_db(path),
            }),
            "opfs_btree" => Box::new(OpfsBTreeBenchEngine {
                db: open_opfs_btree(path),
            }),
            #[cfg(feature = "compare-native")]
            "bf_tree" => Box::new(open_bf_tree(path, value_size)),
            #[cfg(feature = "compare-native")]
            "rocksdb" => Box::new(open_rocksdb(path)),
            #[cfg(feature = "compare-native")]
            "surrealkv" => Box::new(open_surrealkv(path)),
            #[cfg(feature = "compare-native")]
            "fjall" => Box::new(open_fjall(path)),
            _ => panic!("unsupported engine: {engine_name}"),
        }
    }

    fn temp_db_dir(engine: &str, label: &str, value_size: usize, count: usize) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let pid = std::process::id();
        std::env::temp_dir().join(format!(
            "jazz-lsm-{engine}-{label}-{value_size}-{count}-{pid}-{nanos}"
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

    fn preload(db: &mut dyn BenchEngine, key_space: usize, value_size: usize) {
        for i in 0..key_space {
            let k = key(i);
            let v = value(value_size, (i % 251) as u8);
            db.put(&k, &v);
        }
        db.finish();
    }

    fn derive_seed_for_label(base_seed: u64, label: &str, value_size: usize) -> u64 {
        const MAX_JS_SAFE_INT: u64 = 9_007_199_254_740_991;
        let mut h = 0xcbf2_9ce4_8422_2325u64 ^ base_seed ^ (value_size as u64);
        for &b in label.as_bytes() {
            h ^= b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        let mut derived =
            (h ^ ((value_size as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15))) % MAX_JS_SAFE_INT;
        if derived == 0 {
            derived = 1;
        }
        derived
    }

    fn derive_seed(base_seed: u64, scenario: MixedScenario, value_size: usize) -> u64 {
        derive_seed_for_label(base_seed, scenario.name, value_size)
    }

    fn parse_seed(raw: &str) -> Result<u64, String> {
        let trimmed = raw.trim();
        if let Some(hex) = trimmed
            .strip_prefix("0x")
            .or_else(|| trimmed.strip_prefix("0X"))
        {
            return u64::from_str_radix(hex, 16).map_err(|_| {
                "`--seed` must be a valid u64 (decimal or 0x-prefixed hex)".to_string()
            });
        }
        trimmed
            .parse::<u64>()
            .map_err(|_| "`--seed` must be a valid u64 (decimal or 0x-prefixed hex)".to_string())
    }

    fn run_mixed_scenario(
        engine_name: &str,
        scenario: MixedScenario,
        count: usize,
        value_size: usize,
        base_seed: u64,
    ) -> BenchmarkResult {
        let dir = temp_db_dir(engine_name, scenario.name, value_size, count);
        std::fs::create_dir_all(&dir).expect("create temp db directory");
        let mut db = open_engine(engine_name, &dir, value_size);

        // Preload to ensure mixed workloads stress both reads and updates/deletes.
        let initial_key_space = count.max(1);
        preload(db.as_mut(), initial_key_space, value_size);

        let seed = derive_seed(base_seed, scenario, value_size);
        let mut rng = DeterministicRng::new(seed);
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
                    let maybe = db.get(&k);
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
                    db.put(&k, &v);
                }
                OpChoice::Delete => {
                    deletes += 1;
                    let idx = rng.next_usize(key_space.max(1));
                    let k = key(idx);
                    db.delete(&k);
                    checksum = checksum.wrapping_add(idx as u64);
                }
            }

            op_latencies_ns.push(op_start.elapsed().as_nanos() as u64);
        }

        db.finish();
        let runtime_stats = db.runtime_stats();
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
        let elapsed = total_start.elapsed();
        let elapsed_s = elapsed.as_secs_f64();

        BenchmarkResult {
            engine: engine_name.to_string(),
            operation: scenario.name.to_string(),
            value_size: value_size as u32,
            count: count as u32,
            seed,
            elapsed_ms: elapsed_s * 1000.0,
            ops_per_sec: if elapsed_s > 0.0 {
                count as f64 / elapsed_s
            } else {
                0.0
            },
            p95_op_ms: percentile_ms(&mut op_latencies_ns, 0.95),
            reads,
            read_hits,
            read_misses,
            writes,
            deletes,
            checksum,
            runtime_stats,
        }
    }

    fn run_cold_read_scenario(
        engine_name: &str,
        operation: &str,
        count: usize,
        value_size: usize,
        base_seed: u64,
        random_order: bool,
    ) -> BenchmarkResult {
        let dir = temp_db_dir(engine_name, operation, value_size, count);
        std::fs::create_dir_all(&dir).expect("create temp db directory");

        // Prefill + persist.
        let mut prefill_db = open_engine(engine_name, &dir, value_size);
        let key_space = count.max(1);
        preload(prefill_db.as_mut(), key_space, value_size);
        drop(prefill_db);

        let seed = derive_seed_for_label(base_seed, operation, value_size);
        let order = if random_order {
            Some({
                let mut out: Vec<usize> = (0..key_space).collect();
                let mut state: u64 = 0xD1B54A32D192ED03 ^ seed;
                for i in (1..key_space).rev() {
                    state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                    let j = (state as usize) % (i + 1);
                    out.swap(i, j);
                }
                out
            })
        } else {
            None
        };

        let mut db = open_engine(engine_name, &dir, value_size);
        let mut op_latencies_ns = Vec::with_capacity(count);
        let mut checksum = 0u64;
        let total_start = Instant::now();

        match &order {
            Some(order) => {
                for &i in order {
                    let op_start = Instant::now();
                    let k = key(i);
                    let v = db.get(&k).expect("cold random read key present");
                    checksum = checksum.wrapping_add(v[0] as u64);
                    op_latencies_ns.push(op_start.elapsed().as_nanos() as u64);
                }
            }
            None => {
                for i in 0..count {
                    let op_start = Instant::now();
                    let k = key(i);
                    let v = db.get(&k).expect("cold seq read key present");
                    checksum = checksum.wrapping_add(v[0] as u64);
                    op_latencies_ns.push(op_start.elapsed().as_nanos() as u64);
                }
            }
        }

        let elapsed = total_start.elapsed();
        let elapsed_s = elapsed.as_secs_f64();
        let runtime_stats = db.runtime_stats();
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);

        BenchmarkResult {
            engine: engine_name.to_string(),
            operation: operation.to_string(),
            value_size: value_size as u32,
            count: count as u32,
            seed,
            elapsed_ms: elapsed_s * 1000.0,
            ops_per_sec: if elapsed_s > 0.0 {
                count as f64 / elapsed_s
            } else {
                0.0
            },
            p95_op_ms: percentile_ms(&mut op_latencies_ns, 0.95),
            reads: count as u32,
            read_hits: count as u32,
            read_misses: 0,
            writes: 0,
            deletes: 0,
            checksum,
            runtime_stats,
        }
    }

    fn parse_args() -> Result<Args, String> {
        let mut out = Args {
            count: DEFAULT_COUNT,
            value_sizes: DEFAULT_VALUE_SIZES.to_vec(),
            seed: DEFAULT_BASE_SEED,
            engines: DEFAULT_ENGINES.iter().map(|e| e.to_string()).collect(),
            include_cold_read: false,
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
                "--seed" => {
                    let next = argv
                        .get(i + 1)
                        .ok_or_else(|| "`--seed` requires a value".to_string())?;
                    out.seed = parse_seed(next)?;
                    i += 2;
                }
                "--engines" => {
                    let next = argv
                        .get(i + 1)
                        .ok_or_else(|| "`--engines` requires a value".to_string())?;
                    out.engines = parse_engines(next)?;
                    i += 2;
                }
                "--json" => {
                    out.json = true;
                    i += 1;
                }
                "--include-cold-read" => {
                    out.include_cold_read = true;
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
            "  --seed <u64>          Base deterministic seed (decimal or 0x hex)",
            "  --engines <list>      Comma-separated engines (or `all`) (default: jazz_lsm,opfs_btree)",
            "  --include-cold-read   Include cold_seq_read and cold_random_read scenarios",
            "  --json                Emit machine-readable JSON output",
        ]
        .join("\n")
    }

    fn print_table(results: &[BenchmarkResult]) {
        let headers = [
            "engine",
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
                    r.engine.clone(),
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
                for engine_name in &args.engines {
                    if !engine_supports_value_size(engine_name, value_size) {
                        continue;
                    }
                    let result = run_mixed_scenario(
                        engine_name,
                        scenario,
                        args.count,
                        value_size,
                        args.seed,
                    );
                    out.push(result);
                }
            }
            if args.include_cold_read {
                for engine_name in &args.engines {
                    if !engine_supports_value_size(engine_name, value_size) {
                        continue;
                    }
                    out.push(run_cold_read_scenario(
                        engine_name,
                        "cold_seq_read",
                        args.count,
                        value_size,
                        args.seed,
                        false,
                    ));
                    out.push(run_cold_read_scenario(
                        engine_name,
                        "cold_random_read",
                        args.count,
                        value_size,
                        args.seed,
                        true,
                    ));
                }
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
