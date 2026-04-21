import fs from "node:fs";

export const DEFAULT_BENCHMARK_TIMEOUT_SECONDS = 60;
export const DEFAULT_NOISE_REPEAT_COUNT = 3;
export const ACTIVE_SKIP_MIN_OBSERVATIONS = 3;

export const NATIVE_STORAGE_ENGINES = ["rocksdb", "sqlite"];

function nativeStorageEngineLabel(storageEngine) {
  if (storageEngine === "rocksdb") return "RocksDB";
  if (storageEngine === "sqlite") return "SQLite";
  return storageEngine;
}

const NATIVE_EXAMPLE_SCENARIOS = [
  {
    id: "w1_interactive",
    label: "W1 (interactive)",
    output_path: "w1_interactive.json",
    log_path: "logs/w1_interactive.log",
    scenario_path: "benchmarks/realistic/ci/scenarios/w1_interactive.json",
    profile_path: "benchmarks/realistic/ci/profiles/s.json",
    prepare_seed: true,
  },
  {
    id: "w4_cold_start",
    label: "W4 (cold start)",
    output_path: "w4_cold_start.json",
    log_path: "logs/w4_cold_start.log",
    scenario_path: "benchmarks/realistic/ci/scenarios/w4_cold_start.json",
    profile_path: "benchmarks/realistic/ci/profiles/s.json",
    prepare_seed: true,
  },
];

const NATIVE_CRITERION_SCENARIOS = [
  {
    id: "r1_crud_sustained",
    label: "Criterion R1 CRUD sustained",
    criterion_filter_by_engine: {
      rocksdb: "realistic_phase1/crud_sustained_rocksdb",
      sqlite: "realistic_phase1/crud_sustained_sqlite",
    },
  },
  {
    id: "r1_crud_sustained_single_hop",
    label: "Criterion R1 CRUD single-hop",
    criterion_filter_by_engine: {
      rocksdb: "realistic_phase1/crud_sustained_single_hop_rocksdb",
      sqlite: "realistic_phase1/crud_sustained_single_hop_sqlite",
    },
  },
  {
    id: "r2_reads_sustained",
    label: "Criterion R2 reads sustained",
    criterion_filter_by_engine: {
      rocksdb: "realistic_phase1/reads_sustained_rocksdb",
      sqlite: "realistic_phase1/reads_sustained_sqlite",
    },
  },
  {
    id: "r2_reads_sustained_single_hop",
    label: "Criterion R2 reads single-hop",
    criterion_filter_by_engine: {
      rocksdb: "realistic_phase1/reads_sustained_single_hop_rocksdb",
      sqlite: "realistic_phase1/reads_sustained_single_hop_sqlite",
    },
  },
  {
    id: "r2_reads_with_write_churn",
    label: "Criterion R2 reads with churn",
    criterion_filter_by_engine: {
      rocksdb: "realistic_phase1/reads_sustained_with_write_churn_rocksdb",
      sqlite: "realistic_phase1/reads_sustained_with_write_churn_sqlite",
    },
  },
  {
    id: "r3_cold_load",
    label: "Criterion R3 cold-load",
    criterion_filter_by_engine: {
      rocksdb: "realistic_phase1/cold_load_rocksdb",
      sqlite: "realistic_phase1/cold_load_sqlite",
    },
  },
  {
    id: "r4_fanout_updates",
    label: "Criterion R4 fanout updates",
    criterion_filter_by_engine: {
      rocksdb: "realistic_phase1/fanout_updates_rocksdb",
      sqlite: "realistic_phase1/fanout_updates_sqlite",
    },
  },
  {
    id: "r5_permission_recursive",
    label: "Criterion R5 permission recursive",
    criterion_filter_by_engine: {
      rocksdb: "realistic_phase1/permission_recursive_rocksdb",
      sqlite: "realistic_phase1/permission_recursive_sqlite",
    },
  },
  {
    id: "r6_permission_write_heavy",
    label: "Criterion R6 permission write-heavy",
    criterion_filter_by_engine: {
      rocksdb: "realistic_phase1/permission_write_heavy_rocksdb",
      sqlite: "realistic_phase1/permission_write_heavy_sqlite",
    },
  },
  {
    id: "r7_hotspot_history",
    label: "Criterion R7 hotspot history",
    criterion_filter_by_engine: {
      rocksdb: "realistic_phase1/hotspot_history_rocksdb",
      sqlite: "realistic_phase1/hotspot_history_sqlite",
    },
  },
  {
    id: "r8_many_branches",
    label: "Criterion R8 many branches",
    criterion_filter_by_engine: {
      rocksdb: "realistic_phase1/many_branches_rocksdb",
      sqlite: "realistic_phase1/many_branches_sqlite",
    },
  },
  {
    id: "r9_subscribed_write_path",
    label: "Criterion R9 subscribed write path",
    criterion_filter_by_engine: {
      rocksdb: "realistic_phase1/subscribed_write_path_rocksdb",
      sqlite: "realistic_phase1/subscribed_write_path_sqlite",
    },
  },
];

export const NATIVE_BENCHMARKS = NATIVE_STORAGE_ENGINES.flatMap((storage_engine) => [
  ...NATIVE_EXAMPLE_SCENARIOS.map((scenario) => ({
    id: `native:${storage_engine}:${scenario.id}`,
    suite: "native",
    storage_engine,
    label: `${scenario.label} (${nativeStorageEngineLabel(storage_engine)})`,
    kind: "native-example",
    output_path: scenario.output_path,
    log_path: scenario.log_path,
    scenario_path: scenario.scenario_path,
    profile_path: scenario.profile_path,
    prepare_seed: scenario.prepare_seed,
  })),
  ...NATIVE_CRITERION_SCENARIOS.map((scenario) => ({
    id: `native-criterion:${storage_engine}:${scenario.id}`,
    suite: "native",
    storage_engine,
    label: `${scenario.label} (${nativeStorageEngineLabel(storage_engine)})`,
    kind: "criterion",
    log_path: `logs/criterion_${scenario.id}.log`,
    criterion_filter: scenario.criterion_filter_by_engine[storage_engine],
    env: {
      JAZZ_REALISTIC_VARIANT: "ci",
    },
  })),
]);

export const BROWSER_BENCHMARKS = [
  {
    id: "browser:w1",
    suite: "browser",
    label: "Browser W1 (interactive)",
    kind: "browser-scenario",
    scenario_id: "W1",
    output_path: "scenarios/w1_interactive.json",
    log_path: "logs/w1_interactive.log",
  },
  {
    id: "browser:w4",
    suite: "browser",
    label: "Browser W4 (cold start)",
    kind: "browser-scenario",
    scenario_id: "W4",
    output_path: "scenarios/w4_cold_start.json",
    log_path: "logs/w4_cold_start.log",
  },
  {
    id: "browser:b1",
    suite: "browser",
    label: "Browser B1 CRUD sustained",
    kind: "browser-scenario",
    scenario_id: "B1",
    output_path: "scenarios/b1_server_crud_sustained.json",
    log_path: "logs/b1_server_crud_sustained.log",
  },
  {
    id: "browser:b2",
    suite: "browser",
    label: "Browser B2 reads sustained",
    kind: "browser-scenario",
    scenario_id: "B2",
    output_path: "scenarios/b2_server_reads_sustained.json",
    log_path: "logs/b2_server_reads_sustained.log",
  },
  {
    id: "browser:b3",
    suite: "browser",
    label: "Browser B3 cold load large",
    kind: "browser-scenario",
    scenario_id: "B3",
    output_path: "scenarios/b3_server_cold_load_large.json",
    log_path: "logs/b3_server_cold_load_large.log",
  },
  {
    id: "browser:b4",
    suite: "browser",
    label: "Browser B4 fanout updates",
    kind: "browser-scenario",
    scenario_id: "B4",
    output_path: "scenarios/b4_server_fanout_updates.json",
    log_path: "logs/b4_server_fanout_updates.log",
  },
  {
    id: "browser:b5",
    suite: "browser",
    label: "Browser B5 permission recursive",
    kind: "browser-scenario",
    scenario_id: "B5",
    output_path: "scenarios/b5_server_permission_recursive.json",
    log_path: "logs/b5_server_permission_recursive.log",
  },
  {
    id: "browser:b6",
    suite: "browser",
    label: "Browser B6 hotspot history",
    kind: "browser-scenario",
    scenario_id: "B6",
    output_path: "scenarios/b6_server_hotspot_history.json",
    log_path: "logs/b6_server_hotspot_history.log",
  },
];

export function benchmarksForSuite(suite, options = {}) {
  if (suite === "native") {
    if (!options.storageEngine) return NATIVE_BENCHMARKS;
    return NATIVE_BENCHMARKS.filter(
      (benchmark) => benchmark.storage_engine === options.storageEngine,
    );
  }
  if (suite === "browser") return BROWSER_BENCHMARKS;
  throw new Error(`Unsupported suite: ${suite}`);
}

export function readSkipSet(file) {
  if (!file || !fs.existsSync(file)) {
    return { version: 1, updated_at: null, entries: [] };
  }
  const parsed = JSON.parse(fs.readFileSync(file, "utf8"));
  const entries = Array.isArray(parsed?.entries) ? parsed.entries : [];
  return {
    version: Number.isFinite(Number(parsed?.version)) ? Number(parsed.version) : 1,
    updated_at: typeof parsed?.updated_at === "string" ? parsed.updated_at : null,
    entries: entries.filter((entry) => entry && typeof entry.id === "string"),
  };
}

export function skipIds(skipSet) {
  return new Set(
    (skipSet?.entries ?? [])
      .filter((entry) => Number(entry?.observations ?? 1) >= ACTIVE_SKIP_MIN_OBSERVATIONS)
      .map((entry) => entry.id),
  );
}

export function repeatCountForBenchmark(benchmark, requestedCount = DEFAULT_NOISE_REPEAT_COUNT) {
  if (!benchmark || typeof benchmark !== "object") return 1;
  if (benchmark.kind === "native-example" || benchmark.kind === "browser-scenario") {
    return Math.max(1, Number(requestedCount) || DEFAULT_NOISE_REPEAT_COUNT);
  }
  return 1;
}
