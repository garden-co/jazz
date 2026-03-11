import fs from "node:fs";

export const DEFAULT_BENCHMARK_TIMEOUT_SECONDS = 60;
export const DEFAULT_NOISE_REPEAT_COUNT = 3;

export const NATIVE_BENCHMARKS = [
  {
    id: "native:w1_interactive",
    suite: "native",
    label: "W1 (interactive)",
    kind: "native-example",
    output_path: "w1_interactive.json",
    log_path: "logs/w1_interactive.log",
    scenario_path: "benchmarks/realistic/ci/scenarios/w1_interactive.json",
    profile_path: "benchmarks/realistic/ci/profiles/s.json",
    prepare_seed: true,
  },
  {
    id: "native:w4_cold_start",
    suite: "native",
    label: "W4 (cold start)",
    kind: "native-example",
    output_path: "w4_cold_start.json",
    log_path: "logs/w4_cold_start.log",
    scenario_path: "benchmarks/realistic/ci/scenarios/w4_cold_start.json",
    profile_path: "benchmarks/realistic/ci/profiles/s.json",
    prepare_seed: true,
  },
  {
    id: "native-criterion:r1_crud_sustained",
    suite: "native",
    label: "Criterion R1 CRUD sustained",
    kind: "criterion",
    log_path: "logs/criterion_r1_crud_sustained.log",
    criterion_filter: "realistic_phase1/crud_sustained/r1_s",
    env: {
      JAZZ_REALISTIC_VARIANT: "ci",
    },
  },
  {
    id: "native-criterion:r1_crud_sustained_single_hop",
    suite: "native",
    label: "Criterion R1 CRUD single-hop",
    kind: "criterion",
    log_path: "logs/criterion_r1_crud_sustained_single_hop.log",
    criterion_filter: "realistic_phase1/crud_sustained_single_hop",
    env: {
      JAZZ_REALISTIC_VARIANT: "ci",
    },
  },
  {
    id: "native-criterion:r2_reads_sustained",
    suite: "native",
    label: "Criterion R2 reads sustained",
    kind: "criterion",
    log_path: "logs/criterion_r2_reads_sustained.log",
    criterion_filter: "realistic_phase1/reads_sustained/r2_s",
    env: {
      JAZZ_REALISTIC_VARIANT: "ci",
    },
  },
  {
    id: "native-criterion:r2_reads_sustained_single_hop",
    suite: "native",
    label: "Criterion R2 reads single-hop",
    kind: "criterion",
    log_path: "logs/criterion_r2_reads_sustained_single_hop.log",
    criterion_filter: "realistic_phase1/reads_sustained_single_hop",
    env: {
      JAZZ_REALISTIC_VARIANT: "ci",
    },
  },
  {
    id: "native-criterion:r2_reads_with_write_churn",
    suite: "native",
    label: "Criterion R2 reads with churn",
    kind: "criterion",
    log_path: "logs/criterion_r2_reads_with_write_churn.log",
    criterion_filter: "realistic_phase1/reads_sustained_with_write_churn",
    env: {
      JAZZ_REALISTIC_VARIANT: "ci",
    },
  },
  {
    id: "native-criterion:r3_cold_load_fjall",
    suite: "native",
    label: "Criterion R3 cold-load Fjall",
    kind: "criterion",
    log_path: "logs/criterion_r3_cold_load_fjall.log",
    criterion_filter: "realistic_phase1/cold_load_fjall",
    env: {
      JAZZ_REALISTIC_VARIANT: "ci",
    },
  },
  {
    id: "native-criterion:r4_fanout_updates",
    suite: "native",
    label: "Criterion R4 fanout updates",
    kind: "criterion",
    log_path: "logs/criterion_r4_fanout_updates.log",
    criterion_filter: "realistic_phase1/fanout_updates",
    env: {
      JAZZ_REALISTIC_VARIANT: "ci",
    },
  },
  {
    id: "native-criterion:r5_permission_recursive",
    suite: "native",
    label: "Criterion R5 permission recursive",
    kind: "criterion",
    log_path: "logs/criterion_r5_permission_recursive.log",
    criterion_filter: "realistic_phase1/permission_recursive",
    env: {
      JAZZ_REALISTIC_VARIANT: "ci",
    },
  },
  {
    id: "native-criterion:r6_permission_write_heavy",
    suite: "native",
    label: "Criterion R6 permission write-heavy",
    kind: "criterion",
    log_path: "logs/criterion_r6_permission_write_heavy.log",
    criterion_filter: "realistic_phase1/permission_write_heavy",
    env: {
      JAZZ_REALISTIC_VARIANT: "ci",
    },
  },
  {
    id: "native-criterion:r7_hotspot_history",
    suite: "native",
    label: "Criterion R7 hotspot history",
    kind: "criterion",
    log_path: "logs/criterion_r7_hotspot_history.log",
    criterion_filter: "realistic_phase1/hotspot_history",
    env: {
      JAZZ_REALISTIC_VARIANT: "ci",
    },
  },
];

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

export function benchmarksForSuite(suite) {
  if (suite === "native") return NATIVE_BENCHMARKS;
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
  return new Set((skipSet?.entries ?? []).map((entry) => entry.id));
}

export function repeatCountForBenchmark(benchmark, requestedCount = DEFAULT_NOISE_REPEAT_COUNT) {
  if (!benchmark || typeof benchmark !== "object") return 1;
  if (benchmark.kind === "native-example" || benchmark.kind === "browser-scenario") {
    return Math.max(1, Number(requestedCount) || DEFAULT_NOISE_REPEAT_COUNT);
  }
  return 1;
}
