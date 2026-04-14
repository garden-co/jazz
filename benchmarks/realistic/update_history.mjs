#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";

function fail(message) {
  console.error(message);
  process.exit(1);
}

function parseArgs(argv) {
  const out = {
    history: "benchmarks/realistic/history/bench_history.json",
    native: "",
    browser: "",
    maxRuns: 1000,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--") continue;
    if (arg === "--history") {
      out.history = argv[++i] ?? "";
      continue;
    }
    if (arg === "--native") {
      out.native = argv[++i] ?? "";
      continue;
    }
    if (arg === "--browser") {
      out.browser = argv[++i] ?? "";
      continue;
    }
    if (arg === "--max-runs") {
      out.maxRuns = Number(argv[++i] ?? "1000");
      continue;
    }
    if (arg === "--help" || arg === "-h") {
      printHelp();
      process.exit(0);
    }
    fail(`Unknown argument: ${arg}`);
  }

  if (!out.native && !out.browser) {
    fail("Provide at least one input: --native <dir> and/or --browser <dir>");
  }
  if (!Number.isFinite(out.maxRuns) || out.maxRuns < 10) {
    fail("--max-runs must be a number >= 10");
  }

  return out;
}

function printHelp() {
  console.log(`Usage:
  node benchmarks/realistic/update_history.mjs \
    --history benchmarks/realistic/history/bench_history.json \
    [--native site-input/native] \
    [--browser site-input/browser] \
    [--max-runs 1000]
`);
}

function readJsonIfExists(file) {
  if (!file || !fs.existsSync(file)) return null;
  const raw = fs.readFileSync(file, "utf8");
  if (raw.trim().length === 0) return null;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

function readJsonRequired(file) {
  if (!fs.existsSync(file)) fail(`Required file not found: ${file}`);
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function toBranch(ref) {
  if (typeof ref !== "string") return "unknown";
  if (ref.startsWith("refs/heads/")) return ref.slice("refs/heads/".length);
  return ref;
}

function firstNonEmptyString(...values) {
  for (const value of values) {
    if (typeof value === "string" && value.trim().length > 0) {
      return value;
    }
  }
  return null;
}

function resolveBranch(metadata, manifest, fallbackRef) {
  return firstNonEmptyString(metadata?.branch, manifest?.branch) ?? toBranch(fallbackRef);
}

function normalizeStorageEngine(value) {
  if (typeof value !== "string") return null;
  const normalized = value.trim().toLowerCase();
  return normalized.length > 0 ? normalized : null;
}

function resolveStorageEngine(...values) {
  for (const value of values) {
    const normalized = normalizeStorageEngine(value);
    if (normalized) return normalized;
  }
  return null;
}

function hasBenchmarkFiles(dir) {
  if (!dir || !fs.existsSync(dir)) return false;
  for (const file of [
    "metadata.json",
    "manifest.json",
    "suite_status.json",
    "realistic.json",
    "criterion_realistic_phase1.json",
  ]) {
    if (fs.existsSync(path.join(dir, file))) return true;
  }
  return false;
}

function artifactDirs(rootDir) {
  if (!rootDir || !fs.existsSync(rootDir)) return [];
  const dirs = [];
  if (hasBenchmarkFiles(rootDir)) dirs.push(rootDir);
  for (const entry of fs.readdirSync(rootDir, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    const dir = path.join(rootDir, entry.name);
    if (hasBenchmarkFiles(dir)) dirs.push(dir);
  }
  return dirs;
}

function storageEngineFromDir(dir) {
  return resolveStorageEngine(path.basename(dir));
}

function scenarioSummary(scenario) {
  return {
    scenario_id: scenario.scenario_id,
    scenario_name: scenario.scenario_name,
    topology: scenario.topology,
    total_operations: scenario.total_operations,
    wall_time_ms: scenario.wall_time_ms,
    throughput_ops_per_sec: scenario.throughput_ops_per_sec,
    operation_summaries: scenario.operation_summaries ?? {},
    extra: scenario.extra ?? {},
  };
}

function criterionScenarioSummary(benchmark) {
  const metrics = benchmark?.metrics ?? {};
  const wallPoint = Number(metrics.mean_ms);
  const wallLow = Number(metrics.mean_ci_low_ms);
  const wallHigh = Number(metrics.mean_ci_high_ms);
  const throughputPoint = Number(metrics.elems_per_sec ?? metrics.iter_per_sec);
  const throughputLow = Number(metrics.elems_per_sec_ci_low ?? metrics.iter_per_sec_ci_low);
  const throughputHigh = Number(metrics.elems_per_sec_ci_high ?? metrics.iter_per_sec_ci_high);
  const relativeHalfWidthPct = (point, low, high) => {
    if (!Number.isFinite(point) || point === 0) return null;
    const lowerWidth = Number.isFinite(low) ? Math.abs(point - low) / Math.abs(point) : null;
    const upperWidth = Number.isFinite(high) ? Math.abs(high - point) / Math.abs(point) : null;
    const width = Math.max(lowerWidth ?? 0, upperWidth ?? 0);
    return Number.isFinite(width) ? width * 100 : null;
  };
  return {
    scenario_id: benchmark.scenario_id ?? benchmark.full_id ?? "unknown",
    scenario_name: benchmark.scenario_name ?? benchmark.benchmark_id ?? benchmark.full_id ?? null,
    topology: benchmark.group_id ?? "criterion",
    total_operations: benchmark.throughput_elements ?? null,
    wall_time_ms: metrics.mean_ms ?? null,
    throughput_ops_per_sec: metrics.elems_per_sec ?? metrics.iter_per_sec ?? null,
    operation_summaries: {},
    extra: {
      full_id: benchmark.full_id ?? null,
      benchmark_id: benchmark.benchmark_id ?? null,
      throughput_elements: benchmark.throughput_elements ?? null,
      iter_per_sec: metrics.iter_per_sec ?? null,
      iter_per_sec_ci_low: metrics.iter_per_sec_ci_low ?? null,
      iter_per_sec_ci_high: metrics.iter_per_sec_ci_high ?? null,
      elems_per_sec_ci_low: metrics.elems_per_sec_ci_low ?? null,
      elems_per_sec_ci_high: metrics.elems_per_sec_ci_high ?? null,
      noise: {
        source: "criterion_confidence_interval",
        estimator: "mean",
        metrics: {
          wall_time_ms: {
            ci_low: Number.isFinite(wallLow) ? wallLow : null,
            ci_high: Number.isFinite(wallHigh) ? wallHigh : null,
            relative_half_width_pct: relativeHalfWidthPct(wallPoint, wallLow, wallHigh),
          },
          throughput_ops_per_sec: {
            ci_low: Number.isFinite(throughputLow) ? throughputLow : null,
            ci_high: Number.isFinite(throughputHigh) ? throughputHigh : null,
            relative_half_width_pct: relativeHalfWidthPct(
              throughputPoint,
              throughputLow,
              throughputHigh,
            ),
          },
        },
      },
    },
  };
}

function buildRunId(parts) {
  return parts.map((p) => String(p ?? "na")).join(":");
}

function passedBenchmarkIds(status) {
  return new Set(
    (status?.benchmarks ?? [])
      .filter((benchmark) => benchmark?.status === "passed" && typeof benchmark?.id === "string")
      .map((benchmark) => benchmark.id),
  );
}

function nativeBenchmarkIdForFile(file, storageEngine) {
  if (file === "w1_interactive.json") return `native:${storageEngine}:w1_interactive`;
  if (file === "w4_cold_start.json") return `native:${storageEngine}:w4_cold_start`;
  return null;
}

function criterionBenchmarkId(benchmark) {
  const groupId = benchmark?.group_id;
  const exactMap = new Map([
    ["realistic_phase1/crud_sustained_rocksdb", "native-criterion:rocksdb:r1_crud_sustained"],
    ["realistic_phase1/crud_sustained_sqlite", "native-criterion:sqlite:r1_crud_sustained"],
    [
      "realistic_phase1/crud_sustained_single_hop_rocksdb",
      "native-criterion:rocksdb:r1_crud_sustained_single_hop",
    ],
    [
      "realistic_phase1/crud_sustained_single_hop_sqlite",
      "native-criterion:sqlite:r1_crud_sustained_single_hop",
    ],
    ["realistic_phase1/reads_sustained_rocksdb", "native-criterion:rocksdb:r2_reads_sustained"],
    ["realistic_phase1/reads_sustained_sqlite", "native-criterion:sqlite:r2_reads_sustained"],
    [
      "realistic_phase1/reads_sustained_single_hop_rocksdb",
      "native-criterion:rocksdb:r2_reads_sustained_single_hop",
    ],
    [
      "realistic_phase1/reads_sustained_single_hop_sqlite",
      "native-criterion:sqlite:r2_reads_sustained_single_hop",
    ],
    [
      "realistic_phase1/reads_sustained_with_write_churn_rocksdb",
      "native-criterion:rocksdb:r2_reads_with_write_churn",
    ],
    [
      "realistic_phase1/reads_sustained_with_write_churn_sqlite",
      "native-criterion:sqlite:r2_reads_with_write_churn",
    ],
    ["realistic_phase1/cold_load_rocksdb", "native-criterion:rocksdb:r3_cold_load"],
    ["realistic_phase1/cold_load_sqlite", "native-criterion:sqlite:r3_cold_load"],
    ["realistic_phase1/fanout_updates_rocksdb", "native-criterion:rocksdb:r4_fanout_updates"],
    ["realistic_phase1/fanout_updates_sqlite", "native-criterion:sqlite:r4_fanout_updates"],
    [
      "realistic_phase1/permission_recursive_rocksdb",
      "native-criterion:rocksdb:r5_permission_recursive",
    ],
    [
      "realistic_phase1/permission_recursive_sqlite",
      "native-criterion:sqlite:r5_permission_recursive",
    ],
    [
      "realistic_phase1/permission_write_heavy_rocksdb",
      "native-criterion:rocksdb:r6_permission_write_heavy",
    ],
    [
      "realistic_phase1/permission_write_heavy_sqlite",
      "native-criterion:sqlite:r6_permission_write_heavy",
    ],
    ["realistic_phase1/hotspot_history_rocksdb", "native-criterion:rocksdb:r7_hotspot_history"],
    ["realistic_phase1/hotspot_history_sqlite", "native-criterion:sqlite:r7_hotspot_history"],
    [
      "realistic_phase1/subscribed_write_path_rocksdb",
      "native-criterion:rocksdb:r9_subscribed_write_path",
    ],
    [
      "realistic_phase1/subscribed_write_path_sqlite",
      "native-criterion:sqlite:r9_subscribed_write_path",
    ],
  ]);

  if (exactMap.has(groupId)) {
    return exactMap.get(groupId);
  }
  if (typeof groupId === "string" && groupId.startsWith("realistic_phase1/many_branches_rocksdb")) {
    return "native-criterion:rocksdb:r8_many_branches";
  }
  if (typeof groupId === "string" && groupId.startsWith("realistic_phase1/many_branches_sqlite")) {
    return "native-criterion:sqlite:r8_many_branches";
  }
  return null;
}

function extractNative(nativeDir) {
  if (!nativeDir) return [];
  if (!fs.existsSync(nativeDir)) return [];

  const metadata = readJsonIfExists(path.join(nativeDir, "metadata.json")) ?? {};
  const manifest = readJsonIfExists(path.join(nativeDir, "manifest.json")) ?? {};
  const suiteStatus = readJsonIfExists(path.join(nativeDir, "suite_status.json")) ?? {};
  const storageEngine = resolveStorageEngine(
    metadata?.storage_engine,
    manifest?.storage_engine,
    storageEngineFromDir(nativeDir),
  );
  if (!storageEngine) return [];
  const passedIds = passedBenchmarkIds(suiteStatus);
  const scenarios = [];
  for (const file of ["w1_interactive.json", "w4_cold_start.json"]) {
    const benchmarkId = nativeBenchmarkIdForFile(file, storageEngine);
    if (passedIds.size > 0 && benchmarkId && !passedIds.has(benchmarkId)) {
      continue;
    }
    const full = path.join(nativeDir, file);
    const parsed = readJsonIfExists(full);
    if (!parsed || typeof parsed !== "object" || typeof parsed.scenario_id !== "string") {
      continue;
    }
    scenarios.push(scenarioSummary(parsed));
  }
  if (scenarios.length === 0) return [];

  const generatedAt = manifest.generated_at ?? metadata.generated_at ?? new Date().toISOString();
  const branch = resolveBranch(metadata, manifest, metadata.ref ?? manifest.ref);
  return [
    {
      id: buildRunId([
        "native",
        storageEngine,
        metadata.run_id,
        metadata.run_attempt,
        metadata.sha,
        metadata.profile,
      ]),
      suite: "native",
      storage_engine: storageEngine,
      generated_at: generatedAt,
      repository: metadata.repository ?? manifest.repository ?? null,
      run_id: metadata.run_id ?? manifest.run_id ?? null,
      run_attempt: metadata.run_attempt ?? manifest.run_attempt ?? null,
      sha: metadata.sha ?? manifest.sha ?? null,
      ref: metadata.ref ?? manifest.ref ?? null,
      branch,
      profile: metadata.profile ?? manifest.profile ?? null,
      runner_name: metadata.runner_name ?? null,
      runner_os: metadata.runner_os ?? null,
      runner_arch: metadata.runner_arch ?? null,
      scenarios,
    },
  ];
}

function extractNativeCriterion(nativeDir) {
  if (!nativeDir) return [];
  if (!fs.existsSync(nativeDir)) return [];

  const metadata = readJsonIfExists(path.join(nativeDir, "metadata.json")) ?? {};
  const manifest = readJsonIfExists(path.join(nativeDir, "manifest.json")) ?? {};
  const suiteStatus = readJsonIfExists(path.join(nativeDir, "suite_status.json")) ?? {};
  const passedIds = passedBenchmarkIds(suiteStatus);
  const criterion = readJsonIfExists(path.join(nativeDir, "criterion_realistic_phase1.json"));
  if (!criterion || !Array.isArray(criterion.benchmarks)) return [];
  const storageEngine = resolveStorageEngine(
    metadata?.storage_engine,
    manifest?.storage_engine,
    criterion?.storage_engine,
    storageEngineFromDir(nativeDir),
  );
  if (!storageEngine) return [];

  const scenarios = criterion.benchmarks
    .filter((x) => x && typeof x === "object" && typeof x.full_id === "string")
    .filter((x) => {
      if (passedIds.size === 0) return true;
      const benchmarkId = criterionBenchmarkId(x);
      return benchmarkId ? passedIds.has(benchmarkId) : true;
    })
    .map((x) => criterionScenarioSummary(x));
  if (scenarios.length === 0) return [];

  const generatedAt =
    criterion.generated_at ??
    manifest.generated_at ??
    metadata.generated_at ??
    new Date().toISOString();
  const branch = resolveBranch(metadata, manifest, metadata.ref ?? manifest.ref);
  return [
    {
      id: buildRunId([
        "native-criterion",
        storageEngine,
        metadata.run_id,
        metadata.run_attempt,
        metadata.sha,
        metadata.profile,
      ]),
      suite: "native-criterion",
      storage_engine: storageEngine,
      generated_at: generatedAt,
      repository: metadata.repository ?? manifest.repository ?? null,
      run_id: metadata.run_id ?? manifest.run_id ?? null,
      run_attempt: metadata.run_attempt ?? manifest.run_attempt ?? null,
      sha: metadata.sha ?? manifest.sha ?? criterion?.metadata?.sha ?? null,
      ref: metadata.ref ?? manifest.ref ?? null,
      branch,
      profile: metadata.profile ?? manifest.profile ?? null,
      runner_name: metadata.runner_name ?? null,
      runner_os: metadata.runner_os ?? null,
      runner_arch: metadata.runner_arch ?? null,
      scenarios,
    },
  ];
}

function extractBrowser(browserDir) {
  if (!browserDir) return [];
  if (!fs.existsSync(browserDir)) return [];

  const metadata = readJsonIfExists(path.join(browserDir, "metadata.json")) ?? {};
  const manifest = readJsonIfExists(path.join(browserDir, "manifest.json")) ?? {};
  const realistic = readJsonIfExists(path.join(browserDir, "realistic.json"));
  if (!realistic || !Array.isArray(realistic.scenarios)) return [];
  const storageEngine =
    resolveStorageEngine(
      metadata?.storage_engine,
      manifest?.storage_engine,
      realistic?.storage_engine,
    ) ?? "opfs-btree";

  const scenarios = realistic.scenarios
    .filter((x) => x && typeof x === "object" && typeof x.scenario_id === "string")
    .map((x) => scenarioSummary(x));
  if (scenarios.length === 0) return [];

  const generatedAt =
    realistic.generated_at ??
    manifest.generated_at ??
    metadata.generated_at ??
    new Date().toISOString();
  const branch = resolveBranch(metadata, manifest, metadata.ref ?? manifest.ref);
  return [
    {
      id: buildRunId([
        "browser",
        storageEngine,
        metadata.run_id,
        metadata.run_attempt,
        metadata.sha,
        metadata.profile,
      ]),
      suite: "browser",
      storage_engine: storageEngine,
      generated_at: generatedAt,
      repository: metadata.repository ?? manifest.repository ?? null,
      run_id: metadata.run_id ?? manifest.run_id ?? null,
      run_attempt: metadata.run_attempt ?? manifest.run_attempt ?? null,
      sha: metadata.sha ?? manifest.sha ?? null,
      ref: metadata.ref ?? manifest.ref ?? null,
      branch,
      profile: metadata.profile ?? manifest.profile ?? realistic.profile ?? null,
      runner_name: metadata.runner_name ?? null,
      runner_os: metadata.runner_os ?? null,
      runner_arch: metadata.runner_arch ?? null,
      scenarios,
    },
  ];
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const historyPath = path.resolve(args.history);
  const historyDir = path.dirname(historyPath);
  fs.mkdirSync(historyDir, { recursive: true });

  const existing = readJsonIfExists(historyPath) ?? {
    version: 1,
    updated_at: null,
    runs: [],
  };
  if (!Array.isArray(existing.runs)) existing.runs = [];

  const nativeDirs = artifactDirs(path.resolve(args.native || ""));
  const browserDirs = artifactDirs(path.resolve(args.browser || ""));
  const incoming = [
    ...nativeDirs.flatMap((dir) => [...extractNative(dir), ...extractNativeCriterion(dir)]),
    ...browserDirs.flatMap((dir) => extractBrowser(dir)),
  ];
  if (incoming.length === 0) {
    console.log("No benchmark inputs found; history unchanged.");
    return;
  }

  const byId = new Map(existing.runs.map((run) => [run.id, run]));
  for (const run of incoming) {
    byId.set(run.id, run);
  }

  const runs = [...byId.values()]
    .sort((a, b) => Date.parse(b.generated_at ?? "") - Date.parse(a.generated_at ?? ""))
    .slice(0, args.maxRuns);

  const output = {
    version: 1,
    updated_at: new Date().toISOString(),
    runs,
  };

  fs.writeFileSync(historyPath, `${JSON.stringify(output, null, 2)}\n`);
  console.log(`Updated history: ${historyPath} (runs=${runs.length})`);
}

main();
