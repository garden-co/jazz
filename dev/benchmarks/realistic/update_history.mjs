#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";

function fail(message) {
  console.error(message);
  process.exit(1);
}

function parseArgs(argv) {
  const out = {
    history: "dev/benchmarks/realistic/history/bench_history.json",
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
  node dev/benchmarks/realistic/update_history.mjs \
    --history dev/benchmarks/realistic/history/bench_history.json \
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

function criterionBenchmarkId(benchmark, storageEngine) {
  const groupId = benchmark?.group_id;
  const exactMap = new Map([
    ["realistic_phase1/r1_crud", "native-criterion:r1_crud"],
    ["realistic_phase1/r2_reads", "native-criterion:r2_reads"],
    ["realistic_phase1/r3_rocksdb_cold_load", "native-criterion:rocksdb:r3_rocksdb_cold_load"],
    ["realistic_phase1/r4_hot_task_history", "native-criterion:r4_hot_task_history"],
    ["realistic_phase1/r9_subscribed_write", "native-criterion:r9_subscribed_write"],
    ["realistic_phase1/r10_sync_fanout", "native-criterion:r10_sync_fanout"],
    ["realistic_phase1/r11_byte_wire_resume", "native-criterion:r11_byte_wire_resume"],
    [
      "realistic_phase1/r12_recursive_permissions",
      "native-criterion:r12_recursive_permissions",
    ],
  ]);

  if (exactMap.has(groupId)) {
    const id = exactMap.get(groupId);
    if (id.includes(":rocksdb:") || id.includes(":sqlite:")) return id;
    return `native-criterion:${storageEngine}:${id.replace("native-criterion:", "")}`;
  }
  return null;
}

function walkFiles(rootDir, predicate) {
  const files = [];
  const stack = [rootDir];
  while (stack.length > 0) {
    const current = stack.pop();
    let entries = [];
    try {
      entries = fs.readdirSync(current, { withFileTypes: true });
    } catch {
      continue;
    }
    for (const entry of entries) {
      const full = path.join(current, entry.name);
      if (entry.isDirectory()) {
        stack.push(full);
        continue;
      }
      if (entry.isFile() && predicate(full)) files.push(full);
    }
  }
  return files.sort();
}

function readJsonl(file) {
  if (!fs.existsSync(file)) return [];
  return fs
    .readFileSync(file, "utf8")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .flatMap((line) => {
      try {
        const parsed = JSON.parse(line);
        return parsed && typeof parsed === "object" ? [parsed] : [];
      } catch {
        return [];
      }
    });
}

function jazzSimBenchmarkIdForPath(file, rootDir) {
  const rel = path.relative(rootDir, file).split(path.sep).join("/");
  if (!rel.endsWith(".jsonl")) return null;
  const scenario = path.basename(rel, ".jsonl");
  if (!scenario) return null;
  if (rel.startsWith("wire_frames/")) return `jazz-sim:${scenario}:wire_frames`;
  return `jazz-sim:${scenario}`;
}

function stableToken(value) {
  return String(value ?? "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

function jazzSimVariant(record) {
  const parts = [
    record.phase,
    record.api_surface,
    record.driver,
    record.durability_tier,
    record.envelope,
    record.adversary,
    record.trace,
    record.batch_edits != null ? `batch_${record.batch_edits}` : null,
    record.scope,
  ].filter((value) => value != null && String(value).trim().length > 0);
  return parts.map(stableToken).filter(Boolean).join("/") || "result";
}

const JAZZ_SIM_METRIC_EXCLUDE = new Set([
  "batch_edits",
  "commits",
  "edits",
  "seed",
  "git_dirty",
  "final_doc_matched",
]);

function jazzSimMetrics(record) {
  const metrics = {};
  for (const [key, value] of Object.entries(record)) {
    if (JAZZ_SIM_METRIC_EXCLUDE.has(key)) continue;
    if (!Number.isFinite(Number(value))) continue;
    metrics[key] = Number(value);
  }
  return metrics;
}

function jazzSimScenarioSummary(record, benchmarkId) {
  const scenarioId = String(
    record.scenario ?? path.basename(benchmarkId ?? "unknown") ?? "unknown",
  );
  const phase = String(record.phase ?? "result");
  const metrics = jazzSimMetrics(record);
  const wallTimeMs = Number.isFinite(Number(record.elapsed_us))
    ? Number(record.elapsed_us) / 1000
    : Number.isFinite(Number(record.elapsed_ms))
      ? Number(record.elapsed_ms)
      : null;
  const throughput = firstFiniteNumber(
    record.throughput_ops_per_sec,
    record.ingest_edits_per_sec,
    record.replay_edits_per_sec,
    record.adversary_replay_edits_per_sec,
  );

  return {
    scenario_id: scenarioId,
    scenario_name: scenarioId,
    topology: phase,
    total_operations: firstFiniteNumber(record.operations, record.edits, record.commits),
    wall_time_ms: wallTimeMs,
    throughput_ops_per_sec: throughput,
    operation_summaries: {},
    extra: {
      benchmark_id: [benchmarkId, jazzSimVariant(record)].filter(Boolean).join(":"),
      phase,
      source_benchmark_id: benchmarkId,
      metrics,
    },
  };
}

function firstFiniteNumber(...values) {
  for (const value of values) {
    const number = Number(value);
    if (Number.isFinite(number)) return number;
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

function extractJazzSim(jazzSimDir) {
  if (!jazzSimDir) return [];
  if (!fs.existsSync(jazzSimDir)) return [];

  const metadata = readJsonIfExists(path.join(jazzSimDir, "metadata.json")) ?? {};
  const manifest = readJsonIfExists(path.join(jazzSimDir, "manifest.json")) ?? {};
  if (manifest?.kind !== "realistic-bench-jazz-sim") return [];

  const suiteStatus = readJsonIfExists(path.join(jazzSimDir, "suite_status.json")) ?? {};
  const passedIds = passedBenchmarkIds(suiteStatus);
  const manifestJsonlFiles = (manifest.files ?? [])
    .map((entry) => (typeof entry?.path === "string" ? path.join(jazzSimDir, entry.path) : null))
    .filter((file) => file && file.endsWith(".jsonl"));
  const jsonlFiles =
    manifestJsonlFiles.length > 0
      ? manifestJsonlFiles.sort()
      : walkFiles(jazzSimDir, (file) => file.endsWith(".jsonl"));

  const scenarios = [];
  for (const file of jsonlFiles) {
    const benchmarkId = jazzSimBenchmarkIdForPath(file, jazzSimDir);
    if (passedIds.size > 0 && benchmarkId && !passedIds.has(benchmarkId)) continue;
    for (const record of readJsonl(file)) {
      if (!record || typeof record !== "object") continue;
      if (typeof record.scenario !== "string" || typeof record.phase !== "string") continue;
      scenarios.push(jazzSimScenarioSummary(record, benchmarkId));
    }
  }
  if (scenarios.length === 0) return [];

  const generatedAt =
    suiteStatus.generated_at ??
    manifest.generated_at ??
    metadata.generated_at ??
    new Date().toISOString();
  const branch = resolveBranch(metadata, manifest, metadata.ref ?? manifest.ref);
  return [
    {
      id: buildRunId([
        "jazz-sim",
        metadata.run_id ?? manifest.run_id,
        metadata.run_attempt ?? manifest.run_attempt,
        metadata.sha ?? manifest.sha,
        metadata.profile ?? manifest.profile,
      ]),
      suite: "jazz-sim",
      storage_engine: null,
      generated_at: generatedAt,
      repository: metadata.repository ?? manifest.repository ?? null,
      run_id: metadata.run_id ?? manifest.run_id ?? null,
      run_attempt: metadata.run_attempt ?? manifest.run_attempt ?? null,
      sha: metadata.sha ?? manifest.sha ?? null,
      ref: metadata.ref ?? manifest.ref ?? null,
      branch,
      profile: metadata.profile ?? manifest.profile ?? suiteStatus.profile ?? null,
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
      const benchmarkId = criterionBenchmarkId(x, storageEngine);
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
    ...nativeDirs.flatMap((dir) => [
      ...extractNative(dir),
      ...extractNativeCriterion(dir),
      ...extractJazzSim(dir),
    ]),
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
