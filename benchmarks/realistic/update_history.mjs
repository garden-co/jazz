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
  return JSON.parse(fs.readFileSync(file, "utf8"));
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

function buildRunId(parts) {
  return parts.map((p) => String(p ?? "na")).join(":");
}

function extractNative(nativeDir) {
  if (!nativeDir) return [];
  if (!fs.existsSync(nativeDir)) return [];

  const metadata = readJsonIfExists(path.join(nativeDir, "metadata.json")) ?? {};
  const manifest = readJsonIfExists(path.join(nativeDir, "manifest.json")) ?? {};
  const scenarios = [];
  for (const file of ["w1_interactive.json", "w4_cold_start.json"]) {
    const full = path.join(nativeDir, file);
    const parsed = readJsonIfExists(full);
    if (!parsed || typeof parsed !== "object" || typeof parsed.scenario_id !== "string") {
      continue;
    }
    scenarios.push(scenarioSummary(parsed));
  }
  if (scenarios.length === 0) return [];

  const generatedAt = manifest.generated_at ?? metadata.generated_at ?? new Date().toISOString();
  const branch = toBranch(metadata.ref ?? manifest.ref);
  return [
    {
      id: buildRunId([
        "native",
        metadata.run_id,
        metadata.run_attempt,
        metadata.sha,
        metadata.profile,
      ]),
      suite: "native",
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

function extractBrowser(browserDir) {
  if (!browserDir) return [];
  if (!fs.existsSync(browserDir)) return [];

  const metadata = readJsonIfExists(path.join(browserDir, "metadata.json")) ?? {};
  const manifest = readJsonIfExists(path.join(browserDir, "manifest.json")) ?? {};
  const realistic = readJsonIfExists(path.join(browserDir, "realistic.json"));
  if (!realistic || !Array.isArray(realistic.scenarios)) return [];

  const scenarios = realistic.scenarios
    .filter((x) => x && typeof x === "object" && typeof x.scenario_id === "string")
    .map((x) => scenarioSummary(x));
  if (scenarios.length === 0) return [];

  const generatedAt =
    realistic.generated_at ??
    manifest.generated_at ??
    metadata.generated_at ??
    new Date().toISOString();
  const branch = toBranch(metadata.ref ?? manifest.ref);
  return [
    {
      id: buildRunId([
        "browser",
        metadata.run_id,
        metadata.run_attempt,
        metadata.sha,
        metadata.profile,
      ]),
      suite: "browser",
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

  const incoming = [
    ...extractNative(path.resolve(args.native || "")),
    ...extractBrowser(path.resolve(args.browser || "")),
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
