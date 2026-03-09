#!/usr/bin/env node

import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execSync } from "node:child_process";

function fail(message) {
  console.error(message);
  process.exit(1);
}

function printHelp() {
  console.log(`Usage:
  node benchmarks/realistic/export_criterion.mjs \
    [--criterion-root target/criterion] \
    [--scenarios-dir benchmarks/realistic/scenarios] \
    [--prefix realistic_phase1/] \
    [--out bench-out/native/criterion_realistic_phase1.json] \
    [--summary-md bench-out/native/criterion_realistic_phase1.md]
`);
}

function parseArgs(argv) {
  const out = {
    criterionRoot: "target/criterion",
    scenariosDir: "benchmarks/realistic/scenarios",
    prefix: "realistic_phase1/",
    out: "bench-out/native/criterion_realistic_phase1.json",
    summaryMd: "bench-out/native/criterion_realistic_phase1.md",
    allowEmpty: false,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--") continue;
    if (arg === "--criterion-root") {
      out.criterionRoot = argv[++i] ?? "";
      continue;
    }
    if (arg === "--scenarios-dir") {
      out.scenariosDir = argv[++i] ?? "";
      continue;
    }
    if (arg === "--prefix") {
      out.prefix = argv[++i] ?? "";
      continue;
    }
    if (arg === "--out") {
      out.out = argv[++i] ?? "";
      continue;
    }
    if (arg === "--summary-md") {
      out.summaryMd = argv[++i] ?? "";
      continue;
    }
    if (arg === "--allow-empty") {
      out.allowEmpty = true;
      continue;
    }
    if (arg === "--help" || arg === "-h") {
      printHelp();
      process.exit(0);
    }
    fail(`Unknown argument: ${arg}`);
  }

  if (!out.criterionRoot) fail("--criterion-root is required");
  if (!out.out) fail("--out is required");
  return out;
}

function readJson(file) {
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function safeGit(cmd) {
  try {
    return execSync(cmd, { stdio: ["ignore", "pipe", "ignore"] })
      .toString("utf8")
      .trim();
  } catch {
    return null;
  }
}

function walkFiles(root) {
  const out = [];
  if (!fs.existsSync(root)) return out;
  const queue = [root];
  while (queue.length > 0) {
    const current = queue.pop();
    if (!current) continue;
    const entries = fs.readdirSync(current, { withFileTypes: true });
    for (const entry of entries) {
      const full = path.join(current, entry.name);
      if (entry.isDirectory()) {
        queue.push(full);
      } else if (entry.isFile()) {
        out.push(full);
      }
    }
  }
  return out;
}

function loadScenarios(dir) {
  const map = new Map();
  if (!fs.existsSync(dir)) return map;
  const files = fs
    .readdirSync(dir)
    .filter((f) => f.endsWith(".json"))
    .map((f) => path.join(dir, f));
  for (const file of files) {
    try {
      const parsed = readJson(file);
      if (parsed && typeof parsed.id === "string") {
        map.set(parsed.id.toLowerCase(), {
          id: parsed.id,
          name: typeof parsed.name === "string" ? parsed.name : null,
          file: path.relative(process.cwd(), file),
        });
      }
    } catch {
      // Ignore malformed scenario files in export.
    }
  }
  return map;
}

function extractScenarioKey(valueStr) {
  if (typeof valueStr !== "string") return null;
  const m = valueStr.match(/^(r[0-9]+[a-z]?)/i);
  return m ? m[1].toLowerCase() : null;
}

function estimateMetrics(benchmarkJson, estimatesJson) {
  const meanNs = estimatesJson?.mean?.point_estimate;
  const ciLowerNs = estimatesJson?.mean?.confidence_interval?.lower_bound;
  const ciUpperNs = estimatesJson?.mean?.confidence_interval?.upper_bound;
  if (!Number.isFinite(meanNs) || meanNs <= 0) return null;

  const elements = benchmarkJson?.throughput?.Elements;
  const iterPerSec = 1e9 / meanNs;
  const iterPerSecCiLow = Number.isFinite(ciUpperNs) && ciUpperNs > 0 ? 1e9 / ciUpperNs : null;
  const iterPerSecCiHigh = Number.isFinite(ciLowerNs) && ciLowerNs > 0 ? 1e9 / ciLowerNs : null;

  const elemsPerSec = Number.isFinite(elements) && elements > 0 ? iterPerSec * elements : null;
  const elemsPerSecCiLow =
    Number.isFinite(elements) && elements > 0 && Number.isFinite(iterPerSecCiLow)
      ? iterPerSecCiLow * elements
      : null;
  const elemsPerSecCiHigh =
    Number.isFinite(elements) && elements > 0 && Number.isFinite(iterPerSecCiHigh)
      ? iterPerSecCiHigh * elements
      : null;

  return {
    mean_ns: meanNs,
    mean_ms: meanNs / 1e6,
    mean_ci_low_ms: Number.isFinite(ciLowerNs) ? ciLowerNs / 1e6 : null,
    mean_ci_high_ms: Number.isFinite(ciUpperNs) ? ciUpperNs / 1e6 : null,
    iter_per_sec: iterPerSec,
    iter_per_sec_ci_low: iterPerSecCiLow,
    iter_per_sec_ci_high: iterPerSecCiHigh,
    elems_per_sec: elemsPerSec,
    elems_per_sec_ci_low: elemsPerSecCiLow,
    elems_per_sec_ci_high: elemsPerSecCiHigh,
  };
}

function formatElems(value) {
  if (!Number.isFinite(value)) return "-";
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(2)}M`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(2)}K`;
  return value.toFixed(2);
}

function formatMs(value) {
  if (!Number.isFinite(value)) return "-";
  return value.toFixed(3);
}

function buildSummaryMarkdown(exportJson) {
  const lines = [];
  lines.push("# Criterion Realistic Phase1 Export");
  lines.push("");
  lines.push(`Generated: ${exportJson.generated_at}`);
  lines.push(`SHA: ${exportJson.metadata.sha ?? "unknown"}`);
  lines.push(`Branch: ${exportJson.metadata.branch ?? "unknown"}`);
  lines.push("");
  if (exportJson.benchmarks.length === 0) {
    lines.push("_No Criterion benchmarks completed within the configured CI budget._");
    lines.push("");
    return `${lines.join("\n")}\n`;
  }
  lines.push("| Benchmark | elems/s | mean ms | Scenario |");
  lines.push("|---|---:|---:|---|");
  for (const bench of exportJson.benchmarks) {
    const scenarioLabel = bench.scenario_name
      ? `${bench.scenario_id} (${bench.scenario_name})`
      : (bench.scenario_id ?? "-");
    lines.push(
      `| \`${bench.full_id}\` | ${formatElems(bench.metrics.elems_per_sec)} | ${formatMs(bench.metrics.mean_ms)} | ${scenarioLabel} |`,
    );
  }
  lines.push("");
  return `${lines.join("\n")}\n`;
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const criterionRoot = path.resolve(args.criterionRoot);
  const scenariosDir = path.resolve(args.scenariosDir);
  const outFile = path.resolve(args.out);
  const summaryFile = path.resolve(args.summaryMd);

  if (!fs.existsSync(criterionRoot) && !args.allowEmpty) {
    fail(`Criterion root does not exist: ${criterionRoot}`);
  }

  const scenarioMap = loadScenarios(scenariosDir);
  const files = fs.existsSync(criterionRoot)
    ? walkFiles(criterionRoot).filter((file) =>
        file.endsWith(`${path.sep}new${path.sep}benchmark.json`),
      )
    : [];

  const benchmarks = [];
  for (const benchmarkFile of files) {
    const benchmarkJson = readJson(benchmarkFile);
    if (
      !benchmarkJson ||
      typeof benchmarkJson.full_id !== "string" ||
      !benchmarkJson.full_id.startsWith(args.prefix)
    ) {
      continue;
    }
    const estimatesFile = path.join(path.dirname(benchmarkFile), "estimates.json");
    if (!fs.existsSync(estimatesFile)) continue;

    const estimatesJson = readJson(estimatesFile);
    const metrics = estimateMetrics(benchmarkJson, estimatesJson);
    if (!metrics) continue;

    const scenarioKey = extractScenarioKey(benchmarkJson.value_str);
    const scenario = scenarioKey ? (scenarioMap.get(scenarioKey) ?? null) : null;

    benchmarks.push({
      full_id: benchmarkJson.full_id,
      group_id: benchmarkJson.group_id ?? null,
      benchmark_id: benchmarkJson.value_str ?? null,
      throughput_elements: benchmarkJson?.throughput?.Elements ?? null,
      scenario_id: scenario?.id ?? (scenarioKey ? scenarioKey.toUpperCase() : null),
      scenario_name: scenario?.name ?? null,
      scenario_file: scenario?.file ?? null,
      metrics,
    });
  }

  benchmarks.sort((a, b) => a.full_id.localeCompare(b.full_id));
  if (benchmarks.length === 0 && !args.allowEmpty) {
    fail(`No Criterion benchmark data found under ${criterionRoot} with prefix '${args.prefix}'`);
  }

  const exportJson = {
    version: 1,
    generated_at: new Date().toISOString(),
    metadata: {
      criterion_root: path.relative(process.cwd(), criterionRoot),
      prefix: args.prefix,
      sha: process.env.GITHUB_SHA ?? safeGit("git rev-parse HEAD"),
      branch: process.env.GITHUB_REF_NAME ?? safeGit("git rev-parse --abbrev-ref HEAD"),
      host: os.hostname(),
    },
    benchmarks,
  };

  fs.mkdirSync(path.dirname(outFile), { recursive: true });
  fs.writeFileSync(outFile, `${JSON.stringify(exportJson, null, 2)}\n`);

  fs.mkdirSync(path.dirname(summaryFile), { recursive: true });
  fs.writeFileSync(summaryFile, buildSummaryMarkdown(exportJson));

  console.log(`Exported ${benchmarks.length} benchmarks to ${outFile} and ${summaryFile}`);
}

main();
