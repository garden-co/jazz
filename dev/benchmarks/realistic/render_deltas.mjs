#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";

function fail(message) {
  console.error(message);
  process.exit(1);
}

function parseArgs(argv) {
  const out = {
    base: "",
    head: "",
    kind: "all",
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];

    if (arg === "--") {
      continue;
    }

    if (arg === "--base") {
      out.base = argv[++i] ?? "";
      continue;
    }
    if (arg === "--head") {
      out.head = argv[++i] ?? "";
      continue;
    }
    if (arg === "--kind") {
      out.kind = argv[++i] ?? "";
      continue;
    }
    if (arg === "--help" || arg === "-h") {
      printHelp();
      process.exit(0);
    }

    fail(`Unknown argument: ${arg}`);
  }

  if (!out.base || !out.head) {
    printHelp();
    fail("Both --base and --head are required.");
  }

  if (!["all", "native", "browser", "jazz-sim"].includes(out.kind)) {
    fail("--kind must be one of: all, native, browser, jazz-sim");
  }

  return out;
}

function printHelp() {
  console.log(`Usage:
  node dev/benchmarks/realistic/render_deltas.mjs --base <dir> --head <dir> [--kind all|native|browser|jazz-sim]

Description:
  Reads benchmark artifact bundles by discovering manifest.json files under --base and --head,
  then renders absolute values and deltas for overlapping metrics.
`);
}

function walkManifests(rootDir) {
  const manifests = [];
  const stack = [path.resolve(rootDir)];

  while (stack.length > 0) {
    const current = stack.pop();
    if (!current) continue;

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
      if (entry.isFile() && entry.name === "manifest.json") {
        try {
          const parsed = JSON.parse(fs.readFileSync(full, "utf8"));
          if (typeof parsed.kind === "string" && parsed.kind.startsWith("realistic-bench-")) {
            manifests.push({
              path: full,
              dir: path.dirname(full),
              data: parsed,
            });
          }
        } catch {
          // Ignore non-JSON or invalid manifests.
        }
      }
    }
  }

  return manifests;
}

function parseIsoOrZero(value) {
  const ts = Date.parse(value ?? "");
  return Number.isFinite(ts) ? ts : 0;
}

function selectLatestByKind(manifests, kind) {
  const filtered = manifests.filter((m) => m.data.kind === kind);
  if (filtered.length === 0) return null;

  filtered.sort(
    (a, b) => parseIsoOrZero(b.data.generated_at) - parseIsoOrZero(a.data.generated_at),
  );
  return filtered[0];
}

function isBenchResult(value) {
  return value && typeof value === "object" && typeof value.scenario_id === "string";
}

function loadNativeScenarios(manifestDir) {
  const results = [];
  const files = ["w1_interactive.json", "w4_cold_start.json"];
  for (const rel of files) {
    const full = path.join(manifestDir, rel);
    if (!fs.existsSync(full)) continue;
    try {
      const parsed = JSON.parse(fs.readFileSync(full, "utf8"));
      if (isBenchResult(parsed)) results.push(parsed);
    } catch {
      // Ignore parse failures for optional files.
    }
  }
  return results;
}

function loadBrowserScenarios(manifestDir) {
  const full = path.join(manifestDir, "realistic.json");
  if (!fs.existsSync(full)) return [];
  try {
    const parsed = JSON.parse(fs.readFileSync(full, "utf8"));
    if (!parsed || typeof parsed !== "object" || !Array.isArray(parsed.scenarios)) return [];
    return parsed.scenarios.filter((x) => isBenchResult(x));
  } catch {
    return [];
  }
}

function readJsonIfExists(file) {
  if (!fs.existsSync(file)) return null;
  try {
    return JSON.parse(fs.readFileSync(file, "utf8"));
  } catch {
    return null;
  }
}

function walkFiles(rootDir, predicate) {
  const files = [];
  const stack = [rootDir];
  while (stack.length > 0) {
    const current = stack.pop();
    if (!current) continue;

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

function passedBenchmarkIds(suiteStatus) {
  const ids = new Set();
  const benchmarks = Array.isArray(suiteStatus?.benchmarks) ? suiteStatus.benchmarks : [];
  for (const benchmark of benchmarks) {
    if (benchmark?.status === "passed" && typeof benchmark.id === "string") {
      ids.add(benchmark.id);
    }
  }
  return ids;
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

function collectJazzSimMetrics(manifestDir, manifest) {
  const map = new Map();
  const suiteStatus = readJsonIfExists(path.join(manifestDir, "suite_status.json")) ?? {};
  const passedIds = passedBenchmarkIds(suiteStatus);
  const manifestJsonlFiles = (manifest.files ?? [])
    .map((entry) => (typeof entry?.path === "string" ? path.join(manifestDir, entry.path) : null))
    .filter((file) => file && file.endsWith(".jsonl"));
  const jsonlFiles =
    manifestJsonlFiles.length > 0
      ? manifestJsonlFiles.sort()
      : walkFiles(manifestDir, (file) => file.endsWith(".jsonl"));

  for (const file of jsonlFiles) {
    const benchmarkId = jazzSimBenchmarkIdForPath(file, manifestDir);
    if (passedIds.size > 0 && benchmarkId && !passedIds.has(benchmarkId)) continue;

    for (const record of readJsonl(file)) {
      if (typeof record.scenario !== "string" || typeof record.phase !== "string") continue;
      const baseKey = `jazz-sim/${record.scenario}/${jazzSimVariant(record)}`;
      for (const [metric, value] of Object.entries(record).sort()) {
        if (JAZZ_SIM_METRIC_EXCLUDE.has(metric)) continue;
        const number = Number(value);
        if (Number.isFinite(number)) map.set(`${baseKey}/${metric}`, number);
      }
    }
  }

  return map;
}

function collectMetrics(suiteKind, scenarios) {
  const map = new Map();

  for (const scenario of scenarios) {
    const baseKey = `${suiteKind}/${scenario.scenario_id}`;
    map.set(`${baseKey}/wall_time_ms`, scenario.wall_time_ms);
    map.set(`${baseKey}/throughput_ops_per_sec`, scenario.throughput_ops_per_sec);

    const ops = scenario.operation_summaries ?? {};
    for (const opName of Object.keys(ops).sort()) {
      const summary = ops[opName];
      if (!summary || typeof summary !== "object") continue;
      map.set(`${baseKey}/op:${opName}/avg_ms`, summary.avg_ms);
      map.set(`${baseKey}/op:${opName}/p95_ms`, summary.p95_ms);
    }
  }

  return map;
}

function fmtNumber(value) {
  if (value == null || !Number.isFinite(value)) return "n/a";
  const abs = Math.abs(value);
  if (abs >= 1000) return value.toFixed(1);
  if (abs >= 10) return value.toFixed(2);
  return value.toFixed(3);
}

function classifyTrend(metric, delta) {
  if (!Number.isFinite(delta) || delta === 0) return "flat";
  const higherIsBetter = metric.includes("throughput_ops_per_sec") || metric.includes("_per_sec");
  if (higherIsBetter) return delta > 0 ? "better" : "worse";
  const lowerIsBetter = metric.includes("_ms") || metric.includes("_us");
  if (lowerIsBetter) return delta < 0 ? "better" : "worse";
  return delta > 0 ? "up" : "down";
}

function renderSuite(title, baseManifest, headManifest, baseMetrics, headMetrics) {
  const keys = [...baseMetrics.keys()].filter((k) => headMetrics.has(k)).sort();
  if (keys.length === 0) {
    console.log(`\n## ${title}\nNo overlapping metrics found.`);
    return;
  }

  console.log(`\n## ${title}`);
  console.log(
    `base: ${baseManifest.data.sha} (${baseManifest.data.generated_at}) | head: ${headManifest.data.sha} (${headManifest.data.generated_at})`,
  );
  console.log("| Metric | Base | Head | Delta | Delta % | Trend |");
  console.log("|---|---:|---:|---:|---:|---|");

  for (const key of keys) {
    const base = Number(baseMetrics.get(key));
    const head = Number(headMetrics.get(key));
    const delta = head - base;
    const deltaPct = base === 0 ? Number.NaN : (delta / base) * 100;
    const trend = classifyTrend(key, delta);
    console.log(
      `| ${key} | ${fmtNumber(base)} | ${fmtNumber(head)} | ${fmtNumber(delta)} | ${fmtNumber(deltaPct)}% | ${trend} |`,
    );
  }
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const baseManifests = walkManifests(args.base);
  const headManifests = walkManifests(args.head);

  if (baseManifests.length === 0) fail(`No benchmark manifests found under --base: ${args.base}`);
  if (headManifests.length === 0) fail(`No benchmark manifests found under --head: ${args.head}`);

  const runNative = args.kind === "all" || args.kind === "native";
  const runBrowser = args.kind === "all" || args.kind === "browser";
  const runJazzSim = args.kind === "all" || args.kind === "jazz-sim";

  console.log(`# Realistic Benchmark Delta Report`);
  console.log(`base dir: ${path.resolve(args.base)}`);
  console.log(`head dir: ${path.resolve(args.head)}`);

  if (runNative) {
    const baseNative = selectLatestByKind(baseManifests, "realistic-bench-native");
    const headNative = selectLatestByKind(headManifests, "realistic-bench-native");
    if (!baseNative || !headNative) {
      console.log(`\n## Native\nMissing native manifests in base/head inputs.`);
    } else {
      const baseMetrics = collectMetrics("native", loadNativeScenarios(baseNative.dir));
      const headMetrics = collectMetrics("native", loadNativeScenarios(headNative.dir));
      renderSuite("Native", baseNative, headNative, baseMetrics, headMetrics);
    }
  }

  if (runBrowser) {
    const baseBrowser = selectLatestByKind(baseManifests, "realistic-bench-browser");
    const headBrowser = selectLatestByKind(headManifests, "realistic-bench-browser");
    if (!baseBrowser || !headBrowser) {
      console.log(`\n## Browser\nMissing browser manifests in base/head inputs.`);
    } else {
      const baseMetrics = collectMetrics("browser", loadBrowserScenarios(baseBrowser.dir));
      const headMetrics = collectMetrics("browser", loadBrowserScenarios(headBrowser.dir));
      renderSuite("Browser", baseBrowser, headBrowser, baseMetrics, headMetrics);
    }
  }

  if (runJazzSim) {
    const baseJazzSim = selectLatestByKind(baseManifests, "realistic-bench-jazz-sim");
    const headJazzSim = selectLatestByKind(headManifests, "realistic-bench-jazz-sim");
    if (!baseJazzSim || !headJazzSim) {
      console.log(`\n## Jazz Sim\nMissing jazz-sim manifests in base/head inputs.`);
    } else {
      const baseMetrics = collectJazzSimMetrics(baseJazzSim.dir, baseJazzSim.data);
      const headMetrics = collectJazzSimMetrics(headJazzSim.dir, headJazzSim.data);
      renderSuite("Jazz Sim", baseJazzSim, headJazzSim, baseMetrics, headMetrics);
    }
  }
}

main();
