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

  if (!["all", "native", "browser"].includes(out.kind)) {
    fail("--kind must be one of: all, native, browser");
  }

  return out;
}

function printHelp() {
  console.log(`Usage:
  node benchmarks/realistic/render_deltas.mjs --base <dir> --head <dir> [--kind all|native|browser]

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
  const higherIsBetter = metric.includes("throughput_ops_per_sec");
  if (higherIsBetter) return delta > 0 ? "better" : "worse";
  const lowerIsBetter = metric.includes("_ms");
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
}

main();
