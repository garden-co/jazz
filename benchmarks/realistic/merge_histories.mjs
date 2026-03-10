#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";

function fail(message) {
  console.error(message);
  process.exit(1);
}

function printHelp() {
  console.log(`Usage:
  node benchmarks/realistic/merge_histories.mjs \\
    --out bench-out/site-history/working_history.json \\
    --history benchmarks/realistic/history/bench_history.json \\
    [--history bench-out/prev-site/history.json] \\
    [--max-runs 5000]
`);
}

function parseArgs(argv) {
  const out = {
    out: "",
    histories: [],
    maxRuns: 5000,
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--") continue;
    if (arg === "--out") {
      out.out = argv[++i] ?? "";
      continue;
    }
    if (arg === "--history") {
      out.histories.push(argv[++i] ?? "");
      continue;
    }
    if (arg === "--max-runs") {
      out.maxRuns = Number(argv[++i] ?? "5000");
      continue;
    }
    if (arg === "--help" || arg === "-h") {
      printHelp();
      process.exit(0);
    }
    fail(`Unknown argument: ${arg}`);
  }

  if (!out.out) fail("--out is required");
  if (out.histories.length === 0) fail("Provide at least one --history <file>");
  if (!Number.isFinite(out.maxRuns) || out.maxRuns < 10) {
    fail("--max-runs must be a number >= 10");
  }

  return out;
}

function readHistory(file) {
  if (!file || !fs.existsSync(file)) return [];
  const raw = fs.readFileSync(file, "utf8");
  if (raw.trim().length === 0) return [];

  let parsed;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return [];
  }

  return Array.isArray(parsed?.runs) ? parsed.runs : [];
}

function generatedAtValue(run) {
  return Date.parse(run?.generated_at ?? "") || 0;
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const outPath = path.resolve(args.out);
  fs.mkdirSync(path.dirname(outPath), { recursive: true });

  const byId = new Map();
  for (const historyFile of args.histories) {
    for (const run of readHistory(path.resolve(historyFile))) {
      if (!run || typeof run !== "object" || typeof run.id !== "string") continue;
      const existing = byId.get(run.id);
      if (!existing || generatedAtValue(run) >= generatedAtValue(existing)) {
        byId.set(run.id, run);
      }
    }
  }

  const runs = [...byId.values()]
    .sort((a, b) => generatedAtValue(b) - generatedAtValue(a))
    .slice(0, args.maxRuns);

  const merged = {
    version: 1,
    updated_at: new Date().toISOString(),
    runs,
  };

  fs.writeFileSync(outPath, `${JSON.stringify(merged, null, 2)}\n`);
  console.log(`Merged histories into ${outPath} (runs=${runs.length})`);
}

main();
