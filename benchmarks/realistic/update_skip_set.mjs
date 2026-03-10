#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";

function fail(message) {
  console.error(message);
  process.exit(1);
}

function printHelp() {
  console.log(`Usage:
  node benchmarks/realistic/update_skip_set.mjs \\
    --skip-set benchmarks/realistic/ci_skip_set.json \\
    --status site-input/native/suite_status.json \\
    [--status site-input/browser/suite_status.json]
`);
}

function parseArgs(argv) {
  const out = {
    skipSet: "benchmarks/realistic/ci_skip_set.json",
    statusFiles: [],
  };

  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--") continue;
    if (arg === "--skip-set") {
      out.skipSet = argv[++i] ?? "";
      continue;
    }
    if (arg === "--status") {
      out.statusFiles.push(argv[++i] ?? "");
      continue;
    }
    if (arg === "--help" || arg === "-h") {
      printHelp();
      process.exit(0);
    }
    fail(`Unknown argument: ${arg}`);
  }

  if (out.statusFiles.length === 0) fail("Provide at least one --status file");
  return out;
}

function readJsonRequired(file) {
  if (!fs.existsSync(file)) fail(`Required file not found: ${file}`);
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function readSkipSet(file) {
  if (!fs.existsSync(file)) {
    return { version: 1, updated_at: null, entries: [] };
  }
  const parsed = JSON.parse(fs.readFileSync(file, "utf8"));
  return {
    version: Number.isFinite(Number(parsed?.version)) ? Number(parsed.version) : 1,
    updated_at: typeof parsed?.updated_at === "string" ? parsed.updated_at : null,
    entries: Array.isArray(parsed?.entries) ? parsed.entries : [],
  };
}

function writeJson(file, value) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`);
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const skipSetPath = path.resolve(args.skipSet);
  const skipSet = readSkipSet(skipSetPath);
  const byId = new Map(
    skipSet.entries
      .filter((entry) => entry && typeof entry.id === "string")
      .map((entry) => [entry.id, { ...entry }]),
  );

  for (const statusPath of args.statusFiles.map((file) => path.resolve(file))) {
    const status = readJsonRequired(statusPath);
    const generatedAt =
      typeof status?.generated_at === "string" ? status.generated_at : new Date().toISOString();
    const timeoutSeconds = Number.isFinite(Number(status?.timeout_seconds))
      ? Number(status.timeout_seconds)
      : null;

    for (const benchmark of Array.isArray(status?.benchmarks) ? status.benchmarks : []) {
      if (benchmark?.status !== "timed_out" || typeof benchmark?.id !== "string") continue;
      const existing = byId.get(benchmark.id);
      if (existing) {
        existing.last_observed_at = generatedAt;
        existing.observations = (Number(existing.observations) || 1) + 1;
        if (!existing.timeout_seconds && timeoutSeconds) {
          existing.timeout_seconds = timeoutSeconds;
        }
        continue;
      }

      byId.set(benchmark.id, {
        id: benchmark.id,
        suite: typeof benchmark?.suite === "string" ? benchmark.suite : (status?.suite ?? null),
        label: typeof benchmark?.label === "string" ? benchmark.label : benchmark.id,
        reason: "timed_out_over_budget",
        timeout_seconds: timeoutSeconds,
        first_observed_at: generatedAt,
        last_observed_at: generatedAt,
        observations: 1,
      });
    }
  }

  const updated = {
    version: 1,
    updated_at: new Date().toISOString(),
    entries: [...byId.values()].sort((a, b) => a.id.localeCompare(b.id)),
  };
  writeJson(skipSetPath, updated);
  console.log(
    `Updated skip set at ${path.relative(process.cwd(), skipSetPath)} with ${updated.entries.length} entries.`,
  );
}

main();
