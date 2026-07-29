#!/usr/bin/env node
/**
 * Run one benchmark command and retain a smoke-shaped receipt.
 *
 * This deliberately wraps, rather than replaces, the benchmark harnesses. A
 * Criterion report, a browser artifact, and a custom JSON benchmark all keep
 * their native output; this adds the small, common run/ledger envelope used by
 * smoke.sh.
 */
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

function fail(message) { throw new Error(message); }

function parseArgs(argv) {
  const out = { jsonFiles: [], criterion: false, command: [] };
  let command = false;
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (!command && arg === "--") { command = true; continue; }
    if (command) { out.command.push(arg); continue; }
    if (arg === "--scenario") out.scenario = argv[++i];
    else if (arg === "--invocation") out.invocation = argv[++i];
    else if (arg === "--json-file") out.jsonFiles.push(argv[++i]);
    else if (arg === "--criterion") out.criterion = true;
    else if (arg === "--help" || arg === "-h") {
      console.log(`Usage: node dev/benchmarks/run-receipt.mjs \\
  --scenario <stable-name> --invocation <display-command> \\
  [--json-file <native-report>] [--criterion] -- <command> [args...]

The receipt is written under dev/benchmarks/results and its smoke-shaped
summary is appended to dev/benchmarks/SMOKE_LEDGER.md. Set JAZZ_RECEIPT_LEDGER
or JAZZ_RECEIPT_RESULT_ROOT to redirect either for local experiments.`);
      process.exit(0);
    } else fail(`Unknown argument: ${arg}`);
  }
  if (!out.scenario) fail("--scenario is required");
  if (!out.invocation) fail("--invocation is required");
  if (out.command.length === 0) fail("a command after -- is required");
  return out;
}

function safeName(value) { return value.replace(/[^A-Za-z0-9_.-]/g, "_"); }
function runId() { return process.env.JAZZ_RECEIPT_RUN_ID ?? new Date().toISOString().replace(/[-:]/g, "").replace(/\.\d{3}Z$/, "Z"); }

function git(args) {
  const result = spawnSync("git", ["-C", root, ...args], { encoding: "utf8" });
  return result.status === 0 ? result.stdout.trim() : "unknown";
}
function gitDirty() {
  return spawnSync("git", ["-C", root, "diff", "--quiet"]).status !== 0 ||
    spawnSync("git", ["-C", root, "diff", "--cached", "--quiet"]).status !== 0;
}
function rowsFromJson(value) {
  if (Array.isArray(value)) return value.flatMap(rowsFromJson);
  if (value && typeof value === "object") return [value];
  return [{ value }];
}
function parseJsonDocuments(text) {
  const trimmed = text.trim();
  if (!trimmed) return [];
  try { return rowsFromJson(JSON.parse(trimmed)); }
  catch {
    // Custom harnesses commonly interleave progress with one-line JSON. Keep
    // those native rows without attempting to reinterpret their fields.
    const lineRows = text.split(/\r?\n/).flatMap((line) => {
      try { return rowsFromJson(JSON.parse(line)); } catch { return []; }
    });
    if (lineRows.length) return lineRows;
    // `cargo run` leaves progress before a pretty-printed JSON report. Decode
    // the balanced object/array rather than requiring a harness to compact its
    // native output just for the receipt adapter.
    for (let start = 0; start < text.length; start += 1) {
      if (text[start] !== "{" && text[start] !== "[") continue;
      const open = text[start];
      const close = open === "{" ? "}" : "]";
      let depth = 0;
      let quoted = false;
      let escaped = false;
      for (let end = start; end < text.length; end += 1) {
        const char = text[end];
        if (quoted) {
          if (escaped) escaped = false;
          else if (char === "\\") escaped = true;
          else if (char === '"') quoted = false;
          continue;
        }
        if (char === '"') { quoted = true; continue; }
        if (char === open) depth += 1;
        if (char === close) depth -= 1;
        if (depth !== 0) continue;
        try { return rowsFromJson(JSON.parse(text.slice(start, end + 1))); }
        catch { break; }
      }
    }
    return [];
  }
}
function walk(dir, result = []) {
  if (!fs.existsSync(dir)) return result;
  for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
    const file = path.join(dir, entry.name);
    if (entry.isDirectory()) walk(file, result); else result.push(file);
  }
  return result;
}
function criterionRows(startedAt) {
  const criterionRoot = path.join(root, "target/criterion");
  const rows = [];
  for (const estimatesFile of walk(criterionRoot).filter((file) => file.endsWith("/new/estimates.json"))) {
    // Filesystem timestamp resolution is often coarser than Date.now(). Keep
    // a small allowance so a Criterion report written immediately before this
    // process records it is not accidentally missed.
    if (fs.statSync(estimatesFile).mtimeMs < startedAt - 1_000) continue;
    try {
      const estimates = JSON.parse(fs.readFileSync(estimatesFile, "utf8"));
      const sampleFile = path.join(path.dirname(estimatesFile), "sample.json");
      const sample = fs.existsSync(sampleFile) ? JSON.parse(fs.readFileSync(sampleFile, "utf8")) : null;
      const perIteration = sample?.times?.map((time, index) => time / sample.iters[index]).filter(Number.isFinite).sort((a, b) => a - b) ?? [];
      const samples = perIteration.length || null;
      const p95 = perIteration.length ? perIteration[Math.ceil(perIteration.length * 0.95) - 1] : null;
      const relative = path.relative(path.join(root, "target/criterion"), path.dirname(path.dirname(estimatesFile)));
      rows.push({ phase: "criterion", benchmark: relative, unit: "ns", p50_ns: estimates.median?.point_estimate ?? null, p95_ns: p95, sample_count: samples });
    } catch {
      // Criterion's report remains authoritative if a partial file exists.
    }
  }
  return rows;
}
function previousWallTime(ledger, scenario) {
  if (!fs.existsSync(ledger)) return null;
  const text = fs.readFileSync(ledger, "utf8");
  const summaries = [...text.matchAll(/### Summary\n\n/g)];
  for (const match of summaries.reverse()) {
    const end = text.indexOf("\n### Details", match.index + match[0].length);
    const lines = text.slice(match.index + match[0].length, end === -1 ? undefined : end).split("\n").filter((line) => line.startsWith("|"));
    for (const line of lines.slice(2)) {
      const cells = line.slice(1, -1).split("|").map((cell) => cell.trim());
      if (cells[0]?.replace(/\\\|/g, "|") !== scenario) continue;
      const value = Number(cells[2]?.replace(/s$/, ""));
      if (Number.isFinite(value)) return value;
    }
  }
  return null;
}
function seconds(value) { return value == null ? "-" : `${value.toFixed(3)}s`; }
function delta(current, previous) { if (previous == null) return "-"; const value = current - previous; return `${value >= 0 ? "+" : ""}${value.toFixed(3)}s`; }
function appendLedger({ ledger, scenario, status, wallSeconds, invocation, jsonl, log, resultDir }) {
  const previous = previousWallTime(ledger, scenario);
  const timestamp = new Date().toISOString().replace(/\.\d{3}Z$/, "Z");
  const relative = (file) => path.relative(root, file);
  const allRows = fs.readFileSync(jsonl, "utf8").trim().split("\n").filter(Boolean);
  const details = allRows.filter((line) => JSON.parse(line).phase !== "harness").slice(0, 18);
  const header = fs.existsSync(ledger) && fs.statSync(ledger).size > 0 ? "" : `# Benchmark Smoke Ledger

Append-only smoke-shaped benchmark receipts. Full JSONL artifacts live under
\`dev/benchmarks/results/\` and are intentionally gitignored.
`;
  const escapedScenario = scenario.replace(/\|/g, "\\|");
  const content = `${header}
---

## Run ${timestamp} - receipt

- result: \`${status === "pass" ? "pass" : "fail"}\`
- git: \`${git(["rev-parse", "--short", "HEAD"])}\`
- dirty: \`${gitDirty()}\`
- log_dir: \`${relative(path.dirname(log))}\`
- result_dir: \`${relative(resultDir)}\`

### Summary

| Scenario | Status | Wall Time | Previous | Delta | JSONL Rows | Invocation |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| ${escapedScenario} | \`${status}\` | ${seconds(wallSeconds)} | ${seconds(previous)} | ${delta(wallSeconds, previous)} | ${allRows.length} | \`${invocation.replace(/\|/g, "\\|")}\` |

### Details

#### ${scenario}

- status: \`${status}\`
- wall_time: \`${seconds(wallSeconds)}\`
- previous_wall_time: \`${seconds(previous)}\`
- delta: \`${delta(wallSeconds, previous)}\`
- log: \`${relative(log)}\`
- jsonl: \`${relative(jsonl)}\`
- invocation:

\`\`\`sh
${invocation}
\`\`\`

- excerpt:

\`\`\`jsonl
${(details.length ? details : [allRows.at(-1)]).join("\n")}
\`\`\`
`;
  fs.mkdirSync(path.dirname(ledger), { recursive: true });
  fs.appendFileSync(ledger, content);
}
function statusFor(result, output) { return result.status === 0 ? "pass" : /panicked at|thread '.*' panicked/.test(output) ? "panic" : "fail"; }

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const id = runId();
  const resultRoot = path.resolve(process.env.JAZZ_RECEIPT_RESULT_ROOT ?? path.join(root, "dev/benchmarks/results"));
  const logRoot = path.resolve(process.env.JAZZ_RECEIPT_LOG_DIR ?? path.join(root, "target/benchmark-receipts"));
  const ledger = path.resolve(process.env.JAZZ_RECEIPT_LEDGER ?? path.join(root, "dev/benchmarks/SMOKE_LEDGER.md"));
  const resultDir = path.join(resultRoot, id);
  const log = path.join(logRoot, id, `${safeName(args.scenario)}.log`);
  const jsonl = path.join(resultDir, `${safeName(args.scenario)}.jsonl`);
  fs.mkdirSync(path.dirname(log), { recursive: true }); fs.mkdirSync(resultDir, { recursive: true });
  const startedAt = Date.now(); const started = process.hrtime.bigint();
  const commandResult = spawnSync(args.command[0], args.command.slice(1), { cwd: root, encoding: "utf8", env: process.env, maxBuffer: 64 * 1024 * 1024 });
  const elapsed = Number(process.hrtime.bigint() - started) / 1e9;
  const output = `${commandResult.stdout ?? ""}${commandResult.stderr ?? ""}`;
  fs.writeFileSync(log, output);
  const rows = parseJsonDocuments(output);
  for (const file of args.jsonFiles) { const resolved = path.resolve(root, file); if (fs.existsSync(resolved)) rows.push(...parseJsonDocuments(fs.readFileSync(resolved, "utf8"))); }
  if (args.criterion) rows.push(...criterionRows(startedAt));
  const status = statusFor(commandResult, output);
  rows.push({ scenario: args.scenario, phase: "harness", status, wall_us: Math.round(elapsed * 1_000_000), wall_s: Number(elapsed.toFixed(6)), emitted_json_lines: rows.length, invocation: args.invocation, log: path.relative(root, log) });
  fs.writeFileSync(jsonl, `${rows.map((row) => JSON.stringify(row)).join("\n")}\n`);
  appendLedger({ ledger, scenario: args.scenario, status, wallSeconds: elapsed, invocation: args.invocation, jsonl, log, resultDir });
  console.log(`Receipt: ${path.relative(root, jsonl)}`); console.log(`Ledger: ${path.relative(root, ledger)}`);
  process.exit(commandResult.status ?? 1);
}
main().catch((error) => { console.error(error.stack ?? error.message); process.exit(1); });
