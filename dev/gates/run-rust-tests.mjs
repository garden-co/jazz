#!/usr/bin/env node
/**
 * Bounded Rust-test launcher with a portable JSON receipt.
 *
 * Uses cargo-nextest when installed.  Plain Cargo remains a supported local
 * fallback: it preserves Cargo's exact test selection, adds an overall
 * watchdog, and records that per-test attribution requires Nextest.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { checkedOutCommit, sameTrackedSource, sourceIdentity } from "./source-identity.mjs";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const now = () => new Date().toISOString();
const run = (command, args) => {
  const value = spawnSync(command, args, { cwd: root, encoding: "utf8" });
  return value.status === 0 ? value.stdout.trim() : "unavailable";
};
const usage = () =>
  console.log(`Usage: node dev/gates/run-rust-tests.mjs [options] -- [cargo test arguments]

Options:
  --shard-index N       one-based deterministic shard index (default: 1)
  --shard-count N       number of deterministic shards (default: 1)
  --timeout-seconds N   fallback whole-command timeout (default: 900)
  --nextest-profile N   Nextest profile (default: jazz)
  --receipt PATH        JSON receipt path (default: target/test-receipts/...)
  --require-nextest     fail rather than use the Cargo fallback
  --require-nextest-test BINARY=TEST
                       require this exact selected Nextest test (repeatable)

Install the optional faster runner with: cargo install cargo-nextest --locked`);

const args = process.argv.slice(2);
let shardIndex = 1,
  shardCount = 1,
  timeoutSeconds = 900,
  nextestProfile = "jazz",
  receiptPath,
  requireNextest = false,
  requiredNextestTests = [];
let split = args.indexOf("--");
if (args.includes("--help") || args.includes("-h")) {
  usage();
  process.exit(0);
}
const options = split < 0 ? args : args.slice(0, split);
const cargoArgs = split < 0 ? [] : args.slice(split + 1);
for (let i = 0; i < options.length; i += 1) {
  const option = options[i];
  if (option === "--shard-index") shardIndex = Number(options[++i]);
  else if (option === "--shard-count") shardCount = Number(options[++i]);
  else if (option === "--timeout-seconds") timeoutSeconds = Number(options[++i]);
  else if (option === "--nextest-profile") nextestProfile = options[++i];
  else if (option === "--receipt") receiptPath = options[++i];
  else if (option === "--require-nextest") requireNextest = true;
  else if (option === "--require-nextest-test") {
    const requirement = options[++i];
    const separator = requirement?.indexOf("=") ?? -1;
    if (separator <= 0 || separator === requirement.length - 1)
      throw new Error("--require-nextest-test must be BINARY=TEST");
    requiredNextestTests.push({
      binary: requirement.slice(0, separator),
      test: requirement.slice(separator + 1),
    });
  } else {
    usage();
    throw new Error(`Unknown option: ${option}`);
  }
}
if (
  !cargoArgs.length ||
  !Number.isInteger(shardIndex) ||
  !Number.isInteger(shardCount) ||
  shardIndex < 1 ||
  shardIndex > shardCount ||
  !Number.isFinite(timeoutSeconds) ||
  timeoutSeconds <= 0 ||
  typeof nextestProfile !== "string" ||
  !nextestProfile
) {
  usage();
  throw new Error("invalid test command, shard, or timeout");
}

const startedAt = now(),
  startedMs = Date.now();
const nextestAvailable =
  spawnSync("cargo", ["nextest", "--version"], { cwd: root, stdio: "ignore" }).status === 0;
if (requireNextest && !nextestAvailable)
  throw new Error("cargo-nextest is required; install: cargo install cargo-nextest --locked");
if (requiredNextestTests.length && !nextestAvailable)
  throw new Error("--require-nextest-test requires cargo-nextest");
const useNextest = nextestAvailable;
// Nextest's default 2 MiB test-thread stack is smaller than Cargo test's and
// is insufficient for several broad async integration futures. Keep the
// runner deterministic without rewriting those tests solely for harness shape.
const rustMinStack = process.env.RUST_MIN_STACK ?? String(4 * 1024 * 1024);
if (!useNextest && shardCount !== 1)
  throw new Error("sharding requires cargo-nextest; install: cargo install cargo-nextest --locked");
if (requiredNextestTests.length) {
  // The inventory is compiled with precisely the upcoming run's Cargo selection.
  // This makes removing a target, test, or CI feature a hard failure before the
  // broad Nextest run can make an accidental coverage deletion look green.
  const inventory = spawnSync(
    "cargo",
    ["nextest", "list", ...cargoArgs, "--message-format", "json"],
    { cwd: root, encoding: "utf8", maxBuffer: 64 * 1024 * 1024 },
  );
  if (inventory.status !== 0)
    throw new Error(`Nextest inventory failed: ${inventory.stderr.trim()}`);
  let suites;
  try {
    suites = JSON.parse(inventory.stdout)["rust-suites"];
  } catch {
    throw new Error("Nextest inventory did not return JSON");
  }
  if (!suites || typeof suites !== "object" || Array.isArray(suites))
    throw new Error("Nextest inventory is missing rust-suites");
  const selected = new Set();
  for (const suite of Object.values(suites)) {
    if (!suite || typeof suite !== "object" || Array.isArray(suite)) continue;
    const binary = suite["binary-id"];
    const testcases = suite.testcases;
    if (typeof binary !== "string" || !testcases || typeof testcases !== "object") continue;
    for (const [test, testcase] of Object.entries(testcases)) {
      if (testcase?.["filter-match"]?.status === "matches") selected.add(`${binary}=${test}`);
    }
  }
  for (const { binary, test } of requiredNextestTests) {
    if (!selected.has(`${binary}=${test}`))
      throw new Error(`required Nextest test is absent from selected inventory: ${binary}=${test}`);
  }
}
const command = "cargo";
const commandArgs = useNextest
  ? [
      "nextest",
      "run",
      "--profile",
      nextestProfile,
      "--no-fail-fast",
      "--partition",
      `hash:${shardIndex}/${shardCount}`,
      ...cargoArgs,
    ]
  : ["test", ...cargoArgs];
const receipt =
  receiptPath ??
  path.join(root, "target", "test-receipts", `rust-${startedAt.replace(/[:.]/g, "-")}.json`);
fs.mkdirSync(path.dirname(receipt), { recursive: true });

let timedOut = false;
const child = spawn(command, commandArgs, {
  cwd: root,
  stdio: "inherit",
  detached: process.platform !== "win32",
  env: { ...process.env, RUST_MIN_STACK: rustMinStack },
});
const timer = setTimeout(() => {
  timedOut = true;
  try {
    if (process.platform !== "win32") process.kill(-child.pid, "SIGTERM");
    else child.kill("SIGTERM");
  } catch {}
  setTimeout(() => {
    try {
      if (process.platform !== "win32") process.kill(-child.pid, "SIGKILL");
      else child.kill("SIGKILL");
    } catch {}
  }, 5_000).unref();
}, timeoutSeconds * 1000);
const result = await new Promise((resolve) => {
  child.once("exit", (code, signal) => resolve({ code, signal }));
  child.once("error", (error) => resolve({ code: 127, signal: null, error: error.message }));
});
clearTimeout(timer);
const finishedAt = now();
const observedSource = { commit: checkedOutCommit(root), ...sourceIdentity(root) };
const baselinePath = process.env.RUST_SHADOW_SOURCE_BASELINE;
const baseline = baselinePath ? JSON.parse(fs.readFileSync(baselinePath, "utf8")) : null;
if (baseline && !sameTrackedSource(baseline, observedSource))
  throw new Error("Rust test changed the checked-out source after the shadow baseline");
const source = baseline ?? observedSource;
const data = {
  schemaVersion: 1,
  kind: "rust-test-receipt",
  startedAt,
  finishedAt,
  durationMs: Date.now() - startedMs,
  status: timedOut ? "timeout" : result.code === 0 ? "passed" : "failed",
  exitCode: result.code,
  signal: result.signal,
  spawnError: result.error ?? null,
  timedOut,
  runner: useNextest ? "cargo-nextest" : "cargo-fallback",
  nextestProfile: useNextest ? nextestProfile : null,
  perTestTimeout: useNextest ? "configured Nextest slow-timeout + one termination interval" : null,
  hangIdentification: useNextest
    ? "nextest test-name output"
    : "whole command only; install cargo-nextest for per-test attribution",
  shard: {
    strategy: useNextest ? "nextest hash" : "unavailable",
    index: shardIndex,
    count: shardCount,
  },
  command: [command, ...commandArgs],
  source: {
    commit: checkedOutCommit(root),
    ...source,
  },
  environment: {
    platform: process.platform,
    arch: process.arch,
    hostname: os.hostname(),
    rustc: run("rustc", ["--version"]),
    cargo: run("cargo", ["--version"]),
    rustcWrapper: process.env.RUSTC_WRAPPER ?? null,
    sccacheDir: process.env.SCCACHE_DIR ?? null,
    cargoTargetDir: process.env.CARGO_TARGET_DIR ?? "target",
    rustMinStack,
  },
};
fs.writeFileSync(receipt, `${JSON.stringify(data, null, 2)}\n`);
console.log(`Rust test receipt: ${receipt}`);
process.exitCode = timedOut ? 124 : (result.code ?? 1);
