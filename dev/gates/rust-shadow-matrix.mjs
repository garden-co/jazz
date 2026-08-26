#!/usr/bin/env node
/**
 * Non-required Rust throughput-shadow receipts.
 *
 * The production CI suite remains authoritative.  This launcher deliberately
 * records the exact Nextest inventory assigned to each hash shard so the
 * shadow aggregate can prove that a faster shape did not sample or drop tests.
 */
import { spawnSync } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { checkedOutCommit, sameTrackedSource, sourceIdentity } from "./source-identity.mjs";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const testArgs = [
  "--workspace",
  "--lib",
  "--bins",
  "--tests",
  "--features",
  "jazz/testing,jazz/transport-compression-zstd,jazz-server/test,jazz-cli/test",
];
const m3TestName = "node::tests::harness::m3_maintained_one_shot_differential_oracle";
const m3Features = "testing,transport-compression-zstd";
const m3Environment = {
  JAZZ_SEED: "11",
  JAZZ_DIFFERENTIAL_CHURN_DEPTHS: "10,1000",
  JAZZ_DIFFERENTIAL_STEP_COUNT: "3",
};
const now = () => new Date().toISOString();
const run = (command, args, options = {}) =>
  spawnSync(command, args, {
    cwd: root,
    encoding: "utf8",
    maxBuffer: 64 * 1024 * 1024,
    ...options,
  });
const fail = (message) => {
  throw new Error(`rust-shadow-matrix: ${message}`);
};
const plainObject = (value) => typeof value === "object" && value !== null && !Array.isArray(value);
const sourceFingerprint = (source) =>
  crypto
    .createHash("sha256")
    .update(
      ["headTree", "indexTree", "unstaged", "untracked"]
        .map((field) => `${field}\0${source[field]}\0`)
        .join(""),
    )
    .digest("hex");
const validSourceIdentity = (source) =>
  source?.dirty === false &&
  /^[0-9a-f]{40}$/.test(source.commit) &&
  /^[0-9a-f]{40}$/.test(source.headTree) &&
  /^[0-9a-f]{40}$/.test(source.indexTree) &&
  /^[0-9a-f]{64}$/.test(source.unstaged) &&
  /^[0-9a-f]{64}$/.test(source.untracked) &&
  /^[0-9a-f]{64}$/.test(source.fingerprint) &&
  source.fingerprint === sourceFingerprint(source);

function parsedInventory(output) {
  const document = JSON.parse(output);
  if (!plainObject(document) || !plainObject(document["rust-suites"]))
    fail("invalid Nextest list JSON");
  const tests = [];
  for (const suite of Object.values(document["rust-suites"])) {
    if (
      !plainObject(suite) ||
      typeof suite["binary-id"] !== "string" ||
      !plainObject(suite.testcases)
    )
      fail("invalid Nextest suite JSON");
    for (const [name, testcase] of Object.entries(suite.testcases)) {
      const status = testcase?.["filter-match"]?.status;
      if (status !== "matches" && status !== "mismatch")
        fail(`unsupported filter status: ${String(status)}`);
      if (status === "matches" && !testcase.ignored) tests.push(`${suite["binary-id"]}\0${name}`);
    }
  }
  tests.sort();
  if (new Set(tests).size !== tests.length) fail("duplicate executable test in Nextest inventory");
  return tests;
}

function inventory(partition) {
  const args = ["nextest", "list", ...testArgs, "--message-format", "json"];
  if (partition) args.push("--partition", partition);
  const result = run("cargo", args);
  if (result.status !== 0) fail(`Nextest inventory failed: ${result.stderr}`);
  return parsedInventory(result.stdout);
}

function m3OracleTestBinary() {
  // Keep this invocation semantically identical to ci-suite.yml's maintained
  // oracle gate: compile the libtest, then execute that exact binary directly.
  const result = run("cargo", [
    "test",
    "-p",
    "jazz",
    "--lib",
    "--features",
    m3Features,
    "--no-run",
    "--message-format=json",
  ]);
  if (result.status !== 0) fail(`M3 oracle compilation failed: ${result.stderr}`);
  let executable;
  for (const line of result.stdout.split("\n")) {
    try {
      const message = JSON.parse(line);
      if (
        message.reason === "compiler-artifact" &&
        message.target?.name === "jazz" &&
        typeof message.executable === "string"
      )
        executable = message.executable;
    } catch {}
  }
  if (!executable || !fs.statSync(executable).isFile()) fail("M3 oracle test binary was not built");
  return executable;
}

function phase(phases, name, action) {
  const started = Date.now();
  try {
    const value = action();
    phases.push({ name, durationMs: Date.now() - started, status: "passed" });
    return value;
  } catch (error) {
    phases.push({ name, durationMs: Date.now() - started, status: "failed", error: String(error) });
    throw error;
  }
}

function writeJson(file, value) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`);
}

function cleanSourceBaseline(argv) {
  const receipt = argv[0];
  const expectedCommit = argv[1];
  if (!receipt || !/^[0-9a-f]{40}$/.test(expectedCommit))
    fail("usage: clean-source-baseline RECEIPT EXPECTED_COMMIT");
  const source = {
    commit: checkedOutCommit(root),
    ...sourceIdentity(root),
  };
  if (!validSourceIdentity(source))
    fail("checkout contains source changes before dependency setup");
  if (source.commit !== expectedCommit)
    fail("checkout source commit does not match workflow event commit");
  writeJson(receipt, source);
}

function shadowSourceBaseline() {
  const baselinePath = process.env.RUST_SHADOW_SOURCE_BASELINE;
  if (!baselinePath) return null;
  const source = JSON.parse(fs.readFileSync(baselinePath, "utf8"));
  if (!validSourceIdentity(source)) fail("shadow source baseline is not a clean checkout");
  const observed = {
    commit: checkedOutCommit(root),
    ...sourceIdentity(root),
  };
  if (!sameTrackedSource(source, observed))
    fail("dependency setup changed the checked-out source after the shadow baseline");
  return source;
}

function shard(argv) {
  const index = Number(argv[0]);
  const count = Number(argv[1]);
  const receipt = argv[2];
  if (
    !Number.isInteger(index) ||
    !Number.isInteger(count) ||
    index < 1 ||
    index > count ||
    !receipt
  )
    fail("usage: shard INDEX COUNT RECEIPT");
  const startedAt = now();
  const phases = [];
  const partition = `hash:${index}/${count}`;
  const baseline = shadowSourceBaseline();
  const value = {
    schemaVersion: 1,
    kind: "rust-shadow-shard-receipt",
    startedAt,
    shard: { index, count, partition },
    testArgs,
    phases,
    status: "failed",
    source: baseline ?? {
      commit: checkedOutCommit(root),
      ...sourceIdentity(root),
    },
    environment: {
      platform: process.platform,
      arch: process.arch,
      hostname: os.hostname(),
      rustMinStack: process.env.RUST_MIN_STACK ?? String(4 * 1024 * 1024),
      rustcWrapper: process.env.RUSTC_WRAPPER ?? null,
      sccacheDir: process.env.SCCACHE_DIR ?? null,
    },
  };
  try {
    value.inventory = {
      all: phase(phases, "inventory-all", () => inventory()),
      selected: phase(phases, "inventory-partition", () => inventory(partition)),
    };
    const testReceipt = `${receipt}.test.json`;
    phase(phases, "partition-tests", () => {
      const result = run(
        "node",
        [
          "dev/gates/run-rust-tests.mjs",
          "--require-nextest",
          "--shard-index",
          String(index),
          "--shard-count",
          String(count),
          "--timeout-seconds",
          "780",
          "--nextest-profile",
          "jazz-ci",
          "--receipt",
          testReceipt,
          "--",
          ...testArgs,
        ],
        { stdio: "inherit" },
      );
      if (result.status !== 0) fail(`partition tests exited ${result.status}`);
    });
    value.testReceipt = JSON.parse(fs.readFileSync(testReceipt, "utf8"));
    if (
      baseline &&
      !sameTrackedSource(baseline, {
        commit: checkedOutCommit(root),
        ...sourceIdentity(root),
      })
    )
      fail("shadow execution changed the checked-out source after the baseline");
    if (index === 1) {
      // This is the same maintained seed required by CI, folded into shard 1
      // after the complete ordinary workspace partition has run.
      const testBinary = phase(phases, "m3-compile-libtest", m3OracleTestBinary);
      phase(phases, "m3-maintained-seed-11", () => {
        const result = run(
          "timeout",
          [
            "--kill-after=30s",
            "60s",
            "env",
            ...Object.entries(m3Environment).map(([name, value]) => `${name}=${value}`),
            testBinary,
            m3TestName,
            "--exact",
            "--ignored",
          ],
          {
            stdio: "inherit",
          },
        );
        if (result.status !== 0) fail(`M3 seed 11 exited ${result.status}`);
      });
      value.m3 = {
        seed: 11,
        status: "passed",
        runner: "compiled-libtest",
        testName: m3TestName,
        testArgs: ["--exact", "--ignored"],
        environment: m3Environment,
      };
    } else value.m3 = { status: "not-assigned" };
    value.status = "passed";
  } catch (error) {
    value.error = String(error);
  }
  const cache = run("sccache", ["--show-stats"]);
  value.cache = {
    status: cache.status === 0 ? "available" : "unavailable",
    statistics: cache.stdout.trim(),
  };
  value.finishedAt = now();
  value.durationMs = Date.parse(value.finishedAt) - Date.parse(startedAt);
  writeJson(receipt, value);
  if (value.status !== "passed") fail(value.error ?? "shard failed");
}

function aggregate(argv) {
  const directory = argv[0];
  const count = Number(argv[1]);
  const receipt = argv[2];
  const expectedCommit = argv[3];
  if (
    !directory ||
    !Number.isInteger(count) ||
    count < 1 ||
    !receipt ||
    !/^[0-9a-f]{40}$/.test(expectedCommit)
  )
    fail("usage: aggregate DIRECTORY COUNT RECEIPT EXPECTED_COMMIT");
  fs.mkdirSync(directory, { recursive: true });
  const aggregateReceipt = {
    schemaVersion: 1,
    kind: "rust-shadow-aggregate-receipt",
    status: "failed",
    shardCount: count,
    expectedCommit,
  };
  try {
    const files = fs
      .readdirSync(directory, { recursive: true })
      .filter((file) => /^shard-\d+\.json$/.test(path.basename(file)));
    const shards = files.map((file) =>
      JSON.parse(fs.readFileSync(path.join(directory, file), "utf8")),
    );
    if (shards.length !== count) fail(`expected ${count} shard receipts, found ${shards.length}`);
    const seenIndexes = new Set();
    const selected = new Set();
    let all;
    const expectedNextestCommand = (index) => [
      "cargo",
      "nextest",
      "run",
      "--profile",
      "jazz-ci",
      "--no-fail-fast",
      "--partition",
      `hash:${index}/${count}`,
      ...testArgs,
    ];
    const sourceFields = [
      "commit",
      "headTree",
      "indexTree",
      "unstaged",
      "untracked",
      "fingerprint",
    ];
    let source;
    for (const shardReceipt of shards) {
      if (shardReceipt?.kind !== "rust-shadow-shard-receipt" || shardReceipt.status !== "passed")
        fail("shard receipt is missing or failed");
      const { index, count: receiptCount, partition } = shardReceipt.shard ?? {};
      if (
        receiptCount !== count ||
        partition !== `hash:${index}/${count}` ||
        !Number.isInteger(index) ||
        index < 1 ||
        index > count ||
        seenIndexes.has(index)
      )
        fail("invalid or duplicate shard receipt");
      seenIndexes.add(index);
      if (
        !Array.isArray(shardReceipt.inventory?.all) ||
        !Array.isArray(shardReceipt.inventory?.selected)
      )
        fail("shard receipt has no exact inventory");
      const candidateAll = JSON.stringify(shardReceipt.inventory.all);
      if (all === undefined) all = candidateAll;
      else if (all !== candidateAll) fail("shards disagree on the exact inventory");
      for (const test of shardReceipt.inventory.selected) {
        if (selected.has(test)) fail(`test belongs to more than one shard: ${test}`);
        selected.add(test);
      }
      const ran = shardReceipt.testReceipt;
      if (
        ran?.status !== "passed" ||
        ran?.runner !== "cargo-nextest" ||
        ran?.nextestProfile !== "jazz-ci" ||
        ran?.shard?.index !== index ||
        ran?.shard?.count !== count
      )
        fail("partition test receipt does not prove the Nextest shard ran");
      if (JSON.stringify(ran.command) !== JSON.stringify(expectedNextestCommand(index)))
        fail("partition test receipt command does not match the exact shard selector");
      if (JSON.stringify(shardReceipt.testArgs) !== JSON.stringify(testArgs))
        fail("shard receipt test arguments do not match the canonical inventory");
      for (const field of sourceFields)
        if (
          typeof shardReceipt.source?.[field] !== "string" ||
          shardReceipt.source[field] !== ran.source?.[field]
        )
          fail(`partition test receipt source ${field} does not match its inventory receipt`);
      if (!validSourceIdentity(shardReceipt.source) || !validSourceIdentity(ran.source))
        fail("partition receipts must contain a clean checked-out source fingerprint");
      if (shardReceipt.source.commit !== expectedCommit)
        fail("shard source commit does not match the workflow event commit");
      if (source === undefined) source = shardReceipt.source;
      else if (sourceFields.some((field) => shardReceipt.source[field] !== source[field]))
        // This detects accidental checkout or worktree drift between matrix
        // runners; it is measurement integrity, not an adversarial attestation.
        fail("shards disagree on the checked-out source identity");
      if (ran.environment?.rustMinStack !== String(4 * 1024 * 1024))
        fail("partition test receipt did not preserve the 4 MiB Rust stack");
    }
    const expected = JSON.parse(all);
    if (selected.size !== expected.length || expected.some((test) => !selected.has(test)))
      fail("hash shards do not cover the exact executable inventory");
    const m3 = shards.filter((item) => item.m3?.seed === 11 && item.m3?.status === "passed");
    if (
      m3.length !== 1 ||
      m3[0].shard.index !== 1 ||
      m3[0].m3.runner !== "compiled-libtest" ||
      m3[0].m3.testName !== m3TestName ||
      JSON.stringify(m3[0].m3.testArgs) !== JSON.stringify(["--exact", "--ignored"]) ||
      JSON.stringify(m3[0].m3.environment) !== JSON.stringify(m3Environment)
    )
      fail("maintained M3 seed 11 was not folded into shard 1 exactly once");
    Object.assign(aggregateReceipt, {
      status: "passed",
      executableTestCount: expected.length,
      m3Shard: 1,
      shards: [...seenIndexes].sort(),
    });
  } catch (error) {
    aggregateReceipt.error = String(error);
  }
  writeJson(receipt, aggregateReceipt);
  if (aggregateReceipt.status !== "passed") fail(aggregateReceipt.error);
}

const [command, ...argv] = process.argv.slice(2);
if (command === "shard") shard(argv);
else if (command === "aggregate") aggregate(argv);
else if (command === "clean-source-baseline") cleanSourceBaseline(argv);
else fail("expected shard or aggregate command");
