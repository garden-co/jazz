import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";
import { parse } from "yaml";
import {
  RUST_CI_FEATURES,
  RUST_WORKSPACE_TARGETS,
  assertArtifactBoundary,
  assertFullWorkspaceCoverage,
  ciPartitionJobs,
  ciPartitions,
  planFor,
  runPlan,
} from "../local-ci-equivalent.mjs";

const root = path.resolve(import.meta.dirname, "../../..");
const workflow = fs.readFileSync(path.join(root, ".github/workflows/ci-suite.yml"), "utf8");
const workflowModel = parse(workflow);

const partitionCommand = (partition) =>
  `node dev/gates/local-ci-equivalent.mjs --ci-partition ${partition}`;
const isSccacheExport = ({ name, run }) =>
  name === "Export trusted sccache configuration" &&
  run
    .trim()
    .split("\n")
    .every((line) => /^echo "SCCACHE_[A-Z0-9_]+=.*" >> "\$\{GITHUB_ENV\}"$/.test(line));
const isTurboExport = ({ name, run }) =>
  name === "Export trusted Turbo cache signing key" &&
  /^echo "TURBO_REMOTE_CACHE_SIGNATURE_KEY=.*" >> "\$\{GITHUB_ENV\}"$/.test(run.trim());

function assertCiSuiteUsesOnlySharedCorrectnessPartitions(model) {
  const expectedJobs = new Set(Object.values(ciPartitionJobs));
  for (const [partition, jobName] of Object.entries(ciPartitionJobs)) {
    const job = model.jobs?.[jobName];
    assert.ok(job, `CI omits ${jobName} for shared ${partition} partition`);
    const runSteps = (job.steps ?? []).filter(({ run }) => typeof run === "string");
    const expected = partitionCommand(partition);
    assert.equal(
      runSteps.filter(({ run }) => run === expected).length,
      1,
      `${jobName} must invoke its shared ${partition} partition exactly once`,
    );
    for (const step of runSteps) {
      if (
        step.run === expected ||
        step.run === "sccache --show-stats" ||
        isSccacheExport(step) ||
        isTurboExport(step)
      )
        continue;
      assert.fail(`${jobName} has an unshared direct run step: ${step.name ?? step.run}`);
    }
  }

  const aggregate = model.jobs?.["test-rust"];
  assert.ok(aggregate, "CI omits the Rust aggregate status job");
  const aggregateRuns = (aggregate.steps ?? []).filter(({ run }) => typeof run === "string");
  assert.deepEqual(
    aggregateRuns.map(({ run }) => run.trim()),
    ['test "${WORKSPACE_RESULT}" = success\ntest "${DIFFERENTIAL_RESULT}" = success'],
    "the Rust aggregate may check partition statuses, but must not add a correctness command",
  );

  for (const [jobName, job] of Object.entries(model.jobs ?? {})) {
    if (expectedJobs.has(jobName) || jobName === "test-rust") continue;
    const runSteps = (job.steps ?? []).filter(({ run }) => typeof run === "string");
    assert.equal(runSteps.length, 0, `${jobName} bypasses the shared CI partition source of truth`);
  }
}

test("CI invokes only shared partitions and rejects a direct correctness bypass", () => {
  assert.deepEqual(Object.keys(ciPartitionJobs).sort(), Object.keys(ciPartitions).sort());
  assert.doesNotThrow(() => assertCiSuiteUsesOnlySharedCorrectnessPartitions(workflowModel));

  const planted = structuredClone(workflowModel);
  planted.jobs.lint.steps.push({ name: "quiet bypass", run: "cargo test -p jazz" });
  assert.throws(
    () => assertCiSuiteUsesOnlySharedCorrectnessPartitions(planted),
    /unshared direct run step: quiet bypass/,
  );

  assert.deepEqual(
    planFor({ mode: "ci-equivalent" }),
    Object.values(ciPartitions).flat(),
    "local CI-equivalent must not quietly replace a CI partition with an approximation",
  );
});

test("focused mode is explicitly smaller and cannot be mistaken for CI-equivalent", () => {
  const focused = planFor({ mode: "focused" });
  const complete = planFor({ mode: "ci-equivalent" });
  assert.ok(focused.length < complete.length);
  assert.notDeepEqual(focused, complete);
  assert.throws(() => planFor({ mode: "everything" }), /unknown local CI mode/);
  assert.throws(() => planFor({ partition: "made-up" }), /unknown CI partition/);
});

test("CI-equivalent workspace check compiles every target class with the required features", () => {
  const complete = planFor({ mode: "ci-equivalent" });
  assert.doesNotThrow(() => assertFullWorkspaceCoverage(complete));
  for (const omitted of RUST_WORKSPACE_TARGETS) {
    const planted = complete.map((item) =>
      item.label === "all Rust workspace target classes"
        ? { ...item, args: item.args.filter((arg) => arg !== omitted) }
        : item,
    );
    assert.throws(
      () => assertFullWorkspaceCoverage(planted),
      new RegExp(`omits required target class ${omitted.replace("--", "\\-\\-")}`),
      `a ${omitted} compile regression must be caught before calling the gate CI-equivalent`,
    );
  }

  const missingFeature = complete.map((item) =>
    item.label === "all Rust workspace target classes"
      ? { ...item, args: item.args.filter((arg) => arg !== RUST_CI_FEATURES) }
      : item,
  );
  assert.throws(() => assertFullWorkspaceCoverage(missingFeature), /required CI feature selection/);
});

test("planted bench/test/bin/example compile failures are surfaced rather than skipped", async () => {
  const complete = planFor({ mode: "ci-equivalent" });
  for (const [target, failure] of [
    ["--benches", "bench-only compile failure"],
    ["--tests", "test-only compile failure"],
    ["--bins", "bin-only compile failure"],
    ["--examples", "example-only compile failure"],
  ]) {
    const seen = [];
    await assert.rejects(
      () =>
        runPlan(complete, async (item) => {
          seen.push(item.label);
          if (item.label === "all Rust workspace target classes" && item.args.includes(target))
            throw new Error(failure);
        }),
      new RegExp(failure),
      `${target} failure must fail the CI-equivalent gate`,
    );
    assert.ok(seen.includes("all Rust workspace target classes"));
    assert.equal(
      seen.includes("correctness-test artifacts"),
      false,
      "a target-class compile failure must stop before TS artifacts hide it",
    );
  }
});

test("the generated-artifact boundary fails before Node/browser tests can use stale outputs", async () => {
  const typescript = planFor({ partition: "typescript" });
  assert.doesNotThrow(() => assertArtifactBoundary(typescript));
  const reversed = [...typescript].reverse();
  assert.throws(() => assertArtifactBoundary(reversed), /runs tests before correctness artifacts/);

  const seen = [];
  await assert.rejects(
    () =>
      runPlan(typescript, async (item) => {
        seen.push(item.label);
        if (item.label === "correctness-test artifacts")
          throw new Error("generated artifact failure");
      }),
    /generated artifact failure/,
  );
  assert.deepEqual(seen, ["all Rust workspace target classes", "correctness-test artifacts"]);
});
