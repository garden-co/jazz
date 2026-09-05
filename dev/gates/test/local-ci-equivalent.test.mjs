import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import test from "node:test";
import { isDeepStrictEqual } from "node:util";
import { parse } from "yaml";
import {
  RUST_CI_FEATURES,
  RUST_WORKSPACE_TARGETS,
  ALLOWED_INHERITED_CI_JAZZ_ENV,
  assertArtifactBoundary,
  assertFullWorkspaceCoverage,
  ciPartitionJobs,
  ciPartitions,
  commandEnvironment,
  exactCiEnvironment,
  planFor,
  runPlan,
} from "../local-ci-equivalent.mjs";

const root = path.resolve(import.meta.dirname, "../../..");
const workflow = fs.readFileSync(path.join(root, ".github/workflows/ci-suite.yml"), "utf8");
const workflowModel = parse(workflow);

const partitionCommand = (partition) =>
  `node dev/gates/local-ci-equivalent.mjs --ci-partition ${partition}`;
const trustedSccacheCondition =
  "inputs.trusted-cache && (inputs.sccache-write && vars.SCCACHE_TRUSTED_WRITER_AWS_ROLE_ARN != '' || !inputs.sccache-write && vars.SCCACHE_PR_READER_AWS_ROLE_ARN != '')";
const trustedTurboCondition = "inputs.trusted-cache";
const sccacheExportStep = Object.freeze({
  name: "Export trusted sccache configuration",
  if: trustedSccacheCondition,
  env: {
    CACHE_BUCKET: "${{ vars.SCCACHE_BUCKET }}",
    CACHE_REGION: "${{ vars.SCCACHE_REGION }}",
  },
  run:
    [
      'echo "SCCACHE_BUCKET=${CACHE_BUCKET}" >> "${GITHUB_ENV}"',
      'echo "SCCACHE_REGION=${CACHE_REGION}" >> "${GITHUB_ENV}"',
      'echo "SCCACHE_S3_USE_SSL=true" >> "${GITHUB_ENV}"',
      'echo "SCCACHE_S3_KEY_PREFIX=jazz-ci/v1/production/blacksmith-v1" >> "${GITHUB_ENV}"',
    ].join("\n") + "\n",
});
const turboExportStep = Object.freeze({
  name: "Export trusted Turbo cache signing key",
  if: trustedTurboCondition,
  env: { CACHE_SIGNATURE_KEY: "${{ secrets.TURBO_REMOTE_CACHE_SIGNATURE_KEY }}" },
  run: 'echo "TURBO_REMOTE_CACHE_SIGNATURE_KEY=${CACHE_SIGNATURE_KEY}" >> "${GITHUB_ENV}"',
});
const sccacheStatsStep = Object.freeze({
  name: "Report sccache statistics",
  if: "always()",
  run: "sccache --show-stats",
});

function isExactStep(actual, expected) {
  return (
    Object.keys(actual).length === Object.keys(expected).length &&
    Object.keys(expected).every(
      (key) => Object.hasOwn(actual, key) && isDeepStrictEqual(actual[key], expected[key]),
    )
  );
}

const isKnownAdminStep = (step) =>
  isExactStep(step, sccacheExportStep) ||
  isExactStep(step, turboExportStep) ||
  isExactStep(step, sccacheStatsStep);

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
      if (step.run === expected || isKnownAdminStep(step)) continue;
      assert.fail(`${jobName} has an unshared direct run step: ${step.name ?? step.run}`);
    }
  }

  const aggregate = model.jobs?.["test-rust"];
  assert.ok(aggregate, "CI omits the Rust aggregate status job");
  const aggregateRuns = (aggregate.steps ?? []).filter(({ run }) => typeof run === "string");
  assert.deepEqual(
    aggregateRuns.map(({ run }) => run.trim()),
    [
      'test "${WORKSPACE_RESULT}" = success\n' +
        'test "${DIFFERENTIAL_RESULT}" = success\n' +
        'test "${STORAGE_COMPAT_RESULT}" = success',
    ],
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

  const sccacheExploit = structuredClone(workflowModel);
  sccacheExploit.jobs.lint.steps.find(({ name }) => name === sccacheExportStep.name).run =
    'echo "SCCACHE_PROBE=$(cargo test -p jazz)" >> "${GITHUB_ENV}"';
  assert.throws(
    () => assertCiSuiteUsesOnlySharedCorrectnessPartitions(sccacheExploit),
    /unshared direct run step: Export trusted sccache configuration/,
  );

  const turboExploit = structuredClone(workflowModel);
  turboExploit.jobs["test-ts"].steps.find(({ name }) => name === turboExportStep.name).run =
    'echo "TURBO_REMOTE_CACHE_SIGNATURE_KEY=$(cargo test -p jazz)" >> "${GITHUB_ENV}"';
  assert.throws(
    () => assertCiSuiteUsesOnlySharedCorrectnessPartitions(turboExploit),
    /unshared direct run step: Export trusted Turbo cache signing key/,
  );

  const statsShellExploit = structuredClone(workflowModel);
  statsShellExploit.jobs.lint.steps.find(({ name }) => name === sccacheStatsStep.name).shell =
    "bash -c 'cargo test -p jazz; bash {0}'";
  assert.throws(
    () => assertCiSuiteUsesOnlySharedCorrectnessPartitions(statsShellExploit),
    /unshared direct run step: Report sccache statistics/,
  );

  const statsRunExploit = structuredClone(workflowModel);
  statsRunExploit.jobs.lint.steps.find(({ name }) => name === sccacheStatsStep.name).run =
    "sccache --show-stats; cargo test -p jazz";
  assert.throws(
    () => assertCiSuiteUsesOnlySharedCorrectnessPartitions(statsRunExploit),
    /unshared direct run step: Report sccache statistics/,
  );

  assert.deepEqual(
    planFor({ mode: "ci-equivalent" }),
    Object.values(ciPartitions).flat(),
    "local CI-equivalent must not quietly replace a CI partition with an approximation",
  );
});

test("crate publication selector tests are a reachable lint partition contract", () => {
  const lint = planFor({ partition: "lint" });
  const selectorTest = lint.find(({ label }) => label === "crate publication selector contract");
  assert.deepEqual(selectorTest, {
    label: "crate publication selector contract",
    executable: "bash",
    args: ["dev/scripts/publish-crates-alpha.test.sh"],
  });

  const withoutSelectorTest = lint.filter(
    ({ label }) => label !== "crate publication selector contract",
  );
  assert.equal(
    withoutSelectorTest.some(({ args }) => args.includes("publish-crates-alpha.test.sh")),
    false,
    "a removed selector test would leave the lint CI partition without its publish admission receipt",
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

test("CI-equivalent partitions cannot silently substitute their Rust or TypeScript runners", () => {
  const rust = planFor({ partition: "rust-workspace" }).find(
    ({ label }) => label === "workspace Rust tests",
  );
  assert.ok(rust);
  assert.ok(
    rust.args.includes("--require-nextest"),
    "CI-equivalent Rust must fail rather than silently switch to Cargo fallback",
  );

  const typescript = planFor({ partition: "typescript" }).find(
    ({ label }) => label === "TypeScript consumers",
  );
  assert.ok(typescript);
  assert.deepEqual(typescript.env, { JAZZ_REQUIRE_CI_TEST_COMMANDS: "1" });

  const planted = { ...typescript, env: {} };
  assert.notDeepEqual(
    planted.env,
    { JAZZ_REQUIRE_CI_TEST_COMMANDS: "1" },
    "a missing guard would permit inherited suite overrides",
  );
});

test("the shared TypeScript runner rejects inherited suite overrides under the CI guard", () => {
  const result = spawnSync("bash", ["dev/gates/run-ts-tests.sh"], {
    cwd: root,
    encoding: "utf8",
    env: {
      ...process.env,
      JAZZ_REQUIRE_CI_TEST_COMMANDS: "1",
      JAZZ_NODE_TEST_COMMAND: "true",
    },
  });
  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /JAZZ_NODE_TEST_COMMAND.*forbidden by the CI-equivalent partition/);
});

test("exact partitions fail closed on ambient Jazz controls and retain only the CI artifact lock", () => {
  assert.deepEqual(ALLOWED_INHERITED_CI_JAZZ_ENV, ["JAZZ_TEST_ARTIFACT_LOCK_PATH"]);
  assert.throws(
    () =>
      exactCiEnvironment({
        PATH: "/bin",
        JAZZ_UPDATE_WIRE_FIXTURES: "1",
        JAZZ_SKIP_BULK_INGEST_ASSERTS: "1",
      }),
    /JAZZ_SKIP_BULK_INGEST_ASSERTS, JAZZ_UPDATE_WIRE_FIXTURES/,
    "fixture rewrites and assertion skips must fail before any partition starts",
  );

  const base = exactCiEnvironment({
    PATH: "/bin",
    JAZZ_TEST_ARTIFACT_LOCK_PATH: "/tmp/jazz-test-artifacts.lock",
  });
  assert.deepEqual(base, {
    PATH: "/bin",
    JAZZ_TEST_ARTIFACT_LOCK_PATH: "/tmp/jazz-test-artifacts.lock",
  });
  assert.deepEqual(commandEnvironment(base, { JAZZ_REQUIRE_CI_TEST_COMMANDS: "1" }), {
    PATH: "/bin",
    JAZZ_TEST_ARTIFACT_LOCK_PATH: "/tmp/jazz-test-artifacts.lock",
    JAZZ_REQUIRE_CI_TEST_COMMANDS: "1",
  });
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
      seen.includes("native correctness-artifact producer"),
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
        if (item.label === "native correctness-artifact producer")
          throw new Error("generated artifact failure");
      }),
    /generated artifact failure/,
  );
  assert.deepEqual(seen, [
    "all Rust workspace target classes",
    "native correctness-artifact producer",
  ]);
});

test("a successful native producer remains visible when a TypeScript consumer fails", async () => {
  const typescript = planFor({ partition: "typescript" });
  const seen = [];
  await assert.rejects(
    () =>
      runPlan(typescript, async (item) => {
        seen.push(item.label);
        if (item.label === "TypeScript consumers") throw new Error("planted TS consumer failure");
      }),
    /planted TS consumer failure/,
  );
  assert.deepEqual(seen, [
    "all Rust workspace target classes",
    "native correctness-artifact producer",
    "preinstalled Chromium",
    "TypeScript consumers",
  ]);
});

test("storage compatibility executes the historical browser file and propagates its failure", async () => {
  const partition = planFor({ partition: "storage-compat" });
  const assertBrowser = (commands) => {
    const browser = commands.find((item) => item.label === "browser storage compatibility corpus");
    assert.ok(browser, "storage partition must execute browser corpus");
    assert.equal(browser.executable, "pnpm");
    assert.deepEqual(browser.args, [
      "--dir",
      "packages/jazz-tools",
      "test:browser:focused",
      "--",
      "tests/browser/indexeddb-jazz-compat.test.ts",
    ]);
  };
  assertBrowser(partition);
  assert.throws(
    () =>
      assertBrowser(
        partition.filter((item) => item.label !== "browser storage compatibility corpus"),
      ),
    /must execute browser corpus/,
  );
  const seen = [];
  await assert.rejects(
    runPlan(partition, async (item) => {
      seen.push(item.label);
      if (item.label === "browser storage compatibility corpus")
        throw new Error("planted browser corpus failure");
    }),
    /planted browser corpus failure/,
  );
  assert.deepEqual(seen, [
    "native storage compatibility corpus",
    "native correctness-artifact producer",
    "preinstalled Chromium",
    "browser storage compatibility corpus",
  ]);
  assert.equal(workflowModel.jobs["test-storage-compat"]["timeout-minutes"], 30);
});
