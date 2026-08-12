import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const root = path.resolve(import.meta.dirname, "../../..");
const workflow = fs.readFileSync(path.join(root, ".github/workflows/ci.yml"), "utf8");
const packageJson = JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8"));
const job = (name, nextName) => {
  const start = workflow.indexOf(`  ${name}:`);
  const end = nextName ? workflow.indexOf(`  ${nextName}:`, start + 1) : workflow.length;
  assert.notEqual(start, -1, `missing ${name} job`);
  assert.notEqual(end, -1, `missing boundary after ${name} job`);
  return workflow.slice(start, end);
};
const integrationCheckStep = (typescriptJob) => {
  const start = typescriptJob.indexOf("name: Check integration workspace");
  assert.notEqual(start, -1, "missing integration workspace check");
  const end = typescriptJob.indexOf("\n      - ", start + 1);
  return typescriptJob.slice(start, end === -1 ? typescriptJob.length : end);
};
const assertIntegrationCheckIsGating = (typescriptJob) => {
  // Job-level continue-on-error makes every failed step non-gating. Reject the
  // property rather than only the literal `true`, because expressions are
  // equally able to accidentally suppress an integration failure.
  assert.doesNotMatch(
    typescriptJob,
    /^    continue-on-error:/m,
    "test-ts must not suppress job failures",
  );
  assert.doesNotMatch(
    integrationCheckStep(typescriptJob),
    /^\s+continue-on-error:/m,
    "integration workspace check must not suppress its failure",
  );
};

test("Rust CI uses pinned prebuilt tools without charging Rust-only jobs for wasm-pack", () => {
  const lint = job("lint", "test-rust");
  const rust = job("test-rust", "test-ts");
  const typescript = job("test-ts");

  assert.doesNotMatch(workflow, /cargo install cargo-nextest/);
  assert.match(rust, /tool: cargo-nextest@0\.9\.143/);
  assert.doesNotMatch(rust, /ensure:rust-toolchain|wasm-pack/);
  assert.doesNotMatch(rust, /rust-components:/);
  assert.doesNotMatch(lint, /ensure:rust-toolchain|wasm-pack/);
  assert.match(typescript, /tool: wasm-pack@0\.13\.1/);
});

test("the single trusted-runner job checks the integration workspace before TypeScript artifacts", () => {
  const typescript = job("test-ts");

  // The dedicated jazz-ci runner has one worker. Splitting these phases into
  // jobs serializes their checkout/setup work in GitHub's runner queue.
  assert.doesNotMatch(workflow, /^  build-integration:/m);
  assert.match(
    typescript,
    /name: Check integration workspace\s+run: cargo check --workspace --all-targets/,
  );
  assertIntegrationCheckIsGating(typescript);
  assert.ok(
    typescript.indexOf("name: Check integration workspace") <
      typescript.indexOf("name: Build correctness-test artifacts"),
    "workspace check must fail before the expensive correctness artifact build",
  );
  assert.match(typescript, /runs-on: \$\{\{ github\.event_name == 'pull_request'.*'jazz-ci' \}\}/);
});

test("integration workspace check contract rejects planted failure suppression", () => {
  const typescript = job("test-ts");
  const check =
    "name: Check integration workspace\n        run: cargo check --workspace --all-targets";

  assert.throws(
    () =>
      assertIntegrationCheckIsGating(
        typescript.replace(check, `${check}\n        continue-on-error: true`),
      ),
    /integration workspace check must not suppress its failure/,
  );
  assert.throws(
    () =>
      assertIntegrationCheckIsGating(
        typescript.replace(
          "    timeout-minutes: 20",
          "    continue-on-error: true\n    timeout-minutes: 20",
        ),
      ),
    /test-ts must not suppress job failures/,
  );
});

test("lint keeps its one workspace Clippy invocation inside pnpm lint", () => {
  const lint = job("lint", "test-rust");
  assert.match(lint, /run: pnpm lint/);
  assert.doesNotMatch(lint, /^\s*- run: cargo clippy/m);
});

test("CI runs the workflow contract test through its package script", () => {
  const lint = job("lint", "test-rust");
  assert.equal(
    packageJson.scripts["test:ci-workflow"],
    "node --test dev/gates/test/ci-rust-throughput.test.mjs dev/gates/test/test-artifact-pipeline.test.mjs dev/gates/test/release-gates.test.mjs",
  );
  assert.match(lint, /run: pnpm test:ci-workflow/);
});

test("TypeScript CI overlaps independent Node and browser suites after one artifact build", () => {
  const typescript = job("test-ts");
  const runner = fs.readFileSync(path.join(root, "dev/gates/run-ts-tests.sh"), "utf8");
  assert.match(
    typescript,
    /name: Build correctness-test artifacts\s+run: pnpm build:test-artifacts/,
  );
  assert.match(typescript, /name: Run Node and browser test suites in parallel/);
  assert.match(typescript, /run: dev\/gates\/run-ts-tests\.sh/);
  assert.match(runner, /--concurrency=2/);
  assert.match(runner, /setsid bash -c "\$\{node_tests_command\}" &/);
  assert.match(runner, /setsid bash -c "\$\{browser_tests_command\}" &/);
  assert.match(runner, /trap 'interrupt 130' INT/);
  assert.match(runner, /trap 'interrupt 143' TERM/);
  assert.match(runner, /kill -TERM -- "-\$\{child_pid\}"/);
  assert.match(runner, /wait "\$\{node_tests_pid\}"/);
  assert.match(runner, /node_tests_status=\$\?/);
  assert.match(runner, /wait "\$\{browser_tests_pid\}"/);
  assert.match(runner, /browser_tests_status=\$\?/);
  assert.match(runner, /Node test suite exit status:/);
  assert.match(runner, /Browser test suite exit status:/);
  assert.match(runner, /node_tests_status.*-ne 0 \|\|.*browser_tests_status.*-ne 0/);
  assert.doesNotMatch(typescript, /rust-components: clippy,rustfmt/);
});

test("parallel TypeScript runner waits for both suites and combines their failures", () => {
  const runner = path.join(root, "dev/gates/run-ts-tests.sh");
  const cases = [
    { node: 0, browser: 0, expected: 0 },
    { node: 7, browser: 0, expected: 1 },
    { node: 0, browser: 9, expected: 1 },
    { node: 7, browser: 9, expected: 1 },
  ];
  for (const testCase of cases) {
    const fixture = fs.mkdtempSync(path.join(os.tmpdir(), "jazz-ts-ci-runner-"));
    const nodeMarker = path.join(fixture, "node");
    const browserMarker = path.join(fixture, "browser");
    const result = spawnSync("bash", [runner], {
      cwd: root,
      encoding: "utf8",
      env: {
        ...process.env,
        JAZZ_NODE_TEST_COMMAND: `sleep 0.05; touch ${JSON.stringify(nodeMarker)}; exit ${testCase.node}`,
        JAZZ_BROWSER_TEST_COMMAND: `sleep 0.1; touch ${JSON.stringify(browserMarker)}; exit ${testCase.browser}`,
      },
    });
    assert.equal(result.status, testCase.expected, result.stderr);
    assert.equal(fs.existsSync(nodeMarker), true, "node suite was not reaped");
    assert.equal(fs.existsSync(browserMarker), true, "browser suite was not reaped");
    assert.match(result.stdout, new RegExp(`Node test suite exit status: ${testCase.node}`));
    assert.match(result.stdout, new RegExp(`Browser test suite exit status: ${testCase.browser}`));
    fs.rmSync(fixture, { recursive: true, force: true });
  }
});

test("parallel TypeScript runner terminates both child process groups", async () => {
  const fixture = fs.mkdtempSync(path.join(os.tmpdir(), "jazz-ts-ci-interrupt-"));
  const nodeMarker = path.join(fixture, "node-orphan");
  const browserMarker = path.join(fixture, "browser-orphan");
  const child = spawn("bash", [path.join(root, "dev/gates/run-ts-tests.sh")], {
    cwd: root,
    env: {
      ...process.env,
      JAZZ_NODE_TEST_COMMAND: `sleep 0.5; touch ${JSON.stringify(nodeMarker)}`,
      JAZZ_BROWSER_TEST_COMMAND: `sleep 0.5; touch ${JSON.stringify(browserMarker)}`,
    },
    stdio: "ignore",
  });
  await new Promise((resolve) => setTimeout(resolve, 100));
  child.kill("SIGTERM");
  const status = await new Promise((resolve) => child.once("exit", (code) => resolve(code)));
  assert.equal(status, 143);
  await new Promise((resolve) => setTimeout(resolve, 550));
  assert.equal(fs.existsSync(nodeMarker), false, "node descendant survived TERM");
  assert.equal(fs.existsSync(browserMarker), false, "browser descendant survived TERM");
  fs.rmSync(fixture, { recursive: true, force: true });
});
