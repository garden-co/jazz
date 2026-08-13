import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

const root = path.resolve(import.meta.dirname, "../../..");
const workflow = fs.readFileSync(path.join(root, ".github/workflows/ci.yml"), "utf8");
const setupBuildAction = fs.readFileSync(
  path.join(root, ".github/actions/setup-build/action.yml"),
  "utf8",
);
const installRustTool = fs.readFileSync(
  path.join(root, ".github/actions/install-rust-tool/action.yml"),
  "utf8",
);
const packageBuild = fs.readFileSync(
  path.join(root, ".github/workflows/build-jazz-packages.yml"),
  "utf8",
);
const packageJson = JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8"));
const jobs = (() => {
  const jobsStart = workflow.indexOf("\njobs:\n");
  assert.notEqual(jobsStart, -1, "missing jobs section");
  const matches = [...workflow.slice(jobsStart).matchAll(/^  ([A-Za-z0-9_-]+):\s*$/gm)];
  assert.ok(matches.length > 0, "CI workflow must define at least one job");
  return new Map(
    matches.map((match, index) => [
      match[1],
      workflow.slice(
        jobsStart + match.index,
        index + 1 < matches.length ? jobsStart + matches[index + 1].index : workflow.length,
      ),
    ]),
  );
})();
const job = (name) => {
  const source = jobs.get(name);
  assert.notEqual(source, undefined, `missing ${name} job`);
  return source;
};
const trustedRunnerExpression =
  "${{ github.event_name == 'pull_request' && (github.event.pull_request.head.repo.full_name != github.repository || github.event.pull_request.user.login == 'dependabot[bot]') && 'blacksmith-4vcpu-ubuntu-2404' || 'jazz-ci' }}";
const untrustedPullRequestPredicate =
  "github.event_name == 'pull_request' && (github.event.pull_request.head.repo.full_name != github.repository || github.event.pull_request.user.login == 'dependabot[bot]')";
const assertUsesTrustedRunnerPool = (jobName, jobSource) => {
  assert.ok(
    jobSource.includes(`runs-on: ${trustedRunnerExpression}`),
    `${jobName} must use jazz-ci for pushes and trusted PRs, while fork PRs use Blacksmith`,
  );
  assert.doesNotMatch(
    jobSource,
    /^    runs-on: jazz-ci$/m,
    `${jobName} must not run fork or Dependabot PRs unconditionally on jazz-ci`,
  );
};
const expectedRunner = ({ eventName, headRepository, repository, pullRequestUser }) =>
  eventName === "pull_request" &&
  (headRepository !== repository || pullRequestUser === "dependabot[bot]")
    ? "blacksmith-4vcpu-ubuntu-2404"
    : "jazz-ci";
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
  const lint = job("lint");
  const rust = job("test-rust");
  const typescript = job("test-ts");

  assert.doesNotMatch(workflow, /cargo install cargo-nextest/);
  assert.match(rust, /tool: cargo-nextest@0\.9\.143/);
  assert.doesNotMatch(rust, /ensure:rust-toolchain|wasm-pack/);
  assert.doesNotMatch(rust, /rust-components:/);
  assert.doesNotMatch(lint, /ensure:rust-toolchain|wasm-pack/);
  assert.match(typescript, /tool: wasm-pack@0\.13\.1/);
});

test("build setup scopes mutable pnpm and sccache state to the agent temp directory", () => {
  assert.match(setupBuildAction, /dest: \$\{\{ runner\.temp \}\}\/setup-pnpm/);
  assert.match(setupBuildAction, /sccache_dir="\$\{RUNNER_TEMP\}\/sccache"/);
  assert.doesNotMatch(
    setupBuildAction,
    /\$\{HOME\}\/(?:setup-pnpm|\.cache\/sccache)/,
    "shared HOME must not hold mutable pnpm or sccache state",
  );
});

test("build setup isolates mutable Rustup toolchains without moving Cargo caches", () => {
  const rustupSetup =
    /name: Isolate Rustup state[\s\S]*rustup_home="\$\{RUNNER_TEMP\}\/rustup"[\s\S]*echo "RUSTUP_HOME=\$\{rustup_home\}" >> "\$\{GITHUB_ENV\}"/;
  assert.match(setupBuildAction, rustupSetup);
  assert.ok(
    setupBuildAction.indexOf("name: Isolate Rustup state") <
      setupBuildAction.indexOf("name: Install Rust toolchain"),
    "RUSTUP_HOME must be exported before rustup installs a toolchain",
  );
  assert.doesNotMatch(setupBuildAction, /RUSTUP_HOME=.*\$\{HOME\}/);
  assert.throws(
    () =>
      assert.match(
        setupBuildAction.replace(
          'echo "RUSTUP_HOME=${rustup_home}" >> "${GITHUB_ENV}"',
          'echo "RUST_UP_HOME=${rustup_home}" >> "${GITHUB_ENV}"',
        ),
        rustupSetup,
      ),
    /Isolate Rustup state/,
  );
});

test("Rust tool installation is isolated from shared self-hosted runner state", () => {
  const installAction = "taiki-e/install-action@3235f8901fd37ffed0052b276cec25a362fb82e9";
  assert.match(installRustTool, new RegExp(`uses: ${installAction}`));
  assert.match(installRustTool, /HOME: \$\{\{ runner\.temp \}\}\/jazz-install-action/);
  assert.match(installRustTool, /CARGO_HOME: \$\{\{ runner\.temp \}\}\/jazz-install-action\/cargo/);
  for (const caller of [workflow, setupBuildAction, packageBuild]) {
    assert.doesNotMatch(caller, new RegExp(installAction));
  }
  assert.throws(
    () =>
      assert.match(
        installRustTool.replace(
          "        CARGO_HOME: ${{ runner.temp }}/jazz-install-action/cargo\n",
          "",
        ),
        /CARGO_HOME: \$\{\{ runner\.temp \}\}\/jazz-install-action\/cargo/,
      ),
    /CARGO_HOME/,
  );
});

test("the TypeScript CI job checks the integration workspace before TypeScript artifacts", () => {
  const typescript = job("test-ts");

  // Keeping these phases together avoids a separate checkout/setup phase.
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
  assertUsesTrustedRunnerPool("test-ts", typescript);
});

test("every CI job uses jazz-ci only for trusted work and Blacksmith for untrusted PRs", () => {
  for (const [name, source] of jobs) {
    assertUsesTrustedRunnerPool(name, source);
  }
});

test("trusted-runner policy keeps forks and same-repository Dependabot PRs hosted", () => {
  const repository = "gardencmp/jazz";
  assert.equal(expectedRunner({ eventName: "push", repository }), "jazz-ci");
  assert.equal(
    expectedRunner({ eventName: "pull_request", headRepository: repository, repository }),
    "jazz-ci",
  );
  assert.equal(
    expectedRunner({ eventName: "pull_request", headRepository: "fork/jazz", repository }),
    "blacksmith-4vcpu-ubuntu-2404",
  );
  assert.equal(
    expectedRunner({
      eventName: "pull_request",
      headRepository: repository,
      repository,
      pullRequestUser: "dependabot[bot]",
    }),
    "blacksmith-4vcpu-ubuntu-2404",
  );
  assert.match(workflow, /github\.event\.pull_request\.user\.login == 'dependabot\[bot\]'/);
  assert.doesNotMatch(workflow, /github\.actor/);
});

test("TypeScript cache mounts use the same untrusted-PR predicate as the runner", () => {
  const typescript = job("test-ts");
  assert.ok(
    typescript.includes(
      `sccache-sticky-disk: \${{ ${untrustedPullRequestPredicate} && 'true' || 'false' }}`,
    ),
  );
  assert.equal(
    typescript.split(`if: ${untrustedPullRequestPredicate}`).length - 1,
    2,
    "Turbo and Playwright sticky-disk mounts must both follow the runner trust boundary",
  );
});

test("trusted-runner contract rejects planted unsafe runner changes", () => {
  const lint = job("lint");
  assert.throws(
    () => assertUsesTrustedRunnerPool("lint", lint.replace(trustedRunnerExpression, "jazz-ci")),
    /must use jazz-ci for pushes and trusted PRs, while fork PRs use Blacksmith/,
  );
  assert.throws(
    () =>
      assertUsesTrustedRunnerPool(
        "lint",
        lint.replace(" || github.event.pull_request.user.login == 'dependabot[bot]'", ""),
      ),
    /must use jazz-ci for pushes and trusted PRs, while fork PRs use Blacksmith/,
  );
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
  const lint = job("lint");
  assert.match(lint, /run: pnpm lint/);
  assert.doesNotMatch(lint, /^\s*- run: cargo clippy/m);
});

test("CI runs the workflow contract test through its package script", () => {
  const lint = job("lint");
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
