import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { parse } from "yaml";

const root = path.resolve(import.meta.dirname, "../../..");
const workflow = fs.readFileSync(path.join(root, ".github/workflows/ci.yml"), "utf8");
const workflowSuite = fs.readFileSync(path.join(root, ".github/workflows/ci-suite.yml"), "utf8");
const setupBuildAction = fs.readFileSync(
  path.join(root, ".github/actions/setup-build/action.yml"),
  "utf8",
);
const setupBlacksmithAction = fs.readFileSync(
  path.join(root, ".github/actions/setup-blacksmith/action.yml"),
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
const codspeedWorkflow = fs.readFileSync(path.join(root, ".github/workflows/codspeed.yml"), "utf8");
const realisticWorkflow = fs.readFileSync(
  path.join(root, ".github/workflows/benchmarks.yml"),
  "utf8",
);
const benchmarkSmokeGate = fs.readFileSync(path.join(root, "dev/gates/benchmark-smoke.sh"), "utf8");
const otherWorkflows = fs
  .readdirSync(path.join(root, ".github/workflows"))
  .filter((name) => name.endsWith(".yml") && !["ci.yml", "ci-suite.yml"].includes(name))
  .map((name) => fs.readFileSync(path.join(root, ".github/workflows", name), "utf8"));
const packageJson = JSON.parse(fs.readFileSync(path.join(root, "package.json"), "utf8"));
const toolBundleValidator = fs.readFileSync(
  path.join(root, "dev/ci/validate-tool-bundle.mjs"),
  "utf8",
);
const m3Differential = fs.readFileSync(
  path.join(root, "crates/jazz/src/node/tests/m3_differential.rs"),
  "utf8",
);
const jobs = (() => {
  const jobsStart = workflowSuite.indexOf("\njobs:\n");
  assert.notEqual(jobsStart, -1, "missing jobs section");
  const matches = [...workflowSuite.slice(jobsStart).matchAll(/^  ([A-Za-z0-9_-]+):\s*$/gm)];
  assert.ok(matches.length > 0, "CI workflow must define at least one job");
  return new Map(
    matches.map((match, index) => [
      match[1],
      workflowSuite.slice(
        jobsStart + match.index,
        index + 1 < matches.length ? jobsStart + matches[index + 1].index : workflowSuite.length,
      ),
    ]),
  );
})();
const job = (name) => {
  const source = jobs.get(name);
  assert.notEqual(source, undefined, `missing ${name} job`);
  return source;
};
const assertUsesBlacksmithRunner = (jobName, jobSource) => {
  const cpu = jobName === "test-ts" ? 16 : 4;
  assert.match(jobSource, new RegExp(`runs-on: blacksmith-${cpu}vcpu-ubuntu-2404`));
  assert.doesNotMatch(jobSource, /^    runs-on: jazz-ci$/m);
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
const trustedCachePullRequest =
  'github.event_name == \'pull_request\' && github.event.pull_request.head.repo.full_name == github.repository && contains(fromJSON(\'["OWNER","MEMBER","COLLABORATOR"]\'), github.event.pull_request.author_association)';
const trustedCacheCondition =
  "github.event_name == 'push' && github.ref == 'refs/heads/main' || " + trustedCachePullRequest;
const untrustedCacheCondition = "github.event_name != 'push' && !(" + trustedCachePullRequest + ")";
const sccacheWriter =
  "inputs.trusted-cache && inputs.sccache-write && vars.SCCACHE_TRUSTED_WRITER_AWS_ROLE_ARN != ''";
const sccacheReader =
  "inputs.trusted-cache && !inputs.sccache-write && vars.SCCACHE_PR_READER_AWS_ROLE_ARN != ''";
const sccacheS3 =
  "inputs.trusted-cache && (inputs.sccache-write && vars.SCCACHE_TRUSTED_WRITER_AWS_ROLE_ARN != '' || !inputs.sccache-write && vars.SCCACHE_PR_READER_AWS_ROLE_ARN != '')";
const turboCache = "inputs.trusted-cache";
const regex = (source) => new RegExp(source.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
const assertSccacheTrustBoundary = (source) => {
  assert.match(source, regex(`if: ${sccacheWriter}`));
  assert.match(source, regex(`if: ${sccacheReader}`));
  assert.match(source, regex("sccache-s3: ${{ " + sccacheS3 + " }}"));
};
const s3Environment = [
  "SCCACHE_BUCKET",
  "SCCACHE_REGION",
  "SCCACHE_S3_KEY_PREFIX",
  "SCCACHE_S3_USE_SSL",
];
const assertSuiteS3ConfigurationBoundary = (source) => {
  const document = parse(source);
  for (const name of s3Environment)
    assert.equal(
      document.env?.[name],
      undefined,
      `untrusted suite callers must not inherit ${name}`,
    );

  for (const name of ["lint", "test-rust-workspace", "test-rust-differential", "test-ts"]) {
    const parsedJob = document.jobs[name];
    assert.equal(parsedJob.env, undefined, `${name} must not configure S3 at job scope`);
    const exportIndex = parsedJob.steps.findIndex(
      (step) => step.name === "Export trusted sccache configuration",
    );
    const setupIndex = parsedJob.steps.findIndex(
      (step) => step.uses === "./.github/actions/setup-blacksmith",
    );
    assert.notEqual(exportIndex, -1, `${name} is missing trusted S3 configuration export`);
    assert.ok(exportIndex < setupIndex, `${name} must export S3 configuration before setup`);
    const exportStep = parsedJob.steps[exportIndex];
    assert.equal(exportStep.if, sccacheS3);
    assert.deepEqual(exportStep.env, {
      CACHE_BUCKET: "${{ vars.SCCACHE_BUCKET }}",
      CACHE_REGION: "${{ vars.SCCACHE_REGION }}",
    });
    for (const variable of s3Environment)
      assert.match(exportStep.run, new RegExp(`echo "${variable}=`));
    assert.equal(parsedJob.steps[setupIndex].with["sccache-s3"], "${{ " + sccacheS3 + " }}");
  }
};
const cacheAccessFor = ({ eventName, ref, sameRepository = false, authorAssociation = "NONE" }) => {
  const trustedPullRequest =
    eventName === "pull_request" &&
    sameRepository &&
    ["OWNER", "MEMBER", "COLLABORATOR"].includes(authorAssociation);
  const trusted = (eventName === "push" && ref === "refs/heads/main") || trustedPullRequest;
  return {
    invocation: trusted ? "trusted" : "untrusted",
    idToken: trusted ? "write" : "none",
    sccache: trusted ? (eventName === "push" ? "writer" : "reader") : "none",
    turbo: trusted,
  };
};
const workflowDocumentFor = (source) => {
  const document = parse(source);
  assert.equal(typeof document, "object", "CI entry workflow must be a YAML mapping");
  assert.equal(typeof document.jobs, "object", "CI entry workflow must define a jobs mapping");
  return document;
};
const assertEntryCacheTrustBoundary = (source) => {
  const document = workflowDocumentFor(source);
  const entryJobs = document.jobs;
  assert.deepEqual(
    Object.keys(entryJobs).sort(),
    ["test-rust", "trusted", "untrusted"],
    "the credential boundary must account for every entry-workflow job",
  );
  const { untrusted, trusted, "test-rust": aggregate } = entryJobs;
  assert.deepEqual(
    Object.keys(untrusted).sort(),
    ["if", "permissions", "uses", "with"],
    "untrusted caller must expose no additional execution or credential surface",
  );
  assert.equal(untrusted.if, untrustedCacheCondition);
  assert.deepEqual(untrusted.permissions, {
    contents: "read",
    "id-token": "none",
    packages: "read",
  });
  assert.equal(untrusted.uses, "./.github/workflows/ci-suite.yml");
  assert.deepEqual(untrusted.with, { "sccache-write": false, "trusted-cache": false });

  assert.deepEqual(
    Object.keys(trusted).sort(),
    ["if", "permissions", "secrets", "uses", "with"],
    "trusted caller credential surface must stay explicit",
  );
  assert.equal(trusted.if, trustedCacheCondition);
  assert.deepEqual(trusted.permissions, {
    contents: "read",
    "id-token": "write",
    packages: "read",
  });
  assert.equal(trusted.uses, "./.github/workflows/ci-suite.yml");
  assert.deepEqual(trusted.with, {
    "sccache-write": "${{ github.event_name == 'push' }}",
    "trusted-cache": true,
  });
  assert.equal(trusted.secrets, "inherit");

  assert.deepEqual(Object.keys(aggregate).sort(), [
    "if",
    "needs",
    "permissions",
    "runs-on",
    "steps",
    "timeout-minutes",
  ]);
  assert.equal(aggregate.if, "always()");
  assert.deepEqual(aggregate.needs, ["untrusted", "trusted"]);
  assert.deepEqual(aggregate.permissions, { contents: "read" });
  assert.equal(document.on.pull_request_target, undefined);
  assert.equal(document.on.pull_request, null, "stacked PR bases must not be branch-filtered");
  assert.deepEqual(document.on.push, { branches: ["main"] });
};
const assertTurboSigningKeyTrustBoundary = (typescriptJob) => {
  assert.doesNotMatch(
    typescriptJob,
    /^      TURBO_REMOTE_CACHE_SIGNATURE_KEY:/m,
    "the job environment must not expose the signing key to manual feature runs",
  );
  const start = typescriptJob.indexOf("name: Export trusted Turbo cache signing key");
  assert.notEqual(start, -1, "missing conditional Turbo signing-key export");
  const end = typescriptJob.indexOf("\n      - ", start);
  const exportStep = typescriptJob.slice(start, end === -1 ? typescriptJob.length : end);
  assert.match(exportStep, regex(`if: ${turboCache}`));
  assert.match(
    exportStep,
    /CACHE_SIGNATURE_KEY: \$\{\{ secrets\.TURBO_REMOTE_CACHE_SIGNATURE_KEY \}\}/,
  );
  assert.match(
    exportStep,
    /echo "TURBO_REMOTE_CACHE_SIGNATURE_KEY=\$\{CACHE_SIGNATURE_KEY\}" >> "\$\{GITHUB_ENV\}"/,
  );
};
const assertTurboCredentialConditions = (typescriptJob) => {
  assertTurboSigningKeyTrustBoundary(typescriptJob);
  const trustedConditions = typescriptJob.match(/^        if: inputs\.trusted-cache$/gm);
  assert.equal(
    trustedConditions?.length,
    2,
    "the signing-key export and Turbo OIDC setup must share the exact trusted condition",
  );
};

test("Rust CI uses pinned prebuilt tools without charging Rust-only jobs for wasm-pack", () => {
  const lint = job("lint");
  const workspaceRust = job("test-rust-workspace");
  const differentialRust = job("test-rust-differential");
  const typescript = job("test-ts");

  assert.doesNotMatch(workflowSuite, /cargo install cargo-nextest/);
  assert.match(setupBlacksmithAction, /cargo-nextest --version \| grep -F "0\.9\.143"/);
  assert.match(setupBlacksmithAction, /wasm-pack --version \| grep -F "0\.13\.1"/);
  for (const rust of [workspaceRust, differentialRust]) {
    assert.doesNotMatch(rust, /install-rust-tool|ensure:rust-toolchain|wasm-pack/);
    assert.doesNotMatch(rust, /rust-components:/);
  }
  assert.doesNotMatch(lint, /install-rust-tool|ensure:rust-toolchain|wasm-pack/);
  assert.doesNotMatch(typescript, /install-rust-tool/);
  for (const source of [lint, workspaceRust, differentialRust, typescript])
    assert.match(source, /uses: \.\/\.github\/actions\/setup-blacksmith/);
});

test("build setup scopes mutable pnpm and sccache state to the agent temp directory", () => {
  assert.match(setupBuildAction, /dest: \$\{\{ runner\.temp \}\}\/setup-pnpm/);
  assert.match(setupBuildAction, /sccache_dir="\$\{RUNNER_TEMP\}\/sccache"/);
  assert.match(setupBuildAction, /sccache_socket="\$\{sccache_dir\}\/server\.sock"/);
  assert.match(
    setupBuildAction,
    /SCCACHE_DIR="\$\{sccache_dir\}" SCCACHE_SERVER_UDS="\$\{sccache_socket\}" sccache --start-server/,
  );
  const serverSocketExport = /echo "SCCACHE_SERVER_UDS=\$\{sccache_socket\}" >> "\$\{GITHUB_ENV\}"/;
  assert.match(setupBuildAction, serverSocketExport);
  const stickyMount = setupBuildAction.indexOf("name: Mount sccache sticky disk");
  const serverStart = setupBuildAction.indexOf("sccache --start-server");
  const fallbackBranch = setupBuildAction.slice(
    setupBuildAction.indexOf("else\n          # Hosted jobs"),
  );
  const environmentExport = setupBuildAction.indexOf(
    'echo "RUSTC_WRAPPER=sccache"',
    setupBuildAction.indexOf("else\n          # Hosted jobs"),
  );
  assert.ok(stickyMount !== -1 && serverStart !== -1 && environmentExport !== -1);
  assert.ok(
    stickyMount < serverStart && serverStart < environmentExport,
    "mount the restored cache before starting its server and exporting it to compilers",
  );
  assert.doesNotMatch(
    setupBuildAction,
    /\$\{HOME\}\/(?:setup-pnpm|\.cache\/sccache)/,
    "shared HOME must not hold mutable pnpm or sccache state",
  );
  assert.throws(
    () =>
      assert.match(
        setupBuildAction.replace(
          'echo "SCCACHE_SERVER_UDS=${sccache_socket}" >> "${GITHUB_ENV}"',
          "",
        ),
        serverSocketExport,
      ),
    /SCCACHE_SERVER_UDS/,
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

test("trusted runners consume the validated immutable tool bundle", () => {
  const fallbackBranch = setupBuildAction.slice(
    setupBuildAction.indexOf("else\n          # Hosted jobs"),
  );
  assert.match(setupBuildAction, /node dev\/ci\/validate-tool-bundle\.mjs/);
  assert.match(setupBuildAction, /steps\.provisioned-tools\.outputs\.active != 'true'/);
  assert.match(installRustTool, /steps\.provisioned-tool\.outputs\.active != 'true'/);
  assert.match(toolBundleValidator, /BUNDLE_ROOT = "\/opt\/jazz-ci\/toolchains\/v1"/);
  for (const value of ["1.93.1", "0.15.0", "0.9.143", "0.13.1", "0.2.117"]) {
    assert.match(toolBundleValidator, new RegExp(value.replaceAll(".", "\\.")));
  }
  assert.match(setupBuildAction, /sccache --show-stats/);
  assert.match(
    setupBuildAction,
    /echo "RUSTC=\/opt\/jazz-ci\/toolchains\/v1\/rustup\/toolchains\/1\.93\.1-x86_64-unknown-linux-gnu\/bin\/rustc" >> "\$\{GITHUB_ENV\}"/,
  );
  assert.match(setupBuildAction, /Jobs are clients only/);
  assert.match(
    setupBuildAction,
    /name: Cache Rust build artifacts[\s\S]*if: inputs\.rust-toolchain != '' && inputs\.rust-cache == 'true' && steps\.provisioned-tools\.outputs\.active != 'true'[\s\S]*uses: Swatinem\/rust-cache@/,
    "rust-cache cleanup must never mutate a provisioned runner's shared Rust homes",
  );
  assert.doesNotMatch(
    setupBuildAction.match(
      /if \[\[ '\$\{\{ steps\.provisioned-tools\.outputs\.active \}\}' == 'true' \]\]; then[\s\S]*?else/,
    )?.[0] ?? "",
    /sccache --(?:start|stop)-server/,
  );
  assert.match(fallbackBranch, /sccache --start-server/);
  assert.throws(
    () =>
      assert.match(
        setupBuildAction.replace(
          " && steps.provisioned-tools.outputs.active != 'true'\n      uses: Swatinem/rust-cache@",
          "\n      uses: Swatinem/rust-cache@",
        ),
        /name: Cache Rust build artifacts[\s\S]*if: inputs\.rust-toolchain != '' && inputs\.rust-cache == 'true' && steps\.provisioned-tools\.outputs\.active != 'true'[\s\S]*uses: Swatinem\/rust-cache@/,
      ),
    /rust-cache/,
  );
});

test("Rust CI splits a bounded real differential-oracle smoke behind a stable aggregate", () => {
  const workspace = job("test-rust-workspace");
  const differential = job("test-rust-differential");
  const aggregate = job("test-rust");
  const compileStepName = "name: Compile M3 differential oracle libtest";
  const runStepName = "name: Run maintained-vs-one-shot differential oracle smoke";
  const assertM3CompileThenRun = (source) => {
    const compileStart = source.indexOf(compileStepName);
    const runStart = source.indexOf(runStepName);
    assert.ok(compileStart !== -1, "missing M3 libtest compile step");
    assert.ok(runStart !== -1, "missing M3 semantic execution step");
    assert.ok(compileStart < runStart, "compile the M3 libtest before its semantic execution");
    assert.match(
      source.slice(compileStart, runStart),
      /shell: bash/,
      "the M3 compile step must not inherit the container's sh default",
    );
    assert.doesNotMatch(
      source.slice(compileStart, runStart),
      /\btimeout\b/,
      "cold compilation must not consume the semantic-execution timeout",
    );
    assert.match(
      source.slice(runStart),
      /timeout 60s env/,
      "the direct libtest execution must remain bounded",
    );
    assert.match(
      source.slice(runStart),
      /shell: bash/,
      "the M3 semantic execution step must not inherit the container's sh default",
    );
  };

  assert.match(
    workspace,
    /run-rust-tests\.mjs --timeout-seconds 780 --nextest-profile jazz-ci -- --workspace --lib --bins --tests --features jazz\/testing,jazz\/transport-compression-zstd,jazz-server\/test,jazz-cli\/test/,
  );
  for (const testTarget of [
    "incremental_delivery_canary",
    "shared_coverage_differential",
    "warm_reopen_differential",
  ]) {
    assert.doesNotMatch(workspace, new RegExp(`cargo test -p jazz --test ${testTarget}`));
  }
  assert.match(
    differential,
    /cargo test -p jazz --lib --features testing,transport-compression-zstd --no-run --message-format=json/,
  );
  assert.match(differential, /message\.target\.name === "jazz"/);
  assert.match(
    differential,
    /echo "M3_ORACLE_TEST_BINARY=\$\{test_binary\}" >> "\$\{GITHUB_ENV\}"/,
  );
  assert.match(
    differential,
    /timeout 60s env[\s\\]+JAZZ_SEED=11[\s\\]+JAZZ_DIFFERENTIAL_CHURN_DEPTHS=10,1000[\s\\]+JAZZ_DIFFERENTIAL_STEP_COUNT=3[\s\\]+"\$\{M3_ORACLE_TEST_BINARY\}" node::tests::harness::m3_maintained_one_shot_differential_oracle --exact --ignored/,
  );
  assertM3CompileThenRun(differential);
  assert.match(differential, /full randomized matrix[\s\S]*long-running manual soak/);
  assert.doesNotMatch(differential, /tracked red debt/);
  assert.match(
    m3Differential,
    /#\[ignore = "manual randomized differential soak; bounded seed 11 runs in CI"\]\n(?:pub )?fn m3_maintained_one_shot_differential_oracle/,
  );
  assert.doesNotMatch(workspace, /m3_maintained_one_shot_differential_oracle/);
  assert.match(aggregate, /if: always\(\)/);
  assert.match(aggregate, /needs: \[test-rust-workspace, test-rust-differential\]/);
  assert.match(aggregate, /WORKSPACE_RESULT: \$\{\{ needs\.test-rust-workspace\.result \}\}/);
  assert.match(aggregate, /DIFFERENTIAL_RESULT: \$\{\{ needs\.test-rust-differential\.result \}\}/);
  assert.match(aggregate, /test "\$\{WORKSPACE_RESULT\}" = success/);
  assert.match(aggregate, /test "\$\{DIFFERENTIAL_RESULT\}" = success/);
  assert.match(differential, /rust-cache: "false"/);
  assert.throws(
    () =>
      assert.match(
        differential.replace('rust-cache: "false"', 'rust-cache: "true"'),
        /rust-cache: "false"/,
      ),
    /rust-cache/,
  );
  assert.throws(
    () =>
      assert.match(
        differential.replace(
          "cargo test -p jazz --lib --features testing,transport-compression-zstd --no-run --message-format=json",
          "cargo test -p jazz --no-run --message-format=json",
        ),
        /cargo test -p jazz --lib --features testing,transport-compression-zstd --no-run --message-format=json/,
      ),
    /--lib/,
  );
  assert.throws(
    () => assert.match(differential.replace("--exact --ignored", "--ignored"), /--exact --ignored/),
    /exact/,
  );
  assert.throws(
    () =>
      assertM3CompileThenRun(
        differential.replace(
          `${compileStepName}\n        shell: bash\n        run:`,
          `${compileStepName}\n        run:`,
        ),
      ),
    /compile step must not inherit the container's sh default/,
  );
  assert.throws(
    () =>
      assertM3CompileThenRun(
        differential.replace(
          `${runStepName}\n        shell: bash\n        run:`,
          `${runStepName}\n        run:`,
        ),
      ),
    /semantic execution step must not inherit the container's sh default/,
  );
  assert.throws(
    () =>
      assertM3CompileThenRun(
        differential.replace(
          "cargo test -p jazz --lib --features testing,transport-compression-zstd --no-run --message-format=json",
          "timeout 60s cargo test -p jazz --lib --features testing,transport-compression-zstd --no-run --message-format=json",
        ),
      ),
    /cold compilation must not consume the semantic-execution timeout/,
  );
  assert.throws(
    () =>
      assertM3CompileThenRun(
        differential
          .replace(compileStepName, "__M3_STEP_SWAP__")
          .replace(runStepName, compileStepName)
          .replace("__M3_STEP_SWAP__", runStepName),
      ),
    /compile the M3 libtest before its semantic execution/,
  );
});

test("Blacksmith setup can exclude nondeterministic jobs from the Rust artifact cache", () => {
  assert.match(setupBlacksmithAction, /inputs:[\s\S]*rust-cache:[\s\S]*default: "true"/);
  assert.match(
    setupBlacksmithAction,
    /name: Cache Rust build artifacts\n      if: inputs\.rust-cache == 'true'/,
  );
  assert.throws(
    () =>
      assert.match(
        setupBlacksmithAction.replace("if: inputs.rust-cache == 'true'\n", ""),
        /if: inputs\.rust-cache == 'true'/,
      ),
    /rust-cache/,
  );
});

test("Rust CI uses a contention-tolerant but finite Nextest watchdog", () => {
  const nextest = fs.readFileSync(path.join(root, ".config/nextest.toml"), "utf8");
  const localProfile = nextest.match(/\[profile\.jazz\]([\s\S]*?)(?=\n\[|$)/)?.[1] ?? "";
  const ciProfile = nextest.match(/\[profile\.jazz-ci\]([\s\S]*?)(?=\n\[|$)/)?.[1] ?? "";

  assert.match(localProfile, /slow-timeout = \{ period = "60s", terminate-after = 1 \}/);
  assert.match(ciProfile, /fail-fast = false/);
  assert.match(ciProfile, /slow-timeout = \{ period = "180s", terminate-after = 1 \}/);
  assert.throws(
    () =>
      assert.match(
        ciProfile.replace("terminate-after = 1", "terminate-after = 0"),
        /terminate-after = 1/,
      ),
    /terminate-after = 1/,
  );
});

test("the TypeScript CI job checks the integration workspace before TypeScript artifacts", () => {
  const typescript = job("test-ts");

  assert.match(
    setupBlacksmithAction,
    /echo "JAZZ_TEST_ARTIFACT_LOCK_PATH=\$\{RUNNER_TEMP\}\/jazz-test-artifacts\.lock" >> "\$\{GITHUB_ENV\}"/,
  );

  // Keeping these phases together avoids a separate checkout/setup phase.
  assert.doesNotMatch(workflowSuite, /^  build-integration:/m);
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
  assertUsesBlacksmithRunner("test-ts", typescript);
});

test("every CI job uses an independently sized Blacksmith runner", () => {
  for (const [name, source] of jobs) {
    assertUsesBlacksmithRunner(name, source);
  }
});

test("Turbo cache uses its pinned OIDC policy only inside the trusted suite invocation", () => {
  const typescript = job("test-ts");
  assertTurboCredentialConditions(typescript);
  assert.match(typescript, /policy: pol_0b019736-e95d-4f60-a5dd-e9415148834c/);
  assert.match(typescript, /audience: https:\/\/github\.com\/garden-co/);
  assert.match(typescript, /team: garden-co/);
  assert.ok(
    typescript.indexOf("name: Export trusted Turbo cache signing key") <
      typescript.indexOf("name: Set up signed Turbo Remote Cache"),
    "export the signing key before the trusted Turbo setup step",
  );
  assert.match(fs.readFileSync(path.join(root, "turbo.json"), "utf8"), /"signature": true/);
  for (const releaseWorkflow of otherWorkflows)
    assert.doesNotMatch(
      releaseWorkflow,
      /setup-turborepo-remote-cache-action|TURBO_REMOTE_CACHE_SIGNATURE_KEY/,
      "release/deployment workflows must not consume the PR-populated CI cache",
    );
});

test("shared Rust cache writes are main-only while trusted PRs receive read access", () => {
  for (const name of ["lint", "test-rust-workspace", "test-rust-differential", "test-ts"]) {
    const source = job(name);
    assert.match(source, /role-to-assume: \$\{\{ vars\.SCCACHE_TRUSTED_WRITER_AWS_ROLE_ARN \}\}/);
    assert.match(source, /role-to-assume: \$\{\{ vars\.SCCACHE_PR_READER_AWS_ROLE_ARN \}\}/);
    assertSccacheTrustBoundary(source);
  }
  assertSuiteS3ConfigurationBoundary(workflowSuite);
  assert.doesNotMatch(
    workflowSuite,
    /SCCACHE_S3_RW_MODE/,
    "sccache has no S3 read-only switch; untrusted jobs must not enable the S3 tier",
  );
  assert.match(setupBlacksmithAction, /SCCACHE_MULTILEVEL_CHAIN=disk,s3/);
  assert.match(setupBlacksmithAction, /SCCACHE_MULTILEVEL_WRITE_ERROR_POLICY=l0/);
});

test("entry workflow grants credentialed cross-ref caches to main and trusted stacked PRs", () => {
  const cases = [
    [
      "main push",
      { eventName: "push", ref: "refs/heads/main" },
      { invocation: "trusted", idToken: "write", sccache: "writer", turbo: true },
    ],
    [
      "feature push",
      { eventName: "push", ref: "refs/heads/feature/cache-auth" },
      { invocation: "untrusted", idToken: "none", sccache: "none", turbo: false },
    ],
    [
      "trusted same-repository PR",
      {
        eventName: "pull_request",
        ref: "refs/pull/1/merge",
        sameRepository: true,
        authorAssociation: "MEMBER",
      },
      { invocation: "trusted", idToken: "write", sccache: "reader", turbo: true },
    ],
    [
      "trusted upper stack layer",
      {
        eventName: "pull_request",
        ref: "refs/pull/2/merge",
        sameRepository: true,
        authorAssociation: "COLLABORATOR",
      },
      { invocation: "trusted", idToken: "write", sccache: "reader", turbo: true },
    ],
    [
      "outside contributor on same repository",
      {
        eventName: "pull_request",
        ref: "refs/pull/3/merge",
        sameRepository: true,
        authorAssociation: "CONTRIBUTOR",
      },
      { invocation: "untrusted", idToken: "none", sccache: "none", turbo: false },
    ],
    [
      "fork PR",
      {
        eventName: "pull_request",
        ref: "refs/pull/4/merge",
        sameRepository: false,
        authorAssociation: "MEMBER",
      },
      { invocation: "untrusted", idToken: "none", sccache: "none", turbo: false },
    ],
    [
      "Dependabot PR",
      {
        eventName: "pull_request",
        ref: "refs/pull/5/merge",
        sameRepository: true,
        authorAssociation: "CONTRIBUTOR",
      },
      { invocation: "untrusted", idToken: "none", sccache: "none", turbo: false },
    ],
    [
      "manual main",
      { eventName: "workflow_dispatch", ref: "refs/heads/main" },
      { invocation: "untrusted", idToken: "none", sccache: "none", turbo: false },
    ],
    [
      "manual feature",
      { eventName: "workflow_dispatch", ref: "refs/heads/feature/cache-auth" },
      { invocation: "untrusted", idToken: "none", sccache: "none", turbo: false },
    ],
  ];

  for (const [name, input, expected] of cases)
    assert.deepEqual(cacheAccessFor(input), expected, name);

  assertEntryCacheTrustBoundary(workflow);
  assert.doesNotMatch(
    workflowSuite,
    /^\s+id-token:/m,
    "the reusable suite must inherit the caller's job-level OIDC ceiling",
  );
});

test("Blacksmith and cache trust contracts reject planted unsafe changes", () => {
  const typescript = job("test-ts");
  assert.throws(
    () => assertUsesBlacksmithRunner("test-ts", typescript.replace("blacksmith-16vcpu", "jazz-ci")),
    /blacksmith-16vcpu/,
  );
  assert.throws(
    () => assertEntryCacheTrustBoundary(workflow.replace("id-token: none", "id-token: write")),
    /strictly deep-equal/,
  );
  assert.throws(
    () =>
      assertEntryCacheTrustBoundary(
        workflow.replace(
          "\n  trusted:\n",
          '\n  "credential:escape":\n    permissions:\n      id-token: write\n    runs-on: ubuntu-latest\n    env:\n      AWS_SECRET_ACCESS_KEY: ${{ secrets.CACHE_ESCAPE_KEY }}\n    steps:\n      - run: echo unsafe\n\n  trusted:\n',
        ),
      ),
    /credential boundary must account for every entry-workflow job/,
  );
  assert.throws(
    () =>
      assertEntryCacheTrustBoundary(
        workflow.replace(trustedCacheCondition, "github.event_name != 'pull_request'"),
      ),
    /strictly equal/,
  );
  assert.throws(
    () =>
      assertSuiteS3ConfigurationBoundary(
        workflowSuite.replace(
          "env:\n  CACHE_SCOPE_REPOSITORY_ID:",
          "env:\n  SCCACHE_BUCKET: unsafe\n  CACHE_SCOPE_REPOSITORY_ID:",
        ),
      ),
    /untrusted suite callers must not inherit SCCACHE_BUCKET/,
  );
  assert.throws(
    () =>
      assertSuiteS3ConfigurationBoundary(
        workflowSuite.replace(
          `name: Export trusted sccache configuration\n        if: ${sccacheS3}`,
          "name: Export trusted sccache configuration\n        if: true",
        ),
      ),
    /strictly equal/,
  );
  assert.throws(
    () =>
      assertSccacheTrustBoundary(
        typescript.replace("sccache-s3: ${{ " + sccacheS3 + " }}", "sccache-s3: true"),
      ),
    /sccache-s3/,
  );
  assert.throws(
    () =>
      assertTurboCredentialConditions(
        typescript.replace("if: inputs.trusted-cache\n        env:", "if: true\n        env:"),
      ),
    /if:/,
  );
  assert.throws(
    () =>
      assertTurboSigningKeyTrustBoundary(
        typescript.replace(
          "    steps:\n",
          "    env:\n      TURBO_REMOTE_CACHE_SIGNATURE_KEY: ${{ secrets.TURBO_REMOTE_CACHE_SIGNATURE_KEY }}\n    steps:\n",
        ),
      ),
    /job environment must not expose the signing key/,
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
    "node --test dev/gates/test/ci-rust-throughput.test.mjs dev/gates/test/ci-tool-bundle.test.mjs dev/gates/test/test-artifact-pipeline.test.mjs dev/gates/test/release-gates.test.mjs && node dev/gates/test-burndown-ts.mjs",
  );
  assert.match(lint, /run: pnpm test:ci-workflow/);
});

test("CodSpeed caches the root-workspace Cargo target", () => {
  const rootTarget = /workspaces: \. -> target/;
  assert.match(codspeedWorkflow, rootTarget);
  assert.throws(
    () =>
      assert.match(
        codspeedWorkflow.replace(
          "workspaces: . -> target",
          "workspaces: examples/benchmarks/smoke -> target",
        ),
        rootTarget,
      ),
    /workspaces/,
  );
});

test("CodSpeed runs nightly on main and only for benchmark-labeled PRs", () => {
  const document = parse(codspeedWorkflow);
  assert.equal(document.on.push, undefined, "ordinary main pushes must not run CodSpeed");
  assert.deepEqual(document.on.pull_request, {
    types: ["labeled", "synchronize", "reopened"],
  });
  assert.deepEqual(document.on.schedule, [{ cron: "17 3 * * *" }]);
  assert.equal(document.on.workflow_dispatch, null);
  assert.equal(
    document.jobs.examples.if,
    "github.event_name != 'pull_request' || contains(github.event.pull_request.labels.*.name, 'benchmark')",
  );

  assert.throws(() => {
    const unsafe = parse(
      codspeedWorkflow.replace("  schedule:\n", "  push:\n    branches: [main]\n  schedule:\n"),
    );
    assert.equal(unsafe.on.push, undefined, "ordinary main pushes must not run CodSpeed");
  }, /ordinary main pushes/);
  assert.throws(() => {
    const unsafe = parse(
      codspeedWorkflow.replace(
        "github.event_name != 'pull_request' || contains(github.event.pull_request.labels.*.name, 'benchmark')",
        "true",
      ),
    );
    assert.equal(
      unsafe.jobs.examples.if,
      "github.event_name != 'pull_request' || contains(github.event.pull_request.labels.*.name, 'benchmark')",
    );
  }, /strictly equal/);
});

test("benchmark correctness is CI-gated while CodSpeed scope stays explicit", () => {
  const workspace = job("test-rust-workspace");
  assert.match(
    workspace,
    /name: Benchmark API and deterministic scenario smoke\s+run: dev\/gates\/benchmark-smoke\.sh --ci/,
  );
  assert.match(benchmarkSmokeGate, /cargo check -p jazz --benches --features testing/);
  assert.match(benchmarkSmokeGate, /cargo check -p jazz-sim --benches/);
  assert.match(benchmarkSmokeGate, /cargo metadata --no-deps --format-version 1/);
  assert.match(benchmarkSmokeGate, /required-features/);
  assert.match(
    benchmarkSmokeGate,
    /cargo test -p jazz --features testing --test legacy_benchmark_smoke/,
  );
  assert.match(benchmarkSmokeGate, /cargo test -p jazz-sim --test scenario_smoke/);
  assert.doesNotMatch(benchmarkSmokeGate, /^\s*cargo bench|^\s*.*--release/m);
  assert.throws(
    () =>
      assert.match(
        benchmarkSmokeGate.replace("cargo test -p jazz-sim --test scenario_smoke", "true"),
        /cargo test -p jazz-sim --test scenario_smoke/,
      ),
    /scenario_smoke/,
  );
  assert.throws(
    () =>
      assert.match(
        benchmarkSmokeGate.replace('.["required-features"][]?', ""),
        /required-features/,
      ),
    /required-features/,
  );
});

test("realistic timing retains every retired legacy smoke suite", () => {
  for (const bench of [
    "cold_subscription",
    "sync",
    "validation",
    "relation_include_delivery",
    "route_subscription_curve",
    "micro",
    "s8_branch_views",
  ]) {
    assert.match(realisticWorkflow, new RegExp(`--bench ${bench}(?: |\\n|$)`));
  }
  assert.match(realisticWorkflow, /Run legacy Jazz timing suites/);
  assert.match(realisticWorkflow, /status_file=bench-out\/native\/legacy-jazz\/status\.tsv/);
  assert.match(realisticWorkflow, /status: statuses\.get\(name\) \?\? "failed"/);
  assert.match(realisticWorkflow, /exit "\$failed"/);
  assert.throws(
    () =>
      assert.match(
        realisticWorkflow.replaceAll("--bench route_subscription_curve", ""),
        /--bench route_subscription_curve/,
      ),
    /route_subscription_curve/,
  );
  assert.throws(
    () =>
      assert.match(
        realisticWorkflow.replace('status: statuses.get(name) ?? "failed"', 'status: "passed"'),
        /statuses\.get\(name\)/,
      ),
    /statuses/,
  );
});

test("CodSpeed builds and runs the BigLabel benchmark variant", () => {
  for (const command of ["build", "run"]) {
    assert.match(
      codspeedWorkflow,
      new RegExp(
        `cargo codspeed ${command} --package jazz-example-benchmark-smoke --package jazz-example-big-label-benchmark`,
      ),
    );
  }
  assert.throws(
    () =>
      assert.match(
        codspeedWorkflow.replaceAll(" --package jazz-example-big-label-benchmark", ""),
        /jazz-example-big-label-benchmark/,
      ),
    /jazz-example-big-label-benchmark/,
  );
});

test("CodSpeed builds and runs the BandChat benchmark variant", () => {
  for (const command of ["build", "run"]) {
    assert.match(
      codspeedWorkflow,
      new RegExp(`cargo codspeed ${command} [^\\n]*--package jazz-example-band-chat-benchmark`),
    );
  }
  assert.throws(
    () =>
      assert.match(
        codspeedWorkflow.replaceAll(" --package jazz-example-band-chat-benchmark", ""),
        /jazz-example-band-chat-benchmark/,
      ),
    /jazz-example-band-chat-benchmark/,
  );
});

test("Windows NAPI release builds provision libclang for RocksDB bindgen", () => {
  const windowsNapiSetup =
    /name: Install libclang for Windows bindgen[\s\S]*if: matrix\.platform == 'win32-x64-msvc'[\s\S]*choco install llvm --version=21\.1\.8 --yes --no-progress --limit-output[\s\S]*Test-Path \(Join-Path \$libclangPath "libclang\.dll"\)[\s\S]*LIBCLANG_PATH=\$libclangPath[\s\S]*\$env:GITHUB_PATH/;
  assert.match(packageBuild, windowsNapiSetup);
  assert.throws(
    () =>
      assert.match(packageBuild.replace('"LIBCLANG_PATH=$libclangPath" | ', ""), windowsNapiSetup),
    /LIBCLANG_PATH/,
  );
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
  assert.match(runner, /setsid bash -c "\$\{node_tests_command\}" >"\$\{node_tests_log\}" 2>&1 &/);
  assert.match(
    runner,
    /setsid bash -c "\$\{browser_tests_command\}" >"\$\{browser_tests_log\}" 2>&1 &/,
  );
  assert.match(runner, /trap 'interrupt 130' INT/);
  assert.match(runner, /trap 'interrupt 143' TERM/);
  assert.match(runner, /kill -TERM -- "-\$\{child_pid\}"/);
  assert.match(runner, /wait "\$\{node_tests_pid\}"/);
  assert.match(runner, /node_tests_status=\$\?/);
  assert.match(runner, /wait "\$\{browser_tests_pid\}"/);
  assert.match(runner, /browser_tests_status=\$\?/);
  assert.match(runner, /cat "\$\{node_tests_log\}"/);
  assert.match(runner, /cat "\$\{browser_tests_log\}"/);
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
