import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { parse } from "yaml";
import { cleanDist } from "../../../packages/jazz-tools/scripts/clean-dist.mjs";
import { missingJazzToolsTestSurface } from "../verify-jazz-tools-exports.mjs";

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
const previewBuild = fs.readFileSync(
  path.join(root, ".github/workflows/preview-build.yml"),
  "utf8",
);
const codspeedWorkflow = fs.readFileSync(path.join(root, ".github/workflows/codspeed.yml"), "utf8");
const realisticWorkflow = fs.readFileSync(
  path.join(root, ".github/workflows/benchmarks.yml"),
  "utf8",
);
const rustShadowWorkflow = fs.readFileSync(
  path.join(root, ".github/workflows/ci-rust-shadow.yml"),
  "utf8",
);
const rustShadowLauncher = fs.readFileSync(
  path.join(root, "dev/gates/rust-shadow-matrix.mjs"),
  "utf8",
);
const rnNativeArtifactsWorkflow = fs.readFileSync(
  path.join(root, ".github/workflows/rn-native-artifacts.yml"),
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
const benchmarkSmokeMode = (mode) => {
  const start = `if [[ "\${1:-}" == "--${mode}" && $# == 1 ]]; then`;
  const startIndex = benchmarkSmokeGate.indexOf(start);
  assert.notEqual(startIndex, -1, `missing --${mode} benchmark smoke mode`);
  const endIndex = benchmarkSmokeGate.indexOf("\nfi", startIndex);
  assert.notEqual(endIndex, -1, `unterminated --${mode} benchmark smoke mode`);
  return benchmarkSmokeGate.slice(startIndex + start.length, endIndex);
};
const assertUsesBlacksmithRunner = (jobName, jobSource) => {
  const cpu = jobName === "test-ts" ? 16 : 4;
  assert.match(jobSource, new RegExp(`runs-on: blacksmith-${cpu}vcpu-ubuntu-2404`));
  assert.doesNotMatch(jobSource, /^    runs-on: jazz-ci$/m);
};
const integrationCheckStep = (typescriptJob) => {
  const start = typescriptJob.indexOf("name: Run CI-equivalent TypeScript and workspace partition");
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

test("continuous soak precompiles outside seed watchdogs and preserves failure artifacts", () => {
  const source = fs.readFileSync(
    path.join(root, ".github/workflows/continuous-simulation-soak.yml"),
    "utf8",
  );
  const parsed = parse(source);
  const steps = parsed.jobs.soak.steps;
  const precompile = steps.find((step) => step.name === "Precompile exact Jazz soak test binary");
  const assertPrecompileCommand = (run) => {
    assert.match(run, /timeout --kill-after=30s "\$\{PRECOMPILE_TIMEOUT_SECONDS\}s"/);
    assert.match(
      run,
      /cargo test -p jazz --lib --no-default-features \\\n\s+--features testing,transport-compression-zstd --no-run \\\n/,
    );
    assert.doesNotMatch(run, /--no-exec/);
  };
  assert.ok(precompile, "missing named cold precompile step");
  assertPrecompileCommand(precompile.run);
  assert.throws(
    () => assertPrecompileCommand(precompile.run.replace("--no-run", "--no-exec")),
    /does not match|match the regular expression/,
    "contract must reject a planted precompile flag regression",
  );
  for (const id of ["shard_one", "shard_two"])
    assert.equal(
      steps.find((step) => step.id === id)?.if,
      "steps.precompile.outcome == 'success'",
      `${id} must require successful precompile`,
    );
  assert.equal(steps.find((step) => step.name === "Upload soak receipts")?.if, "always()");
  assert.match(
    steps.find((step) => step.name === "Fail job when a shard failed")?.if,
    /steps\.precompile\.outcome == 'failure'/,
  );
  assert.match(precompile.run, /phase:\s*"precompile"/);
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

test("Rust CI keeps the bounded real differential oracle in its shared command partition", () => {
  const workspace = job("test-rust-workspace");
  const differential = job("test-rust-differential");
  const aggregate = job("test-rust");
  const localCi = fs.readFileSync(path.join(root, "dev/gates/local-ci-equivalent.mjs"), "utf8");

  assert.match(workspace, /local-ci-equivalent\.mjs --ci-partition rust-workspace/);
  assert.match(differential, /local-ci-equivalent\.mjs --ci-partition rust-differential/);
  assert.match(
    localCi,
    /run-rust-tests\.mjs[\s\S]*--timeout-seconds[\s\S]*780[\s\S]*--nextest-profile[\s\S]*jazz-ci/,
  );
  assert.match(
    localCi,
    /cargo test -p jazz --lib --features testing,transport-compression-zstd --no-run --message-format=json/,
  );
  assert.match(localCi, /message\.target\.name === "jazz"/);
  assert.match(
    localCi,
    /timeout 60s env[\s\S]*JAZZ_SEED=11[\s\S]*JAZZ_DIFFERENTIAL_CHURN_DEPTHS=10,1000[\s\S]*JAZZ_DIFFERENTIAL_STEP_COUNT=3[\s\S]*m3_maintained_one_shot_differential_oracle --exact --ignored/,
  );
  assert.match(
    m3Differential,
    /#\[ignore = "#\d+: manual randomized differential soak; bounded seed 11 runs in CI"\]\n(?:pub )?fn m3_maintained_one_shot_differential_oracle/,
  );
  assert.match(aggregate, /if: always\(\)/);
  assert.match(aggregate, /needs: \[test-rust-workspace, test-rust-differential\]/);
  assert.match(aggregate, /test "\$\{WORKSPACE_RESULT\}" = success/);
  assert.match(aggregate, /test "\$\{DIFFERENTIAL_RESULT\}" = success/);
  assert.match(differential, /rust-cache: "false"/);
  assert.throws(
    () => assert.match(localCi.replace("--exact --ignored", "--ignored"), /--exact --ignored/),
    /exact/,
  );
});

test("the non-required Rust throughput shadow proves two exact hash partitions and folds M3 once", () => {
  const document = parse(rustShadowWorkflow);
  const shard = document.jobs.shard;
  const aggregate = document.jobs.aggregate;
  assert.deepEqual(document.permissions, { contents: "read", packages: "read" });
  assert.deepEqual(
    document.on.pull_request,
    { types: ["opened", "reopened", "synchronize", "labeled", "unlabeled"] },
    "adding benchmark must start its shadow without a new push, while removing it cancels queued work",
  );
  assert.deepEqual(document.on.push, { branches: ["main"] });
  assert.equal(document.on.workflow_dispatch, null);
  assert.match(
    shard.if,
    /github\.event_name != 'pull_request' \|\| contains\(github\.event\.pull_request\.labels\.\*\.name, 'benchmark'\)/,
    "the expensive non-required shadow must run on PRs only when deliberately labelled",
  );
  assert.deepEqual(shard.strategy.matrix.index, [1, 2]);
  assert.equal(shard.strategy["max-parallel"], 2);
  assert.equal(shard["runs-on"], "blacksmith-8vcpu-ubuntu-2404");
  assert.equal(aggregate.needs[0], "shard");
  assert.match(aggregate.if, /always/);
  assert.match(
    aggregate.if,
    /github\.event_name != 'pull_request' \|\| contains\(github\.event\.pull_request\.labels\.\*\.name, 'benchmark'\)/,
    "the aggregate must not fail when intentionally unlabelled PR shards are skipped",
  );
  assert.equal(
    aggregate.steps[0].uses,
    "actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd",
    "aggregate checkout must precede artifact download because checkout cleans the worktree",
  );
  assert.equal(
    aggregate.steps[1].uses,
    "actions/download-artifact@634f93cb2916e3fdff6788551b99b062d0335ce0",
  );
  assert.match(
    aggregate.steps[2].run,
    /node dev\/gates\/rust-shadow-matrix\.mjs aggregate rust-shadow-receipts 2/,
  );
  assert.match(
    rustShadowWorkflow,
    /node dev\/gates\/rust-shadow-matrix\.mjs shard \$\{\{ matrix\.index \}\} 2/,
  );
  assert.match(
    rustShadowWorkflow,
    /node dev\/gates\/rust-shadow-matrix\.mjs aggregate rust-shadow-receipts 2 rust-shadow-receipts\/aggregate\.json "\$\{\{ github\.sha \}\}"/,
  );
  assert.match(rustShadowWorkflow, /merge-multiple: true/);
  assert.match(rustShadowLauncher, /--require-nextest/);
  assert.match(rustShadowLauncher, /RUST_MIN_STACK.*4 \* 1024 \* 1024/);
  assert.match(rustShadowLauncher, /"--workspace",\s*"--lib",\s*"--bins",\s*"--tests"/);
  assert.match(rustShadowLauncher, /"--kill-after=30s",\s*"60s",\s*"env"/);
  assert.match(rustShadowLauncher, /JAZZ_SEED: "11"/);
  const exactM3Invocation = /testBinary,\s*m3TestName,\s*"--exact",\s*"--ignored"/;
  assert.match(rustShadowLauncher, exactM3Invocation);
  assert.match(rustShadowLauncher, /"--no-run",\s*"--message-format=json"/);
  assert.match(rustShadowLauncher, /runner: "compiled-libtest"/);
  assert.match(rustShadowLauncher, /m3\.length !== 1[\s\S]*m3\[0\]\.shard\.index !== 1/);
  assert.match(rustShadowLauncher, /test belongs to more than one shard/);
  assert.match(rustShadowLauncher, /hash shards do not cover the exact executable inventory/);
  assert.match(rustShadowLauncher, /sourceIdentity\(root\)/);
  assert.match(rustShadowLauncher, /clean-source-baseline RECEIPT EXPECTED_COMMIT/);
  assert.match(
    rustShadowLauncher,
    /dependency setup changed the checked-out source after the shadow baseline/,
  );
  assert.match(
    rustShadowLauncher,
    /shadow execution changed the checked-out source after the baseline/,
  );
  assert.match(rustShadowWorkflow, /Seal clean checked-out source baseline/);
  assert.match(
    rustShadowWorkflow,
    /RUST_SHADOW_SOURCE_BASELINE: \$\{\{ runner\.temp \}\}\/rust-shadow-source\.json/,
  );
  assert.match(rustShadowLauncher, /shards disagree on the checked-out source identity/);
  assert.match(rustShadowLauncher, /workflow event commit/);
  assert.match(
    rustShadowLauncher,
    /partition test receipt command does not match the exact shard selector/,
  );
  assert.throws(
    () => assert.match(rustShadowLauncher.replace("JAZZ_SEED=11", "JAZZ_SEED=12"), /JAZZ_SEED=11/),
    /JAZZ_SEED/,
  );
  assert.throws(
    () =>
      assert.match(
        rustShadowLauncher.replace("--require-nextest", "--nextest-optional"),
        /--require-nextest/,
      ),
    /require-nextest/,
  );
  assert.throws(
    () =>
      assert.match(
        rustShadowLauncher.replace(
          "testBinary,\n            m3TestName,",
          '"cargo",\n            "test",',
        ),
        exactM3Invocation,
      ),
    /m3_maintained_one_shot_differential_oracle/,
  );

  const source = {
    commit: "a".repeat(40),
    headTree: "b".repeat(40),
    indexTree: "b".repeat(40),
    unstaged: "c".repeat(64),
    untracked: "d".repeat(64),
    dirty: false,
  };
  const sourceFingerprint = (value) =>
    crypto
      .createHash("sha256")
      .update(
        ["headTree", "indexTree", "unstaged", "untracked"]
          .map((field) => `${field}\0${value[field]}\0`)
          .join(""),
      )
      .digest("hex");
  source.fingerprint = sourceFingerprint(source);
  const expectedCommand = (index) => [
    "cargo",
    "nextest",
    "run",
    "--profile",
    "jazz-ci",
    "--no-fail-fast",
    "--partition",
    `hash:${index}/2`,
    "--workspace",
    "--lib",
    "--bins",
    "--tests",
    "--features",
    "jazz/testing,jazz/transport-compression-zstd,jazz-server/test,jazz-cli/test",
  ];
  const shardReceipt = (index) => ({
    kind: "rust-shadow-shard-receipt",
    status: "passed",
    shard: { index, count: 2, partition: `hash:${index}/2` },
    testArgs: expectedCommand(index).slice(8),
    source: structuredClone(source),
    inventory: {
      all: ["binary\\0one", "binary\\0two"],
      selected: [index === 1 ? "binary\\0one" : "binary\\0two"],
    },
    testReceipt: {
      status: "passed",
      runner: "cargo-nextest",
      nextestProfile: "jazz-ci",
      shard: { index, count: 2 },
      command: expectedCommand(index),
      source: structuredClone(source),
      environment: { rustMinStack: String(4 * 1024 * 1024) },
    },
    m3:
      index === 1
        ? {
            seed: 11,
            status: "passed",
            runner: "compiled-libtest",
            testName: "node::tests::harness::m3_maintained_one_shot_differential_oracle",
            testArgs: ["--exact", "--ignored"],
            environment: {
              JAZZ_SEED: "11",
              JAZZ_DIFFERENTIAL_CHURN_DEPTHS: "10,1000",
              JAZZ_DIFFERENTIAL_STEP_COUNT: "3",
            },
          }
        : { status: "not-assigned" },
  });
  const runAggregate = (mutate) => {
    const directory = fs.mkdtempSync(path.join(os.tmpdir(), "jazz-shadow-receipts-"));
    const shards = [shardReceipt(1), shardReceipt(2)];
    mutate?.(shards);
    for (const [index, receipt] of shards.entries())
      fs.writeFileSync(path.join(directory, `shard-${index + 1}.json`), JSON.stringify(receipt));
    const result = spawnSync(
      "node",
      [
        rustShadowLauncherPath,
        "aggregate",
        directory,
        "2",
        path.join(directory, "aggregate.json"),
        source.commit,
      ],
      { encoding: "utf8" },
    );
    fs.rmSync(directory, { recursive: true, force: true });
    return result;
  };
  const rustShadowLauncherPath = path.join(root, "dev/gates/rust-shadow-matrix.mjs");
  assert.equal(runAggregate().status, 0);
  {
    const directory = path.join(os.tmpdir(), `jazz-shadow-missing-${crypto.randomUUID()}`);
    const aggregatePath = path.join(directory, "aggregate.json");
    const result = spawnSync(
      "node",
      [rustShadowLauncherPath, "aggregate", directory, "2", aggregatePath, source.commit],
      { encoding: "utf8" },
    );
    assert.notEqual(result.status, 0, "missing downloaded shard receipts must fail closed");
    assert.match(result.stderr, /expected 2 shard receipts, found 0/);
    const missing = JSON.parse(fs.readFileSync(aggregatePath, "utf8"));
    assert.equal(missing.status, "failed");
    assert.equal(missing.shardCount, 2);
    assert.equal(missing.expectedCommit, source.commit);
    assert.match(missing.error, /expected 2 shard receipts, found 0/);
    fs.rmSync(directory, { recursive: true, force: true });
  }
  for (const [name, mutate, message] of [
    ["runner", (shards) => (shards[0].testReceipt.runner = "cargo-fallback"), /Nextest shard/],
    [
      "source fingerprint",
      (shards) => (shards[0].testReceipt.source.fingerprint = "f".repeat(64)),
      /source fingerprint/,
    ],
    [
      "receipt profile",
      (shards) => (shards[0].testReceipt.nextestProfile = "jazz"),
      /Nextest shard/,
    ],
    [
      "command profile",
      (shards) => (shards[0].testReceipt.command[4] = "jazz"),
      /exact shard selector/,
    ],
    [
      "no-fail-fast",
      (shards) => (shards[0].testReceipt.command[5] = "--fail-fast"),
      /exact shard selector/,
    ],
    [
      "partition",
      (shards) => (shards[0].testReceipt.command[7] = "hash:1\/3"),
      /exact shard selector/,
    ],
    ["test arguments", (shards) => shards[0].testArgs.pop(), /test arguments/],
    [
      "M3 receipt semantics",
      (shards) => (shards[0].m3.runner = "cargo-test"),
      /maintained M3 seed 11/,
    ],
    [
      "nested receipt source identity",
      (shards) => {
        shards[0].testReceipt.source.untracked = "e".repeat(64);
        shards[0].testReceipt.source.fingerprint = sourceFingerprint(shards[0].testReceipt.source);
      },
      /partition test receipt source untracked does not match its inventory receipt/,
    ],
    [
      "cross-shard source identity",
      (shards) => {
        shards[1].source.untracked = "e".repeat(64);
        shards[1].source.fingerprint = sourceFingerprint(shards[1].source);
        shards[1].testReceipt.source = structuredClone(shards[1].source);
      },
      /shards disagree on the checked-out source identity/,
    ],
    [
      "workflow event commit",
      (shards) => {
        shards[0].source.commit = "f".repeat(40);
        shards[0].testReceipt.source = structuredClone(shards[0].source);
      },
      /workflow event commit/,
    ],
  ]) {
    const result = runAggregate(mutate);
    assert.notEqual(result.status, 0, `planted ${name} mismatch must fail`);
    assert.match(
      result.stderr,
      message,
      `planted ${name} mismatch must identify the violated binding`,
    );
  }
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
    /name: Run CI-equivalent TypeScript and workspace partition\s+run: node dev\/gates\/local-ci-equivalent\.mjs --ci-partition typescript/,
  );
  assertIntegrationCheckIsGating(typescript);
  assert.ok(
    typescript.indexOf("name: Run CI-equivalent TypeScript and workspace partition") !== -1,
    "workspace check and artifacts must use the shared CI-equivalent partition",
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
    "name: Run CI-equivalent TypeScript and workspace partition\n        run: node dev/gates/local-ci-equivalent.mjs --ci-partition typescript";

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
  assert.match(lint, /local-ci-equivalent\.mjs --ci-partition lint/);
  assert.doesNotMatch(lint, /^\s*- run: cargo clippy/m);
});

test("CI runs the workflow contract test through its package script", () => {
  const lint = job("lint");
  assert.equal(
    packageJson.scripts["test:ci-workflow"],
    "node --test dev/gates/test/ci-rust-throughput.test.mjs dev/gates/test/local-ci-equivalent.test.mjs dev/gates/test/ci-tool-bundle.test.mjs dev/gates/test/test-artifact-pipeline.test.mjs dev/gates/test/release-gates.test.mjs dev/gates/test/jazz-rn-packaging.test.mjs dev/artifacts/provenance.test.mjs dev/artifacts/wasm-build-contract.test.mjs dev/artifacts/napi-build-contract.test.mjs dev/artifacts/release-staging-contract.test.mjs && node dev/gates/ignored-tests.mjs --self-test",
  );
  assert.match(lint, /local-ci-equivalent\.mjs --ci-partition lint/);
});

test("CodSpeed caches the root-workspace Cargo target", () => {
  const rootTarget = /workspaces: \. -> target/;
  assert.match(codspeedWorkflow, rootTarget);
  assert.throws(
    () =>
      assert.match(
        codspeedWorkflow.replace(
          /workspaces: \. -> target/g,
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

test("React Native artifact builds are explicit same-repository label opt-ins", () => {
  const document = parse(rnNativeArtifactsWorkflow);
  assert.deepEqual(document.on.pull_request, {
    types: ["opened", "reopened", "synchronize", "labeled", "unlabeled"],
  });
  assert.deepEqual(document.on.push, {
    branches: ["main"],
    paths: ["crates/jazz-native-relay/**", "crates/jazz-storage-sqlite/**", "crates/jazz-rn/**"],
  });
  assert.equal(document.on.workflow_dispatch, undefined);
  assert.equal(document.on.schedule, undefined);
  assert.deepEqual(document.permissions, { contents: "read" });
  assert.equal(document.permissions["id-token"], undefined);
  assert.doesNotMatch(rnNativeArtifactsWorkflow, /\bsecrets\./);
  assert.doesNotMatch(rnNativeArtifactsWorkflow, /SCCACHE_(?:TRUSTED|PR)_/);
  assert.deepEqual(document.concurrency, {
    group: "rn-native-artifacts-${{ github.event.pull_request.number || github.ref }}",
    "cancel-in-progress": "${{ github.event_name == 'pull_request' }}",
  });

  const expectedCondition =
    "${{ github.event_name != 'pull_request' || ( contains(github.event.pull_request.labels.*.name, 'react-native') && github.event.pull_request.head.repo.full_name == github.repository ) }}";
  const normalizeCondition = (condition) => condition.replace(/\s+/g, " ").trim();
  for (const jobName of ["android", "ios"]) {
    const job = document.jobs[jobName];
    assert.equal(normalizeCondition(job.if), expectedCondition, `${jobName} must be label-gated`);
  }

  assert.throws(() => {
    const unsafe = parse(
      rnNativeArtifactsWorkflow.replace(
        "&& github.event.pull_request.head.repo.full_name == github.repository",
        "",
      ),
    );
    assert.equal(
      normalizeCondition(unsafe.jobs.android.if),
      expectedCondition,
      "a labeled fork must not execute native artifact builds",
    );
  }, /a labeled fork/);
  assert.throws(() => {
    const unsafe = parse(
      rnNativeArtifactsWorkflow.replace(
        "cancel-in-progress: ${{ github.event_name == 'pull_request' }}",
        "cancel-in-progress: false",
      ),
    );
    assert.equal(
      unsafe.concurrency?.["cancel-in-progress"],
      "${{ github.event_name == 'pull_request' }}",
      "label removal must cancel queued artifact work",
    );
  }, /label removal/);
});

test("benchmark correctness stays on ordinary CI while API compilation uses realistic benchmarks", () => {
  const workspace = job("test-rust-workspace");
  const scenarioMode = benchmarkSmokeMode("ci");
  const compileMode = benchmarkSmokeMode("compile-ci");
  assert.match(workspace, /local-ci-equivalent\.mjs --ci-partition rust-workspace/);
  assert.match(
    realisticWorkflow,
    /name: Compile maintained benchmark APIs\s+run: dev\/gates\/benchmark-smoke\.sh --compile-ci/,
  );
  assert.match(
    compileMode,
    /run_phase jazz-benchmark-api cargo check -p jazz --benches --features testing/,
  );
  assert.match(compileMode, /run_phase jazz-sim-benchmark-api cargo check -p jazz-sim --benches/);
  assert.doesNotMatch(scenarioMode, /cargo check -p (?:jazz|jazz-sim) --benches/);
  assert.doesNotMatch(compileMode, /cargo test -p (?:jazz|jazz-sim)/);
  assert.match(benchmarkSmokeGate, /cargo metadata --no-deps --format-version 1/);
  assert.match(benchmarkSmokeGate, /required-features/);
  assert.match(scenarioMode, /cargo test -p jazz --features testing --test legacy_benchmark_smoke/);
  assert.match(scenarioMode, /cargo test -p jazz-sim --test scenario_smoke/);
  assert.match(benchmarkSmokeGate, /benchmark-smoke phase=%s duration_seconds=%s status=%s/);
  assert.throws(
    () =>
      assert.match(
        scenarioMode.replace("cargo test -p jazz-sim --test scenario_smoke", "true"),
        /cargo test -p jazz-sim --test scenario_smoke/,
      ),
    /scenario_smoke/,
  );
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

test("realistic benchmark compilation has an explicit guarded trigger matrix", () => {
  const document = parse(realisticWorkflow);
  assert.deepEqual(document.on.pull_request, {
    branches: ["main"],
    types: ["opened", "reopened", "synchronize", "labeled"],
  });
  assert.deepEqual(document.on.push, { branches: ["main"] });
  assert.deepEqual(document.on.schedule, [{ cron: "0 5 * * *" }]);
  assert.equal(document.on.workflow_dispatch.inputs.profile.default, "s");
  assert.equal(document.on.workflow_dispatch.inputs.include_browser.type, "boolean");
  assert.deepEqual(document.permissions, { contents: "read", issues: "write" });

  const native = document.jobs.native;
  assert.deepEqual(native["runs-on"], ["self-hosted", "linux", "x64", "jazz-bench"]);
  assert.equal(native["timeout-minutes"], 180);
  const expectedNativeCondition =
    "${{ (github.event_name != 'push' || github.actor != 'github-actions[bot]') && ( github.event_name != 'pull_request' || ( contains(github.event.pull_request.labels.*.name, 'benchmark') && github.event.pull_request.head.repo.full_name == github.repository ) ) }}";
  const normalizeCondition = (condition) => condition.replace(/\s+/g, " ").trim();
  assert.equal(normalizeCondition(native.if), expectedNativeCondition);

  assert.throws(() => {
    const unsafe = parse(realisticWorkflow.replace('  schedule:\n    - cron: "0 5 * * *"\n', ""));
    assert.ok(unsafe.on.schedule, "nightly schedule must remain");
  }, /nightly schedule/);
  assert.throws(() => {
    const unsafe = parse(
      realisticWorkflow.replace(
        "github.event.pull_request.head.repo.full_name == github.repository",
        "true",
      ),
    );
    assert.equal(normalizeCondition(unsafe.jobs.native.if), expectedNativeCondition);
  }, /true/);
  assert.throws(() => {
    const unsafe = parse(
      realisticWorkflow.replace(
        "        )\n      }}\n    runs-on:",
        "        ) || true\n      }}\n    runs-on:",
      ),
    );
    assert.equal(normalizeCondition(unsafe.jobs.native.if), expectedNativeCondition);
  }, /true/);
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

test("pkg.pr.new previews omit Windows while release package builds retain it", () => {
  assert.match(packageBuild, /default: .*win32-x64-msvc/);
  assert.match(packageBuild, /include: \$\{\{ fromJSON\(inputs\.napi_matrix\) \}\}/);
  assert.match(packageBuild, /Remove Windows package omitted by this build/);
  assert.match(packageBuild, /stage-napi-manifests\.mjs linux-x64-gnu darwin-x64 darwin-arm64/);
  assert.doesNotMatch(previewBuild, /win32-x64-msvc/);
  assert.match(previewBuild, /napi_matrix: .*linux-x64-gnu.*darwin-x64.*darwin-arm64/);
});

test("TypeScript CI overlaps independent Node and browser suites after one artifact build", () => {
  const typescript = job("test-ts");
  const runner = fs.readFileSync(path.join(root, "dev/gates/run-ts-tests.sh"), "utf8");
  const localCi = fs.readFileSync(path.join(root, "dev/gates/local-ci-equivalent.mjs"), "utf8");
  assert.match(typescript, /local-ci-equivalent\.mjs --ci-partition typescript/);
  assert.match(localCi, /correctness-test artifacts[\s\S]*build:test-artifacts/);
  assert.match(localCi, /parallel Node and browser suites[\s\S]*run-ts-tests\.sh/);
  assert.match(runner, /require\('\.\/crates\/jazz-napi'\)/);
  assert.match(runner, /JAZZ_TEST_SEALED_TOOLS_DIST=1/);
  assert.match(runner, /verify-jazz-tools-exports\.mjs/);
  assert.match(runner, /public export surface is incomplete/);
  assert.match(runner, /Test children only\s+# consume those immutable artifacts/);
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

test("a sealed test surface rejects a child clean before it can delete prepared exports", async () => {
  const fixture = fs.mkdtempSync(path.join(os.tmpdir(), "jazz-sealed-tools-dist-"));
  const marker = path.join(fixture, "testing", "index.js");
  fs.mkdirSync(path.dirname(marker), { recursive: true });
  fs.writeFileSync(marker, "prepared export");
  const previous = process.env.JAZZ_TEST_SEALED_TOOLS_DIST;
  process.env.JAZZ_TEST_SEALED_TOOLS_DIST = "1";
  try {
    await assert.rejects(() => cleanDist(fixture), /sealed for concurrent tests/);
    assert.equal(fs.existsSync(marker), true, "sealed child build deleted a prepared export");
  } finally {
    if (previous === undefined) delete process.env.JAZZ_TEST_SEALED_TOOLS_DIST;
    else process.env.JAZZ_TEST_SEALED_TOOLS_DIST = previous;
    fs.rmSync(fixture, { recursive: true, force: true });
  }
});

test("Turbo preserves the sealed surface for the real Jazz Tools build task", () => {
  const result = spawnSync(
    "pnpm",
    ["exec", "turbo", "run", "build", "--filter=jazz-tools", "--only", "--force"],
    {
      cwd: root,
      encoding: "utf8",
      env: { ...process.env, JAZZ_TEST_SEALED_TOOLS_DIST: "1" },
    },
  );
  assert.notEqual(result.status, 0, "a sealed Jazz Tools build must not clean dist");
  assert.match(
    `${result.stdout}\n${result.stderr}`,
    /jazz-tools dist is sealed for concurrent tests/,
    "Turbo strict-env child dropped JAZZ_TEST_SEALED_TOOLS_DIST before clean-dist",
  );
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
        JAZZ_SKIP_JAZZ_TOOLS_BUILD: "1",
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

test("a missing prepared native artifact prevents both TypeScript suites from starting", () => {
  const runner = path.join(root, "dev/gates/run-ts-tests.sh");
  const fixture = fs.mkdtempSync(path.join(os.tmpdir(), "jazz-ts-ci-prebuild-"));
  const fakeNode = path.join(fixture, "node");
  const nodeMarker = path.join(fixture, "node-suite-ran");
  const browserMarker = path.join(fixture, "browser");
  try {
    fs.writeFileSync(fakeNode, "#!/bin/sh\nexit 23\n", { mode: 0o755 });
    const result = spawnSync("bash", [runner], {
      cwd: root,
      encoding: "utf8",
      env: {
        ...process.env,
        PATH: `${fixture}:${process.env.PATH}`,
        JAZZ_NODE_TEST_COMMAND: `touch ${JSON.stringify(nodeMarker)}`,
        JAZZ_BROWSER_TEST_COMMAND: `touch ${JSON.stringify(browserMarker)}`,
      },
    });
    assert.equal(result.status, 1, result.stderr);
    assert.equal(
      fs.existsSync(nodeMarker),
      false,
      "node suite started after failed artifact check",
    );
    assert.equal(
      fs.existsSync(browserMarker),
      false,
      "browser suite started after failed artifact check",
    );
    assert.match(result.stderr, /prepared release jazz-napi artifact did not load/);
  } finally {
    fs.rmSync(fixture, { recursive: true, force: true });
  }
});

test("the Jazz Tools preflight derives public exports and keeps test-only entrypoints explicit", () => {
  const fixture = fs.mkdtempSync(path.join(os.tmpdir(), "jazz-tools-test-surface-"));
  const packageRoot = path.join(fixture, "packages/jazz-tools");
  const write = (relative, contents = "export {};") => {
    const target = path.join(packageRoot, relative);
    fs.mkdirSync(path.dirname(target), { recursive: true });
    fs.writeFileSync(target, contents);
  };
  try {
    write(
      "package.json",
      JSON.stringify({
        exports: {
          ".": { types: "./dist/index.d.ts", default: "./dist/index.js" },
          "./react": { types: "./dist/react/index.d.ts", default: "./dist/react/index.js" },
          "./testing": { default: "./dist/testing/index.js" },
        },
      }),
    );
    for (const file of [
      "dist/index.d.ts",
      "dist/index.js",
      "dist/react/index.d.ts",
      "dist/react/index.js",
      "dist/testing/index.js",
      "dist/cli.js",
      "dist/runtime/client-session.js",
      "dist/backend/request-auth.js",
    ])
      write(file);
    assert.deepEqual(missingJazzToolsTestSurface(fixture), []);

    fs.rmSync(path.join(packageRoot, "dist/index.js"));
    assert.deepEqual(missingJazzToolsTestSurface(fixture), ["dist/index.js"]);
    write("dist/index.js");

    fs.rmSync(path.join(packageRoot, "dist/react/index.js"));
    assert.deepEqual(missingJazzToolsTestSurface(fixture), ["dist/react/index.js"]);
  } finally {
    fs.rmSync(fixture, { recursive: true, force: true });
  }
});

test("missing public root or framework exports prevent both TypeScript suites from starting", () => {
  const fixture = fs.mkdtempSync(path.join(os.tmpdir(), "jazz-ts-ci-public-export-"));
  const write = (relative, contents = "export {};") => {
    const target = path.join(fixture, relative);
    fs.mkdirSync(path.dirname(target), { recursive: true });
    fs.writeFileSync(target, contents);
  };
  const packageFiles = [
    "dist/index.d.ts",
    "dist/index.js",
    "dist/react/index.d.ts",
    "dist/react/index.js",
    "dist/testing/index.js",
    "dist/cli.js",
    "dist/runtime/client-session.js",
    "dist/backend/request-auth.js",
  ];
  try {
    // The real runner and verifier resolve from cwd, so this is a controlled
    // checkout rather than a mutation of the developer's generated dist. Lint
    // CI deliberately has no Jazz Tools build before it runs this contract.
    for (const source of ["run-ts-tests.sh", "verify-jazz-tools-exports.mjs"])
      write(`dev/gates/${source}`, fs.readFileSync(path.join(root, "dev/gates", source), "utf8"));
    write("crates/jazz-napi/package.json", JSON.stringify({ type: "commonjs" }));
    write("crates/jazz-napi/index.js", "module.exports = {};\n");
    write(
      "packages/jazz-tools/package.json",
      JSON.stringify({
        exports: {
          ".": { types: "./dist/index.d.ts", default: "./dist/index.js" },
          "./react": { types: "./dist/react/index.d.ts", default: "./dist/react/index.js" },
          "./testing": { default: "./dist/testing/index.js" },
        },
      }),
    );
    for (const file of packageFiles) write(`packages/jazz-tools/${file}`);

    for (const relative of ["dist/index.js", "dist/react/index.js"]) {
      const nodeMarker = path.join(fixture, "node-suite-ran");
      const browserMarker = path.join(fixture, "browser-suite-ran");
      fs.rmSync(nodeMarker, { force: true });
      fs.rmSync(browserMarker, { force: true });
      fs.rmSync(path.join(fixture, "packages/jazz-tools", relative));
      const result = spawnSync("bash", ["dev/gates/run-ts-tests.sh"], {
        cwd: fixture,
        encoding: "utf8",
        env: {
          ...process.env,
          JAZZ_NODE_TEST_COMMAND: `touch ${JSON.stringify(nodeMarker)}`,
          JAZZ_BROWSER_TEST_COMMAND: `touch ${JSON.stringify(browserMarker)}`,
        },
      });
      assert.equal(result.status, 1, `${relative}: ${result.stderr}`);
      assert.equal(fs.existsSync(nodeMarker), false, `${relative}: node suite started`);
      assert.equal(fs.existsSync(browserMarker), false, `${relative}: browser suite started`);
      assert.match(
        `${result.stdout}\n${result.stderr}`,
        new RegExp(`public export is missing: ${relative}`),
      );
      write(`packages/jazz-tools/${relative}`);
    }
  } finally {
    fs.rmSync(fixture, { recursive: true, force: true });
  }
});

test("TypeScript test children do not retain obsolete artifact fallback builds", () => {
  const todoServer = JSON.parse(
    fs.readFileSync(path.join(root, "examples/docs/todo-server-ts/package.json"), "utf8"),
  );
  assert.equal(todoServer.scripts.pretest, undefined);
  assert.doesNotMatch(JSON.stringify(todoServer.scripts), /jazz-napi build|jazz-tools build/);
});

test("parallel TypeScript runner terminates both child process groups", async () => {
  const fixture = fs.mkdtempSync(path.join(os.tmpdir(), "jazz-ts-ci-interrupt-"));
  const nodeMarker = path.join(fixture, "node-orphan");
  const browserMarker = path.join(fixture, "browser-orphan");
  const child = spawn("bash", [path.join(root, "dev/gates/run-ts-tests.sh")], {
    cwd: root,
    env: {
      ...process.env,
      JAZZ_SKIP_JAZZ_TOOLS_BUILD: "1",
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
