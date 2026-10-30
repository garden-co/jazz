import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { pathToFileURL } from "node:url";
import { parse } from "yaml";
import { cleanDist } from "../../../packages/jazz-tools/scripts/clean-dist.mjs";
import { missingJazzToolsTestSurface } from "../verify-jazz-tools-exports.mjs";
import { TARGETS as jazzToolsTargets } from "../../../packages/jazz-tools/scripts/targets.mjs";

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
const installRustToolScript = fs.readFileSync(
  path.join(root, "dev/ci/install-rust-tool.sh"),
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
const webkitIndexedDbReceipt = fs.readFileSync(
  path.join(root, ".github/workflows/ios-webkit-indexeddb-receipt.yml"),
  "utf8",
);
const codspeedWorkflow = fs.readFileSync(path.join(root, ".github/workflows/codspeed.yml"), "utf8");
const bandChatWalltime = fs.readFileSync(
  path.join(root, "examples/band-chat/benchmarks/benches/walltime.rs"),
  "utf8",
);
const nightlyWorkflow = fs.readFileSync(
  path.join(root, ".github/workflows/nightly-tests.yml"),
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
const differentialOracle = fs.readFileSync(
  path.join(root, "crates/jazz/tests/maintained_differential.rs"),
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
  const cpu = [
    "lint",
    "test-rust-workspace",
    "test-storage-compat",
    "test-ts",
    "test-react-native",
  ].includes(jobName)
    ? 16
    : 4;
  assert.match(jobSource, new RegExp(`runs-on: blacksmith-${cpu}vcpu-ubuntu-2404`));
  assert.doesNotMatch(jobSource, /^    runs-on: jazz-ci$/m);
};
const rnNativeArtifactPushPaths = [
  "Cargo.lock",
  "Cargo.toml",
  "crates/jazz/**",
  "crates/jazz-compression/**",
  "crates/jazz-native-relay/**",
  "crates/jazz-native-transport/**",
  "crates/jazz-rn/**",
];
const assertRnNativeArtifactPushPaths = (paths) =>
  assert.deepEqual(
    paths,
    rnNativeArtifactPushPaths,
    "RN artifact main pushes must cover exactly the relay production dependency closure",
  );
const integrationCheckStep = (lintJob) => {
  const start = lintJob.indexOf(
    "name: Run CI-equivalent lint, metadata and workspace check partition",
  );
  assert.notEqual(start, -1, "missing integration workspace check");
  const end = lintJob.indexOf("\n      - ", start + 1);
  return lintJob.slice(start, end === -1 ? lintJob.length : end);
};
const assertIntegrationCheckIsGating = (lintJob) => {
  // Job-level continue-on-error makes every failed step non-gating. Reject the
  // property rather than only the literal `true`, because expressions are
  // equally able to accidentally suppress an integration failure.
  assert.doesNotMatch(lintJob, /^    continue-on-error:/m, "lint must not suppress job failures");
  assert.doesNotMatch(
    integrationCheckStep(lintJob),
    /^\s+continue-on-error:/m,
    "integration workspace check must not suppress its failure",
  );
};
const trustedCachePullRequest =
  'github.event_name == \'pull_request\' && github.event.pull_request.head.repo.full_name == github.repository && contains(fromJSON(\'["OWNER","MEMBER","COLLABORATOR"]\'), github.event.pull_request.author_association)';
// merge-group/** pushes are trusted: a ruleset limits who can push them.
const trustedCacheCondition =
  "(github.event_name == 'push' && (github.ref == 'refs/heads/main' || github.ref == 'refs/heads/release' || startsWith(github.ref, 'refs/heads/merge-group/')) || " +
  trustedCachePullRequest +
  ")";
// A main push a green merge group already covered skips the trusted suite.
const trustedCallerCondition =
  "!cancelled() && needs.main-coverage.outputs.covered != 'true' && " + trustedCacheCondition;
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

  for (const name of ["lint", "test-rust-workspace", "test-storage-compat", "test-ts"]) {
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
  const mergeGroupPush = eventName === "push" && ref.startsWith("refs/heads/merge-group/");
  const trusted =
    (eventName === "push" && ["refs/heads/main", "refs/heads/release"].includes(ref)) ||
    mergeGroupPush ||
    trustedPullRequest;
  const credentialedCache =
    trusted && (eventName !== "push" || ref === "refs/heads/main" || mergeGroupPush);
  return {
    invocation: trusted ? "trusted" : "untrusted",
    idToken: trusted ? "write" : "none",
    sccache: credentialedCache
      ? (eventName === "push" && ref === "refs/heads/main") || mergeGroupPush
        ? "writer"
        : "reader"
      : "none",
    turbo: credentialedCache,
  };
};
// Ready PRs get the fast lint partition; the full suite runs on pushes
// (main, release and the Review Loop's merge-group/**), dispatches and release
// PRs.
const ciScope =
  "${{ github.event_name != 'pull_request' && 'full' || github.base_ref == 'release' && 'full' || github.event.pull_request.draft && 'none' || 'fast' }}";
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
    [
      "main-coverage",
      "report-dispatched-test-rust",
      "report-full-suite",
      "test-rust",
      "trusted",
      "untrusted",
    ],
    "the credential boundary must account for every entry-workflow job",
  );
  const {
    untrusted,
    trusted,
    "test-rust": aggregate,
    "report-dispatched-test-rust": dispatchedStatus,
    "report-full-suite": fullSuiteStatus,
    "main-coverage": mainCoverage,
  } = entryJobs;
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
  assert.deepEqual(untrusted.with, {
    scope: ciScope,
    "sccache-write": false,
    "trusted-cache": false,
  });

  assert.deepEqual(
    Object.keys(trusted).sort(),
    ["if", "needs", "permissions", "secrets", "uses", "with"],
    "trusted caller credential surface must stay explicit",
  );
  assert.equal(trusted.if, trustedCallerCondition);
  assert.deepEqual(trusted.needs, ["main-coverage"]);
  assert.deepEqual(trusted.permissions, {
    contents: "read",
    "id-token": "write",
    packages: "read",
  });
  assert.equal(trusted.uses, "./.github/workflows/ci-suite.yml");
  assert.deepEqual(trusted.with, {
    "sccache-write":
      "${{ github.event_name == 'push' && (github.ref == 'refs/heads/main' || startsWith(github.ref, 'refs/heads/merge-group/')) }}",
    scope: ciScope,
    "trusted-cache":
      "${{ github.event_name != 'push' || github.ref == 'refs/heads/main' || startsWith(github.ref, 'refs/heads/merge-group/') }}",
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
  assert.deepEqual(aggregate.needs, ["main-coverage", "untrusted", "trusted"]);
  assert.deepEqual(aggregate.permissions, { contents: "read" });
  // Both callers may be skipped only for drafts and covered main pushes.
  assert.match(
    aggregate.steps[0].run,
    /if \[ "\$\{DRAFT\}" = true \] \|\| \[ "\$\{COVERED\}" = true \]; then\n\s+test "\$\{UNTRUSTED_RESULT\}:\$\{TRUSTED_RESULT\}" = "skipped:skipped" && exit 0/,
  );
  assert.equal(
    aggregate.steps[0].env.COVERED,
    "${{ needs.main-coverage.outputs.covered == 'true' }}",
  );

  // The main-push skip reads git and commit statuses, nothing more, and runs
  // only for main pushes.
  assert.deepEqual(Object.keys(mainCoverage).sort(), [
    "if",
    "outputs",
    "permissions",
    "runs-on",
    "steps",
    "timeout-minutes",
  ]);
  assert.equal(mainCoverage.if, "github.event_name == 'push' && github.ref == 'refs/heads/main'");
  assert.deepEqual(mainCoverage.permissions, {
    contents: "read",
    statuses: "read",
  });
  assert.equal(mainCoverage.steps.length, 2);
  assert.equal(
    mainCoverage.steps[0].uses,
    "actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd",
  );
  assert.equal(mainCoverage.steps[0].with["persist-credentials"], false);
  assert.match(mainCoverage.steps[1].run, /dev\/ci\/main-push-coverage\.sh /);
  assert.match(mainCoverage.steps[1].run, /echo "covered=true" >> "\$\{GITHUB_OUTPUT\}"/);

  // Only dispatched runs may write a commit status, and only test-rust's own.
  assert.deepEqual(Object.keys(dispatchedStatus).sort(), [
    "if",
    "needs",
    "permissions",
    "runs-on",
    "steps",
    "timeout-minutes",
  ]);
  assert.equal(dispatchedStatus.if, "always() && github.event_name == 'workflow_dispatch'");
  assert.deepEqual(dispatchedStatus.needs, ["test-rust"]);
  assert.deepEqual(dispatchedStatus.permissions, { statuses: "write" });
  assert.equal(dispatchedStatus.steps.length, 1);
  assert.equal(dispatchedStatus.steps[0].uses, undefined);
  assert.match(dispatchedStatus.steps[0].run, /-f context=test-rust /);

  // full-suite coverage (#4091) is posted only after test-rust passed, and only
  // by runs that executed the full suite with a write-capable token.
  assert.deepEqual(Object.keys(fullSuiteStatus).sort(), [
    "if",
    "needs",
    "permissions",
    "runs-on",
    "steps",
    "timeout-minutes",
  ]);
  assert.match(fullSuiteStatus.if, /^always\(\) && needs\.test-rust\.result == 'success' && \(/);
  assert.match(
    fullSuiteStatus.if,
    /github\.event_name == 'pull_request' && needs\.trusted\.result == 'success' && github\.base_ref == 'release'\n/,
  );
  assert.doesNotMatch(fullSuiteStatus.if, /head_ref/);
  assert.doesNotMatch(fullSuiteStatus.if, /needs\.untrusted/);
  assert.deepEqual(fullSuiteStatus.needs, ["trusted", "test-rust"]);
  assert.deepEqual(fullSuiteStatus.permissions, { contents: "read", statuses: "write" });
  assert.equal(fullSuiteStatus.steps.length, 2);
  assert.equal(
    fullSuiteStatus.steps[0].uses,
    "actions/checkout@de0fac2e4500dabe0009e67214ff5f5447ce83dd",
  );
  assert.equal(fullSuiteStatus.steps[1].uses, undefined);
  assert.match(fullSuiteStatus.steps[1].run, /dev\/ci\/full-suite-coverage\.sh origin\/main /);
  assert.equal(
    fullSuiteStatus.steps[1].env.IS_GROUP,
    "${{ github.event_name == 'push' && startsWith(github.ref, 'refs/heads/merge-group/') }}",
  );
  assert.match(fullSuiteStatus.steps[1].run, /-f context=full-suite /);
  assert.match(fullSuiteStatus.steps[1].run, /-f state=success /);
  assert.equal(document.on.pull_request_target, undefined);
  assert.deepEqual(
    document.on.pull_request,
    { types: ["opened", "synchronize", "reopened", "ready_for_review"] },
    "stacked PR bases must not be branch-filtered, and marking a draft ready must run CI",
  );
  assert.deepEqual(document.on.push, {
    branches: ["main", "release", "merge-group/**"],
  });
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
  const storageCompatRust = job("test-storage-compat");
  const typescript = job("test-ts");

  assert.doesNotMatch(workflowSuite, /cargo install cargo-nextest/);
  assert.match(setupBlacksmithAction, /cargo-nextest --version \| grep -F "0\.9\.143"/);
  assert.match(setupBlacksmithAction, /wasm-pack --version \| grep -F "0\.13\.1"/);
  for (const rust of [workspaceRust, storageCompatRust]) {
    assert.doesNotMatch(rust, /install-rust-tool|ensure:rust-toolchain|wasm-pack/);
    assert.doesNotMatch(rust, /rust-components:/);
  }
  assert.doesNotMatch(lint, /install-rust-tool|ensure:rust-toolchain|wasm-pack/);
  assert.doesNotMatch(typescript, /install-rust-tool/);
  for (const source of [lint, workspaceRust, storageCompatRust, typescript])
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
      /cargo test -p jazz --no-default-features \\\n\s+--features testing,transport-compression-zstd --no-run \\\n\s+--test integration --test maintained_differential \\\n/,
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

test("Rust tool installation is pinned, allowlisted, and action-download independent", () => {
  const toolCacheKey =
    "jazz-rust-tool-${{ inputs.installer-revision }}-${{ runner.os }}-${{ runner.arch }}-${{ inputs.tool }}";
  assert.doesNotMatch(installRustTool, /taiki-e\/install-action/);
  assert.match(installRustTool, /run: dev\/ci\/install-rust-tool\.sh/);
  assert.match(installRustTool, /CARGO_HOME: \$\{\{ runner\.temp \}\}\/jazz-rust-tool\/cargo/);
  assert.match(installRustTool, /JAZZ_RUST_TOOL: \$\{\{ inputs\.tool \}\}/);
  assert.match(
    installRustTool,
    /uses: actions\/cache\/restore@caa296126883cff596d87d8935842f9db880ef25/,
  );
  assert.match(
    installRustTool,
    /uses: actions\/cache\/save@caa296126883cff596d87d8935842f9db880ef25/,
  );
  assert.match(
    installRustTool,
    new RegExp(toolCacheKey.replaceAll("$", "\\$").replaceAll("{", "\\{").replaceAll("}", "\\}")),
  );
  assert.match(installRustTool, /path: \$\{\{ runner\.temp \}\}\/jazz-rust-tool/);
  assert.match(
    installRustTool,
    /JAZZ_RUST_TOOL_CACHE_HIT: \$\{\{ steps\.tool-cache\.outputs\.cache-hit \}\}/,
  );
  assert.match(
    installRustTool,
    /JAZZ_RUST_TOOL_CACHE_KEY: jazz-rust-tool-\$\{\{ inputs\.installer-revision \}\}-\$\{\{ runner\.os \}\}-\$\{\{ runner\.arch \}\}-\$\{\{ inputs\.tool \}\}/,
  );
  assert.match(installRustTool, /installer-revision:[\s\S]*default: "v1"/);
  assert.match(
    installRustTool,
    /steps\.tool-cache\.outputs\.cache-hit != 'true'[\s\S]*uses: actions\/cache\/save@/,
  );
  for (const source of [workflow, setupBuildAction, packageBuild])
    assert.doesNotMatch(source, /taiki-e\/install-action/);
  for (const [tool, crate, version, provisioned] of [
    ["sccache@0.15.0", "sccache", "0.15.0", true],
    ["cargo-nextest@0.9.143", "cargo-nextest", "0.9.143", true],
    // cargo-zigbuild is needed only for hosted Linux package cross-builds;
    // the provisioned bundle intentionally does not include Zig.
    ["cargo-zigbuild@0.20.1", "cargo-zigbuild", "0.20.1", false],
    ["wasm-pack@0.13.1", "wasm-pack", "0.13.1", true],
  ]) {
    if (provisioned)
      assert.match(
        installRustTool,
        new RegExp(`${tool.replaceAll(".", "\\.")}(?:\\||\\))`),
        `provisioned and hosted paths must both support ${tool}`,
      );
    assert.match(
      installRustToolScript,
      new RegExp(
        `${tool.replaceAll(".", "\\.")}[\\s\\S]*crate=${crate}[\\s\\S]*version=${version.replaceAll(".", "\\.")}`,
      ),
    );
  }
  assert.match(
    installRustToolScript,
    /cargo install "\$\{crate\}" --version "\$\{version\}" --locked --root "\$\{install_root\}"/,
  );
  assert.match(installRustToolScript, /if \[\[ "\$\{JAZZ_RUST_TOOL_CACHE_HIT:-\}" == "true" \]\]/);
  assert.match(installRustToolScript, /"\$\{install_root\}\/bin\/\$\{binary\}" --version/);
  assert.match(
    installRustToolScript,
    /failed version validation[\s\S]*Bump installer-revision before retrying/,
  );
  assert.match(installRustToolScript, /exit 65/);
  assert.doesNotMatch(installRustToolScript, /rm -rf/);
  assert.match(installRustToolScript, /echo "\$\{install_root\}\/bin" >> "\$\{GITHUB_PATH/);
  assert.match(setupBuildAction, /tool: sccache@0\.15\.0/);
  assert.match(packageBuild, /tool: cargo-zigbuild@0\.20\.1/);
  assert.throws(
    () => assert.match(installRustToolScript.replace(" --locked", ""), / --locked/),
    /--locked/,
    "contract must reject a planted unlocked install",
  );
  assert.throws(
    () =>
      assert.match(
        installRustTool.replace(
          "        JAZZ_RUST_TOOL_CACHE_HIT: ${{ steps.tool-cache.outputs.cache-hit }}\n",
          "",
        ),
        /JAZZ_RUST_TOOL_CACHE_HIT: \$\{\{ steps\.tool-cache\.outputs\.cache-hit \}\}/,
      ),
    /JAZZ_RUST_TOOL_CACHE_HIT/,
    "contract must reject a cache hit that skips validation",
  );
  assert.throws(
    () => assert.match(installRustToolScript.replace("  exit 65", "  exit 0"), /  exit 65/),
    /exit 65/,
    "contract must reject a corrupt cache hit that does not fail closed",
  );
});

test("a corrupt exact-key Rust tool cache fails closed without installing or deleting it", () => {
  const temp = fs.mkdtempSync(path.join(os.tmpdir(), "jazz-rust-tool-invalid-cache-"));
  const githubPath = path.join(temp, "github-path");
  const cachedBinary = path.join(temp, "jazz-rust-tool", "bin", "sccache");
  try {
    fs.mkdirSync(path.dirname(cachedBinary), { recursive: true });
    fs.writeFileSync(cachedBinary, "#!/usr/bin/env bash\necho 'sccache 0.14.0'\n");
    fs.chmodSync(cachedBinary, 0o755);
    const result = spawnSync("bash", ["dev/ci/install-rust-tool.sh"], {
      cwd: root,
      encoding: "utf8",
      env: {
        ...process.env,
        GITHUB_PATH: githubPath,
        JAZZ_RUST_TOOL: "sccache@0.15.0",
        JAZZ_RUST_TOOL_CACHE_HIT: "true",
        JAZZ_RUST_TOOL_CACHE_KEY: "jazz-rust-tool-v1-Linux-X64-sccache@0.15.0",
        RUNNER_TEMP: temp,
      },
    });
    assert.equal(result.status, 65);
    assert.match(result.stderr, /failed version validation/);
    assert.match(result.stderr, /Bump installer-revision/);
    assert.equal(fs.existsSync(cachedBinary), true);
  } finally {
    fs.rmSync(temp, { force: true, recursive: true });
  }
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
  const storageCompat = job("test-storage-compat");
  const aggregate = job("test-rust");
  const localCi = fs.readFileSync(path.join(root, "dev/gates/local-ci-equivalent.mjs"), "utf8");

  assert.match(workspace, /local-ci-equivalent\.mjs --ci-partition rust-workspace/);
  assert.match(storageCompat, /local-ci-equivalent\.mjs --ci-partition storage-compat/);
  // The oracle links the jazz library the native corpus just built, and it
  // still reports when the corpus step fails.
  assert.match(
    storageCompat,
    /--ci-partition storage-compat\n[\s\S]*?if: \$\{\{ !cancelled\(\) \}\}\n\s+run: node dev\/gates\/local-ci-equivalent\.mjs --ci-partition rust-differential/,
  );
  assert.match(
    localCi,
    /run-rust-tests\.mjs[\s\S]*--timeout-seconds[\s\S]*780[\s\S]*--nextest-profile[\s\S]*jazz-ci/,
  );
  assert.match(
    localCi,
    /cargo test -p jazz --test maintained_differential --no-default-features --features testing,transport-compression-zstd --no-run --message-format=json/,
  );
  assert.match(localCi, /message\.target\.name === "maintained_differential"/);
  assert.match(
    localCi,
    /timeout 60s env[\s\S]*JAZZ_SEED=11[\s\S]*JAZZ_DIFFERENTIAL_CHURN_DEPTHS=10,1000[\s\S]*JAZZ_DIFFERENTIAL_STEP_COUNT=60[\s\S]*maintained_one_shot_differential_oracle --exact --ignored/,
  );
  assert.match(
    differentialOracle,
    /#\[ignore = "#\d+: manual randomized differential soak; bounded seed 11 runs in CI"\]\n(?:pub )?fn maintained_one_shot_differential_oracle\(/,
  );
  assert.match(aggregate, /if: always\(\)/);
  assert.match(aggregate, /needs: \[test-rust-workspace, test-storage-compat\]/);
  assert.match(aggregate, /test "\$\{WORKSPACE_RESULT\}" = success/);
  assert.match(aggregate, /test "\$\{STORAGE_COMPAT_RESULT\}" = success/);
  assert.throws(
    () => assert.match(localCi.replace("--exact --ignored", "--ignored"), /--exact --ignored/),
    /exact/,
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

test("the lint job checks the integration workspace and TypeScript runs its own partition", () => {
  const typescript = job("test-ts");
  const lint = job("lint");

  assert.match(
    setupBlacksmithAction,
    /echo "JAZZ_TEST_ARTIFACT_LOCK_PATH=\$\{RUNNER_TEMP\}\/jazz-test-artifacts\.lock" >> "\$\{GITHUB_ENV\}"/,
  );

  // Keeping these phases together avoids a separate checkout/setup phase.
  assert.doesNotMatch(workflowSuite, /^  build-integration:/m);
  assert.match(
    typescript,
    /name: Run CI-equivalent TypeScript partition\s+run: node dev\/gates\/local-ci-equivalent\.mjs --ci-partition typescript/,
  );
  assert.match(
    lint,
    /name: Run CI-equivalent lint, metadata and workspace check partition\s+run: node dev\/gates\/local-ci-equivalent\.mjs --ci-partition lint/,
  );
  assertIntegrationCheckIsGating(lint);
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

test("shared Rust cache writes are main and merge-group only while trusted PRs receive read access", () => {
  for (const name of ["lint", "test-rust-workspace", "test-storage-compat", "test-ts"]) {
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

test("entry workflow grants credentialed cross-ref caches only to main, merge groups and trusted stacked PRs", () => {
  const cases = [
    [
      "main push",
      { eventName: "push", ref: "refs/heads/main" },
      {
        invocation: "trusted",
        idToken: "write",
        sccache: "writer",
        turbo: true,
      },
    ],
    [
      "release push",
      { eventName: "push", ref: "refs/heads/release" },
      {
        invocation: "trusted",
        idToken: "write",
        sccache: "none",
        turbo: false,
      },
    ],
    [
      "merge-group push",
      { eventName: "push", ref: "refs/heads/merge-group/4" },
      {
        invocation: "trusted",
        idToken: "write",
        sccache: "writer",
        turbo: true,
      },
    ],
    [
      "feature push",
      { eventName: "push", ref: "refs/heads/feature/cache-auth" },
      {
        invocation: "untrusted",
        idToken: "none",
        sccache: "none",
        turbo: false,
      },
    ],
    [
      "trusted same-repository PR",
      {
        eventName: "pull_request",
        ref: "refs/pull/1/merge",
        sameRepository: true,
        authorAssociation: "MEMBER",
      },
      {
        invocation: "trusted",
        idToken: "write",
        sccache: "reader",
        turbo: true,
      },
    ],
    [
      "trusted upper stack layer",
      {
        eventName: "pull_request",
        ref: "refs/pull/2/merge",
        sameRepository: true,
        authorAssociation: "COLLABORATOR",
      },
      {
        invocation: "trusted",
        idToken: "write",
        sccache: "reader",
        turbo: true,
      },
    ],
    [
      "outside contributor on same repository",
      {
        eventName: "pull_request",
        ref: "refs/pull/3/merge",
        sameRepository: true,
        authorAssociation: "CONTRIBUTOR",
      },
      {
        invocation: "untrusted",
        idToken: "none",
        sccache: "none",
        turbo: false,
      },
    ],
    [
      "fork PR",
      {
        eventName: "pull_request",
        ref: "refs/pull/4/merge",
        sameRepository: false,
        authorAssociation: "MEMBER",
      },
      {
        invocation: "untrusted",
        idToken: "none",
        sccache: "none",
        turbo: false,
      },
    ],
    [
      "Dependabot PR",
      {
        eventName: "pull_request",
        ref: "refs/pull/5/merge",
        sameRepository: true,
        authorAssociation: "CONTRIBUTOR",
      },
      {
        invocation: "untrusted",
        idToken: "none",
        sccache: "none",
        turbo: false,
      },
    ],
    [
      "manual main",
      { eventName: "workflow_dispatch", ref: "refs/heads/main" },
      {
        invocation: "untrusted",
        idToken: "none",
        sccache: "none",
        turbo: false,
      },
    ],
    [
      "manual feature",
      { eventName: "workflow_dispatch", ref: "refs/heads/feature/cache-auth" },
      {
        invocation: "untrusted",
        idToken: "none",
        sccache: "none",
        turbo: false,
      },
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

test("on-demand WebKit IndexedDB receipt scopes build caches to its repository", () => {
  assert.equal(
    parse(webkitIndexedDbReceipt).env.CACHE_SCOPE_REPOSITORY_ID,
    "${{ github.repository_id }}",
  );
  assert.match(webkitIndexedDbReceipt, /uses: \.\/\.github\/actions\/setup-build/);
  assert.equal(
    parse(
      webkitIndexedDbReceipt.replace(
        "  CACHE_SCOPE_REPOSITORY_ID: ${{ github.repository_id }}\n",
        "",
      ),
    ).env?.CACHE_SCOPE_REPOSITORY_ID,
    undefined,
  );
});

test("integration workspace check contract rejects planted failure suppression", () => {
  const lint = job("lint");
  const check =
    "name: Run CI-equivalent lint, metadata and workspace check partition\n        run: node dev/gates/local-ci-equivalent.mjs --ci-partition lint";

  assert.throws(
    () =>
      assertIntegrationCheckIsGating(
        lint.replace(check, `${check}\n        continue-on-error: true`),
      ),
    /integration workspace check must not suppress its failure/,
  );
  assert.throws(
    () =>
      assertIntegrationCheckIsGating(
        lint.replace(
          "    timeout-minutes: 15",
          "    continue-on-error: true\n    timeout-minutes: 15",
        ),
      ),
    /lint must not suppress job failures/,
  );
});

test("lint keeps its one workspace Clippy invocation inside pnpm lint", () => {
  const lint = job("lint");
  assert.match(lint, /local-ci-equivalent\.mjs --ci-partition lint/);
  assert.doesNotMatch(lint, /^\s*- run: cargo clippy/m);
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

test("CodSpeed baselines every main merge and runs only for benchmark-labeled PRs", () => {
  // Every main merge queues a CodSpeed run. Otherwise PR reports compare
  // against a stale main run and attribute intervening merges to the PR
  // (#3488).
  const document = parse(codspeedWorkflow);
  assert.deepEqual(document.on.push, { branches: ["main"] });
  assert.deepEqual(document.on.pull_request, {
    types: ["labeled", "synchronize", "reopened"],
  });
  // One daily schedule, and it runs only the nightly suite (the nightly
  // extras, never a per-merge case). It is not a main baseline: merges still
  // baseline themselves, and nightly runs use their own concurrency group, so
  // they never replace or delay a pending merge run.
  assert.equal(document.on.schedule.length, 1, "one nightly schedule");
  assert.match(document.on.schedule[0].cron, /^\d+ \d+ \* \* \*$/, "daily");
  assert.deepEqual(document.on.workflow_dispatch.inputs.suite.options, ["merge", "nightly"]);
  assert.equal(document.on.workflow_dispatch.inputs.suite.default, "merge");
  const nightly = "(github.event_name == 'schedule' || inputs.suite == 'nightly')";
  assert.equal(
    document.env.JAZZ_CODSPEED_SUITE,
    `\${{ ${nightly} && 'nightly' || 'merge' }}`,
    "the schedule, and only it or an explicit dispatch, selects the nightly suite",
  );
  for (const [name, job] of Object.entries(document.jobs)) {
    assert.equal(job.env?.JAZZ_CODSPEED_SUITE, undefined, `${name} must not override the suite`);
  }
  // Every root job carries the label gate; dependent jobs inherit its skip.
  const labelGate =
    "github.event_name != 'pull_request' || contains(github.event.pull_request.labels.*.name, 'benchmark')";
  const rootJobsAreGated = (jobs) => {
    for (const [name, job] of Object.entries(jobs)) {
      if (!job.needs) assert.equal(job.if, labelGate, `${name} must carry the label gate`);
    }
  };
  rootJobsAreGated(document.jobs);
  // Main runs share one group and are never cancelled mid-run, so a burst of
  // merges coalesces to the running commit plus the latest. PR runs cancel
  // superseded pushes. Nightly runs get a separate group, so a merge never
  // replaces a pending nightly run and a nightly run never holds up a merge.
  assert.deepEqual(document.concurrency, {
    group: `codspeed-example-benchmarks-\${{ github.event.pull_request.number || github.ref }}\${{ ${nightly} && '-nightly' || '' }}`,
    "cancel-in-progress": "${{ github.event_name == 'pull_request' }}",
  });
  assert.throws(() => {
    // Keying main runs by commit would run every merge of a burst in parallel.
    const thrash = parse(codspeedWorkflow.replace("|| github.ref }}", "|| github.sha }}"));
    assert.match(thrash.concurrency.group, /github\.ref \}\}\$\{\{/);
  }, /match/);

  assert.throws(() => {
    const unsafe = parse(codspeedWorkflow.replace("  push:\n    branches: [main]\n", ""));
    assert.deepEqual(unsafe.on.push, { branches: ["main"] });
  }, /Expected values to be strictly deep-equal/);
  assert.throws(() => {
    const unsafe = parse(
      codspeedWorkflow.replace(
        "cancel-in-progress: ${{ github.event_name == 'pull_request' }}",
        "cancel-in-progress: true",
      ),
    );
    assert.equal(
      unsafe.concurrency["cancel-in-progress"],
      "${{ github.event_name == 'pull_request' }}",
    );
  }, /strictly equal/);
  assert.throws(() => {
    const unsafe = parse(
      codspeedWorkflow.replace(
        "github.event_name != 'pull_request' || contains(github.event.pull_request.labels.*.name, 'benchmark')",
        "true",
      ),
    );
    rootJobsAreGated(unsafe.jobs);
  }, /label gate/);
});

test("CodSpeed measures route fan-out through BandChat's live-rooms case", () => {
  const document = parse(codspeedWorkflow);
  const plan = document.jobs["native-workloads-plan"];
  const measure = document.jobs["native-workloads-walltime"];
  assert.equal(
    plan.if,
    "github.event_name != 'pull_request' || contains(github.event.pull_request.labels.*.name, 'benchmark')",
  );
  assert.deepEqual(measure.needs, ["native-workloads-plan", "native-workloads-build"]);
  assert.equal(measure["runs-on"], "codspeed-macro");
  const matrix = spawnSync(
    "node",
    [path.join(root, "dev/benchmarks/codspeed-artifact.mjs"), "matrix"],
    { encoding: "utf8" },
  ).stdout;
  assert.ok(
    JSON.parse(matrix).includes("band-chat"),
    "BandChat wall-time workload (owner of route fan-out) must remain present",
  );
  // Features are selected at `cargo codspeed build` time. `run` only executes
  // that copied target; cargo-codspeed rejects Cargo feature flags there.
  const args = (action) =>
    spawnSync(
      "node",
      [path.join(root, "dev/benchmarks/codspeed-artifact.mjs"), action, "band-chat"],
      { encoding: "utf8" },
    ).stdout.trim();
  assert.equal(
    args("build-args"),
    "--package jazz-example-band-chat-benchmark --bench walltime --features jazz-benchmark-guard/mimalloc",
  );
  assert.equal(args("run-args"), "--package jazz-example-band-chat-benchmark --bench walltime");
  assert.doesNotMatch(JSON.stringify(measure), /--features|JAZZ_ROUTE_CURVE_ROUTES/);
  assert.match(bandChatWalltime, /#\[divan::bench\(args = \[ROOMS_OPEN\]/);
  assert.match(bandChatWalltime, /fn band_chat_new_message_rooms_open/);
});

test("React Native artifact builds are explicit same-repository label opt-ins", () => {
  const document = parse(rnNativeArtifactsWorkflow);
  assert.deepEqual(document.on.pull_request, {
    types: ["opened", "reopened", "synchronize", "labeled", "unlabeled"],
  });
  assert.deepEqual(document.on.push.branches, ["main", "release"]);
  assertRnNativeArtifactPushPaths(document.on.push.paths);
  assert.throws(
    () =>
      assertRnNativeArtifactPushPaths(
        document.on.push.paths.filter((path) => path !== "Cargo.lock"),
      ),
    /dependency closure/,
    "omitting Cargo.lock must not leave artifact builds stale after a lockfile update",
  );
  assert.throws(
    () => assertRnNativeArtifactPushPaths([...document.on.push.paths, "crates/irrelevant/**"]),
    /dependency closure/,
    "unrelated paths must not silently broaden the expensive RN artifact trigger",
  );
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
    "${{ github.event_name != 'pull_request' || ( ( contains(github.event.pull_request.labels.*.name, 'react-native') || contains(github.event.pull_request.labels.*.name, 'rn-preview-release') ) && github.event.pull_request.head.repo.full_name == github.repository ) }}";
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

test("benchmark correctness stays on ordinary CI while API compilation runs nightly", () => {
  const workspace = job("test-rust-workspace");
  const localCi = fs.readFileSync(path.join(root, "dev/gates/local-ci-equivalent.mjs"), "utf8");
  const scenarioMode = benchmarkSmokeMode("ci");
  const compileMode = benchmarkSmokeMode("compile-ci");
  const requiredSmokeTests = [
    "jazz-sim::core_benchmark_smoke=cold_subscription_correctness_smoke",
    "jazz-sim::core_benchmark_smoke=sync_correctness_smoke",
    "jazz-sim::core_benchmark_smoke=validation_correctness_smoke",
    "jazz-sim::core_benchmark_smoke=relation_include_delivery_correctness_smoke",
    "jazz-sim::core_benchmark_smoke=route_subscription_curve_correctness_smoke",
    "jazz-sim::scenario_smoke=s1_saas_smoke",
    "jazz-sim::scenario_smoke=micro_correctness_smoke",
    "jazz-sim::scenario_smoke=s1_saas_db_surface_smoke",
    "jazz-sim::scenario_smoke=s2_canvas_smoke",
    "jazz-sim::scenario_smoke=s3_permissions_smoke",
    "jazz-sim::scenario_smoke=s4_order_processing_smoke_debug_profile",
    "jazz-sim::scenario_smoke=s5_durable_stream_smoke",
    "jazz-sim::scenario_smoke=s7_migrations_smoke",
    "jazz-sim::scenario_smoke=s8_branch_views_smoke",
    "jazz-sim::scenario_smoke=s9_durable_execution_smoke",
  ];
  assert.match(workspace, /local-ci-equivalent\.mjs --ci-partition rust-workspace/);
  assert.deepEqual(
    [...localCi.matchAll(/"jazz-sim::(?:core_benchmark_smoke|scenario_smoke)=[^"]+"/g)].map(
      (match) => match[0].slice(1, -1),
    ),
    requiredSmokeTests,
  );
  assert.doesNotMatch(
    localCi,
    /command\("benchmark deterministic scenario smoke", "bash", \[[\s\S]*benchmark-smoke\.sh[\s\S]*"--ci"/,
  );
  assert.throws(
    () =>
      assert.match(
        localCi.replace('"jazz-sim::scenario_smoke=s4_order_processing_smoke_debug_profile",', ""),
        /jazz-sim::scenario_smoke=s4_order_processing_smoke_debug_profile/,
      ),
    /s4_order_processing_smoke_debug_profile/,
  );
  assert.match(
    nightlyWorkflow,
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
  assert.match(scenarioMode, /cargo test -p jazz-sim --test core_benchmark_smoke/);
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

test("CodSpeed measures every example benchmark suite in wall-clock mode", () => {
  // The examples page reads wall-clock results only, so no example suite may
  // fall back to simulation.
  assert.doesNotMatch(codspeedWorkflow, /mode: simulation/);

  // Build and run arguments come from the artifact script's workload table.
  const args = (action, workload) =>
    spawnSync("node", [path.join(root, "dev/benchmarks/codspeed-artifact.mjs"), action, workload], {
      encoding: "utf8",
    }).stdout.trim();
  for (const [workload, benchmarkPackage, benches] of [
    ["big-label", "jazz-example-big-label-benchmark", ["ingest_walltime", "loads"]],
    ["stage-plan", "jazz-example-stage-plan-benchmark", ["walltime"]],
  ]) {
    for (const action of ["build-args", "run-args"]) {
      const line = args(action, workload);
      assert.match(line, new RegExp(`--package ${benchmarkPackage}(?: |$)`));
      for (const bench of benches) assert.match(line, new RegExp(`--bench ${bench}(?: |$)`));
    }
  }
  assert.match(codspeedWorkflow, /cargo codspeed build -m walltime "\$\{args\[@\]\}"/);
  assert.throws(
    () =>
      assert.match(
        args("build-args", "big-label").replace(" --bench loads", ""),
        /--bench loads(?: |$)/,
      ),
    /loads/,
  );
});

test("CodSpeed measures BandChat and WorldTour through the native wall-time matrix", async () => {
  const { workloads, groups, groupWorkloads } = await import(
    pathToFileURL(path.join(root, "dev/benchmarks/codspeed-artifact.mjs")).href
  );
  for (const workload of ["band-chat", "world-tour"]) {
    assert.ok(workloads.includes(workload));
    assert.ok(groups.some((group) => groupWorkloads(group).includes(workload)));
  }
  assert.equal(
    codspeedWorkflow.match(
      /group: \$\{\{ fromJSON\(needs\.native-workloads-plan\.outputs\.groups\) \}\}/g,
    )?.length,
    2,
    "both native matrix jobs read the single group list",
  );
  assert.match(codspeedWorkflow, /node dev\/benchmarks\/codspeed-artifact\.mjs groups/);
});

test("jazz-tools advertises exactly the CLI artifacts its build matrix produces", () => {
  const producedCliArtifacts = [...packageBuild.matchAll(/^\s+output: (jazz-tools-\S+)$/gm)].map(
    (match) => match[1],
  );

  assert.deepEqual(Object.values(jazzToolsTargets).sort(), producedCliArtifacts.sort());
  assert.equal(jazzToolsTargets["win32-x64"], undefined);
  assert.match(packageBuild, /default: .*"platform":"win32-x64-msvc"/);
});

test("the jazz-tools launcher reports Windows as unsupported instead of a missing artifact", () => {
  const launcherPath = path.join(root, "packages/jazz-tools/bin/jazz-tools.js");
  const probe = spawnSync(
    process.execPath,
    [
      "--input-type=module",
      "--eval",
      `
        Object.defineProperty(process, "platform", { value: "win32" });
        Object.defineProperty(process, "arch", { value: "x64" });
        process.argv = ["node", "jazz-tools", "server"];
        await import(${JSON.stringify(launcherPath)});
      `,
    ],
    { encoding: "utf8" },
  );

  assert.equal(probe.status, 1);
  assert.match(probe.stderr, /jazz-tools CLI is not supported on win32\/x64\./);
  assert.doesNotMatch(probe.stderr, /Bundled binary missing/);
});

test("native release builds need no libclang because no Rust dependency uses bindgen", () => {
  // RocksDB's sys crates pulled in bindgen, which needed libclang on every
  // build host. If bindgen returns, provision libclang where it is needed
  // (Windows MSVC runners, the Bullseye NAPI image, Vercel) before updating this.
  const lockfile = fs.readFileSync(path.join(root, "Cargo.lock"), "utf8");
  for (const crate of ["bindgen", "clang-sys", "rust-librocksdb-sys"])
    assert.doesNotMatch(lockfile, new RegExp(`^name = "${crate}"$`, "m"), crate);
  assert.doesNotMatch(packageBuild, /libclang|LIBCLANG_PATH/);
});

test("pkg.pr.new previews omit Windows while release package builds retain it", () => {
  assert.match(packageBuild, /default: .*win32-x64-msvc/);
  assert.match(packageBuild, /include: \$\{\{ fromJSON\(inputs\.napi_matrix\) \}\}/);
  assert.match(packageBuild, /Remove Windows package omitted by this build/);
  const stagedPreviewPlatforms =
    /stage-napi-manifests\.mjs linux-x64-gnu linux-arm64-gnu darwin-x64 darwin-arm64/;
  const previewPlatforms =
    /napi_matrix: .*linux-x64-gnu.*linux-arm64-gnu.*darwin-x64.*darwin-arm64/;
  assert.match(packageBuild, stagedPreviewPlatforms);
  assert.doesNotMatch(previewBuild, /win32-x64-msvc/);
  assert.match(previewBuild, previewPlatforms);
  // Both Linux bindings must be built and admitted into the staged preview.
  for (const platform of ["linux-x64-gnu", "linux-arm64-gnu"]) {
    assert.throws(
      () =>
        assert.match(packageBuild.replaceAll(platform, "omitted-platform"), stagedPreviewPlatforms),
      { code: "ERR_ASSERTION" },
    );
    assert.throws(
      () => assert.match(previewBuild.replaceAll(platform, "omitted-platform"), previewPlatforms),
      { code: "ERR_ASSERTION" },
    );
  }
});

test("TypeScript CI overlaps independent Node and browser suites after one artifact build", () => {
  const typescript = job("test-ts");
  const runner = fs.readFileSync(path.join(root, "dev/gates/run-ts-tests.sh"), "utf8");
  const localCi = fs.readFileSync(path.join(root, "dev/gates/local-ci-equivalent.mjs"), "utf8");
  assert.match(typescript, /local-ci-equivalent\.mjs --ci-partition typescript/);
  assert.match(
    localCi,
    /native correctness-artifact producer[\s\S]*ensure-correctness-artifacts\.mjs/,
  );
  assert.match(localCi, /TypeScript consumers[\s\S]*test:typescript-consumers/);
  assert.match(runner, /require\('\.\/crates\/jazz-napi'\)/);
  assert.match(runner, /JAZZ_TEST_SEALED_TOOLS_DIST=1/);
  assert.match(runner, /verify-jazz-tools-exports\.mjs/);
  assert.match(runner, /public export surface is incomplete/);
  assert.match(runner, /producer receipt is checked before either suite starts/);
  assert.match(runner, /correctness-artifact-producer\.mjs/);
  assert.match(runner, /--concurrency=3/);
  assert.match(
    runner,
    /browser_tests_command=.*pnpm --parallel --filter jazz-tools --filter inspector --filter band-chat-nextjs-betterauth --filter record-player-next-betterauth --filter auth-workos-chat test:browser/,
  );
  assert.match(runner, /set -m/);
  assert.match(runner, /bash -c "\$\{node_tests_command\}" >"\$\{node_tests_log\}" 2>&1 &/);
  assert.match(runner, /bash -c "\$\{browser_tests_command\}" >"\$\{browser_tests_log\}" 2>&1 &/);
  assert.match(runner, /browser_tests_pid=\$![\s\S]*log_monitor_pid=\$![\s\S]*set \+m/);
  assert.doesNotMatch(runner, /^setsid /m);
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

test("React Native CI has a separate bridge-enabled producer and real Vitest admission", () => {
  const reactNative = job("test-react-native");
  const localCi = fs.readFileSync(path.join(root, "dev/gates/local-ci-equivalent.mjs"), "utf8");
  assert.match(reactNative, /local-ci-equivalent\.mjs --ci-partition react-native/);
  assert.match(
    localCi,
    /React Native bridge correctness-artifact producer[\s\S]*pnpm[\s\S]*build:correctness-artifacts/,
  );
  assert.match(
    localCi,
    /admitted Jazz Tools build for React Native[\s\S]*run-correctness-consumer\.mjs/,
  );
  assert.match(localCi, /React Native bridge tests[\s\S]*vitest\.react-native\.config\.ts/);
  assert.match(localCi, /JAZZ_RN_TEST_BRIDGE: "1"/);
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
        JAZZ_CORRECTNESS_ARTIFACT_RUN: "1",
        JAZZ_CORRECTNESS_WASM_PACKAGE: "/sealed/wasm",
        JAZZ_CORRECTNESS_NAPI_BINDING: "/sealed/napi/index.js",
        JAZZ_CORRECTNESS_NAPI_FINGERPRINT: "sealed",
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
    assert.match(
      result.stderr,
      /prepared native correctness artifact manifest is missing or stale/,
    );
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
          "./react": {
            types: "./dist/react/index.d.ts",
            default: "./dist/react/index.js",
          },
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

test("missing public exports or inspector assets prevent both TypeScript suites from starting", () => {
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
    write("dev/artifacts/correctness-artifact-producer.mjs", "process.exit(0);\n");
    write("crates/jazz-napi/package.json", JSON.stringify({ type: "commonjs" }));
    write("crates/jazz-napi/index.js", "module.exports = {};\n");
    write(
      "packages/jazz-tools/package.json",
      JSON.stringify({
        exports: {
          ".": { types: "./dist/index.d.ts", default: "./dist/index.js" },
          "./react": {
            types: "./dist/react/index.d.ts",
            default: "./dist/react/index.js",
          },
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
          JAZZ_SKIP_JAZZ_TOOLS_BUILD: "0",
          JAZZ_REQUIRE_CI_TEST_COMMANDS: "0",
          JAZZ_CORRECTNESS_ARTIFACT_RUN: "1",
          JAZZ_CORRECTNESS_WASM_PACKAGE: "/sealed/wasm",
          JAZZ_CORRECTNESS_NAPI_BINDING: "/sealed/napi/index.js",
          JAZZ_CORRECTNESS_NAPI_FINGERPRINT: "sealed",
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

    const nodeMarker = path.join(fixture, "node-inspector.html");
    const browserMarker = path.join(fixture, "browser-inspector.html");
    const run = () =>
      spawnSync("bash", ["dev/gates/run-ts-tests.sh"], {
        cwd: fixture,
        encoding: "utf8",
        env: {
          ...process.env,
          JAZZ_SKIP_JAZZ_TOOLS_BUILD: "0",
          JAZZ_REQUIRE_CI_TEST_COMMANDS: "0",
          JAZZ_CORRECTNESS_ARTIFACT_RUN: "1",
          JAZZ_CORRECTNESS_WASM_PACKAGE: "/sealed/wasm",
          JAZZ_CORRECTNESS_NAPI_BINDING: "/sealed/napi/index.js",
          JAZZ_CORRECTNESS_NAPI_FINGERPRINT: "sealed",
          JAZZ_NODE_TEST_COMMAND: `touch ${JSON.stringify(nodeMarker)}; test "$JAZZ_TEST_SEALED_INSPECTOR_DIST" = 1 && cp packages/inspector/dist-embedded/embedded.html ${JSON.stringify(nodeMarker)}`,
          JAZZ_BROWSER_TEST_COMMAND: `touch ${JSON.stringify(browserMarker)}; test "$JAZZ_TEST_SEALED_INSPECTOR_DIST" = 1 && cp packages/inspector/dist-embedded/embedded.html ${JSON.stringify(browserMarker)}`,
        },
      });
    const missing = run();
    assert.notEqual(missing.status, 0, missing.stdout);
    assert.equal(fs.existsSync(nodeMarker), false, "node suite started without inspector assets");
    assert.equal(
      fs.existsSync(browserMarker),
      false,
      "browser suite started without inspector assets",
    );

    write("packages/inspector/dist-embedded/embedded.html", "prepared inspector");
    const prepared = run();
    assert.equal(prepared.status, 0, `${prepared.stdout}\n${prepared.stderr}`);
    assert.equal(fs.readFileSync(nodeMarker, "utf8"), "prepared inspector");
    assert.equal(fs.readFileSync(browserMarker, "utf8"), "prepared inspector");
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

for (const [signal, expected] of [
  ["SIGINT", 130],
  ["SIGTERM", 143],
]) {
  test(`parallel TypeScript runner retains bounded diagnostics on ${signal}`, async () => {
    const fixture = fs.mkdtempSync(path.join(os.tmpdir(), "jazz-ts-ci-tails-"));
    const command = (suite) =>
      `printf '%20000s\\n' x; echo ${suite}-last-test; touch "$RUNNER_TEMP/${suite}-ready"; sleep 30`;
    const child = spawn("bash", [path.join(root, "dev/gates/run-ts-tests.sh")], {
      cwd: root,
      env: {
        ...process.env,
        RUNNER_TEMP: fixture,
        JAZZ_SKIP_JAZZ_TOOLS_BUILD: "1",
        JAZZ_NODE_TEST_COMMAND: command("node"),
        JAZZ_BROWSER_TEST_COMMAND: command("browser"),
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let output = "";
    child.stdout.on("data", (chunk) => {
      output += chunk;
    });
    child.stderr.on("data", (chunk) => {
      output += chunk;
    });
    const closed = new Promise((resolve) => child.once("close", (code) => resolve(code)));
    const watchdog = setTimeout(() => child.kill("SIGKILL"), 5000);
    try {
      const deadline = Date.now() + 3000;
      while (
        !["node", "browser"].every((suite) => fs.existsSync(path.join(fixture, `${suite}-ready`)))
      ) {
        assert.ok(Date.now() < deadline, "synthetic suites did not start");
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      child.kill(signal);
      assert.equal(await closed, expected, output);
      assert.match(output, /node-last-test/);
      assert.match(output, /browser-last-test/);
      assert.ok(Buffer.byteLength(output) < 34000, "interruption diagnostics were not bounded");
      for (const suite of ["node", "browser"]) {
        const log = fs.readdirSync(fixture).find((name) => name.startsWith(`jazz-${suite}-tests-`));
        assert.ok(log, "complete suite log was not retained");
        assert.ok(fs.statSync(path.join(fixture, log)).size > 20000);
      }
    } finally {
      clearTimeout(watchdog);
      if (child.exitCode === null) child.kill("SIGTERM");
      await closed;
      fs.rmSync(fixture, { recursive: true, force: true });
    }
  });
}

for (const ending of ["complete", "SIGTERM", "SIGKILL"]) {
  test(`parallel TypeScript runner publishes bounded progress before ${ending} and stops its monitor`, async () => {
    const fixture = fs.mkdtempSync(path.join(os.tmpdir(), "jazz-ts-ci-progress-"));
    const command = (suite) =>
      `echo $$ > "$RUNNER_TEMP/${suite}-pid"; printf '%20000s\\n' x; echo ${suite}-progress; while [ ! -f "$RUNNER_TEMP/finish" ]; do sleep 0.1; done`;
    const child = spawn("bash", [path.join(root, "dev/gates/run-ts-tests.sh")], {
      cwd: root,
      env: {
        ...process.env,
        RUNNER_TEMP: fixture,
        JAZZ_SKIP_JAZZ_TOOLS_BUILD: "1",
        JAZZ_TEST_LOG_INTERVAL_SECONDS: "1",
        JAZZ_NODE_TEST_COMMAND: command("node"),
        JAZZ_BROWSER_TEST_COMMAND: command("browser"),
      },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let output = "";
    child.stdout.on("data", (chunk) => {
      output += chunk;
    });
    child.stderr.on("data", (chunk) => {
      output += chunk;
    });
    const closed = new Promise((resolve) =>
      child.once("close", (code, signal) => resolve({ code, signal })),
    );
    const watchdog = setTimeout(() => child.kill("SIGKILL"), 8000);
    try {
      const deadline = Date.now() + 5000;
      while (!output.includes("node-progress") || !output.includes("browser-progress")) {
        assert.ok(Date.now() < deadline, "no suite progress was published before termination");
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      assert.equal(child.exitCode, null, "progress arrived only after suite completion");
      assert.ok(Buffer.byteLength(output) < 34000, "live diagnostics were not bounded");
      const monitor = Number(output.match(/live log monitor PID: (\d+)/)?.[1]);
      assert.ok(monitor > 0, "monitor PID not reported");
      await new Promise((resolve) => setTimeout(resolve, 1200));
      assert.equal(
        (output.match(/bounded live log tail:/g) ?? []).length,
        2,
        "unchanged suite logs were replayed again",
      );
      if (ending === "complete") fs.writeFileSync(path.join(fixture, "finish"), "");
      else child.kill(ending);
      let closeTimer;
      const result = await Promise.race([
        closed,
        new Promise((_, reject) => {
          closeTimer = setTimeout(
            () => reject(new Error("monitor kept its output pipe open")),
            3000,
          );
        }),
      ]).finally(() => clearTimeout(closeTimer));
      assert.deepEqual(
        result,
        ending === "SIGKILL"
          ? { code: null, signal: "SIGKILL" }
          : { code: ending === "complete" ? 0 : 143, signal: null },
      );
      // SIGKILL cannot reap its child: an init/subreaper may briefly retain an
      // exited zombie. It must never leave a running monitor or inherited pipe.
      const state = spawnSync("ps", ["-o", "stat=", "-p", String(monitor)], {
        encoding: "utf8",
      }).stdout.trim();
      assert.ok(
        state === "" || (ending === "SIGKILL" && state.startsWith("Z")),
        `monitor survived: ${state}`,
      );
      for (const suite of ["node", "browser"]) {
        const log = fs.readdirSync(fixture).find((name) => name.startsWith(`jazz-${suite}-tests-`));
        if (ending === "complete") assert.equal(log, undefined);
        else assert.ok(log && fs.statSync(path.join(fixture, log)).size > 20000);
      }
    } finally {
      clearTimeout(watchdog);
      if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
      const monitorPid = Number(output.match(/live log monitor PID: (\d+)/)?.[1]);
      if (monitorPid > 0) {
        try {
          process.kill(-monitorPid, "SIGTERM");
        } catch (error) {
          if (error.code !== "ESRCH") throw error;
        }
      }
      // The hard-kill scenario cannot run the parent's suite cleanup trap.
      for (const suite of ["node", "browser"]) {
        const pidFile = path.join(fixture, `${suite}-pid`);
        if (fs.existsSync(pidFile)) {
          try {
            process.kill(-Number(fs.readFileSync(pidFile, "utf8").trim()), "SIGTERM");
          } catch (error) {
            if (error.code !== "ESRCH") throw error;
          }
        }
      }
      await closed;
      fs.rmSync(fixture, { recursive: true, force: true });
    }
  });
}

test("CI rejects inherited live-log timing overrides", () => {
  const result = spawnSync("bash", [path.join(root, "dev/gates/run-ts-tests.sh")], {
    cwd: root,
    encoding: "utf8",
    env: {
      ...process.env,
      JAZZ_REQUIRE_CI_TEST_COMMANDS: "1",
      JAZZ_TEST_LOG_INTERVAL_SECONDS: "1",
    },
  });
  assert.equal(result.status, 1);
  assert.match(result.stderr, /JAZZ_TEST_LOG_INTERVAL_SECONDS.*forbidden/);
});

test("ready PRs run only the lint partition and drafts run nothing", () => {
  const suite = parse(workflowSuite);
  assert.deepEqual(suite.on.workflow_call.inputs.scope, {
    description: "Which partitions to run, full, fast or none.",
    required: false,
    type: "string",
    default: "full",
  });
  assert.equal(suite.jobs.lint.if, "inputs.scope != 'none'");
  for (const name of ["test-rust-workspace", "test-storage-compat", "test-ts", "test-react-native"])
    assert.equal(
      suite.jobs[name].if,
      "inputs.scope == 'full'",
      `${name} must run only in the full scope`,
    );
  assert.equal(suite.jobs["test-rust"].if, "always() && inputs.scope == 'full'");
  assert.deepEqual(
    Object.keys(suite.jobs).sort(),
    [
      "lint",
      "test-react-native",
      "test-rust",
      "test-rust-workspace",
      "test-storage-compat",
      "test-ts",
    ],
    "a new suite job must declare which scope runs it",
  );
});

test("full-suite coverage credits merge-group PR heads only through clean merges", () => {
  const repo = fs.mkdtempSync(path.join(os.tmpdir(), "full-suite-coverage-"));
  const git = (...args) => {
    const result = spawnSync("git", args, { cwd: repo, encoding: "utf8" });
    assert.equal(result.status, 0, result.stderr);
    return result.stdout.trim();
  };
  const commitFile = (name) => {
    fs.writeFileSync(path.join(repo, name), `${name}\n`);
    git("add", name);
    git("commit", "-qm", name);
    return git("rev-parse", "HEAD");
  };
  const coverage = (base, head) => {
    const result = spawnSync(
      "bash",
      [path.join(root, "dev/ci/full-suite-coverage.sh"), base, head],
      { cwd: repo, encoding: "utf8" },
    );
    assert.equal(result.status, 0, result.stderr);
    return result.stdout.trim().split("\n").sort();
  };
  try {
    git("init", "-q", "-b", "main");
    git("config", "user.email", "ci@example.com");
    git("config", "user.name", "CI");
    git("config", "core.hooksPath", "/dev/null");
    const main = commitFile("base");
    git("checkout", "-qb", "pr-a");
    const prA = commitFile("a");
    git("checkout", "-qb", "pr-b", main);
    const prB = commitFile("b");
    git("checkout", "-qb", "merge-group/1", main);
    git("merge", "-q", "--no-ff", "-m", "merge a", prA);
    git("merge", "-q", "--no-ff", "-m", "merge b", prB);
    const group = git("rev-parse", "HEAD");
    assert.deepEqual(coverage(main, group), [group, prA, prB].sort());

    // A plain commit on the group changes the tested tree for every PR.
    const fixup = commitFile("fixup");
    assert.deepEqual(coverage(main, fixup), [fixup]);

    // So does a merge whose tree is not git's own merge of its parents.
    const dirty = git(
      "commit-tree",
      `${fixup}^{tree}`,
      "-p",
      `${group}^1`,
      "-p",
      prB,
      "-m",
      "hand-resolved",
    );
    assert.deepEqual(coverage(main, dirty), [dirty]);
  } finally {
    fs.rmSync(repo, { recursive: true, force: true });
  }
});

test("main pushes skip the full suite only for trees a green merge group tested", () => {
  const repo = fs.mkdtempSync(path.join(os.tmpdir(), "main-push-coverage-"));
  const bin = fs.mkdtempSync(path.join(os.tmpdir(), "main-push-coverage-bin-"));
  const git = (...args) => {
    const result = spawnSync("git", args, { cwd: repo, encoding: "utf8" });
    assert.equal(result.status, 0, result.stderr);
    return result.stdout.trim();
  };
  const commitFile = (name) => {
    fs.writeFileSync(path.join(repo, name), `${name}\n`);
    git("add", name);
    git("commit", "-qm", name);
    return git("rev-parse", "HEAD");
  };
  // Stands in for `gh api repos/<repo>/commits/<sha>/statuses --jq <expr>`:
  // applies the script's own jq filter to the statuses STATUSES gives that SHA.
  fs.writeFileSync(
    path.join(bin, "gh"),
    '#!/usr/bin/env bash\nsha="$(sed -E "s#.*/commits/([0-9a-f]+)/statuses.*#\\1#" <<<"$2")"\n' +
      'jq -c --arg sha "${sha}" \'.[$sha] // []\' <<<"${STATUSES}" | jq -r "$4"\n',
    { mode: 0o755 },
  );
  const status = (state, login = "github-actions[bot]") => ({
    context: "full-suite",
    state,
    creator: { login },
  });
  const coverage = (head, statuses = {}) =>
    spawnSync("bash", [path.join(root, "dev/ci/main-push-coverage.sh"), head], {
      cwd: repo,
      encoding: "utf8",
      env: {
        ...process.env,
        PATH: `${bin}:${process.env.PATH}`,
        REPO: "garden-co/jazz",
        STATUSES: JSON.stringify(statuses),
      },
    });
  try {
    git("init", "-q", "-b", "main");
    git("config", "user.email", "ci@example.com");
    git("config", "user.name", "CI");
    git("config", "core.hooksPath", "/dev/null");
    const base = commitFile("base");
    git("checkout", "-qb", "pr-a");
    const prA = commitFile("a");
    git("checkout", "-qb", "pr-b", base);
    const prB = commitFile("b");
    git("checkout", "-qb", "group", base);
    git("merge", "-q", "--no-ff", "-m", "merge a", prA);
    git("merge", "-q", "--no-ff", "-m", "merge b", prB);
    const tip = git("rev-parse", "HEAD");
    git("update-ref", "refs/remotes/origin/merge-group/4", tip);

    // The queue then merges each PR into main on its own.
    git("checkout", "-q", "main");
    git("merge", "-q", "--no-ff", "-m", "Merge pull request a", prA);
    const mainA = git("rev-parse", "HEAD");
    git("merge", "-q", "--no-ff", "-m", "Merge pull request b", prB);
    const mainB = git("rev-parse", "HEAD");

    const green = { [tip]: [status("success")] };
    for (const head of [mainA, mainB]) {
      const result = coverage(head, green);
      assert.equal(result.status, 0, result.stderr);
      assert.equal(result.stdout.trim(), `merge-group/4 ${tip}`);
    }
    // Without a green full-suite status from Actions on the group's tip,
    // nothing is covered.
    assert.equal(coverage(mainB).status, 1);
    assert.equal(
      coverage(mainB, { [prA]: [status("success")], [prB]: [status("success")] }).status,
      1,
    );
    assert.equal(coverage(mainB, { [tip]: [status("success", "aeplay")] }).status, 1);
    // The latest status from Actions decides.
    assert.equal(coverage(mainB, { [tip]: [status("failure"), status("success")] }).status, 1);

    // A main commit with any other tree runs the suite.
    git("checkout", "-q", "main");
    const other = commitFile("hotfix");
    assert.equal(coverage(other, green).status, 1);
  } finally {
    fs.rmSync(repo, { recursive: true, force: true });
    fs.rmSync(bin, { recursive: true, force: true });
  }
});
