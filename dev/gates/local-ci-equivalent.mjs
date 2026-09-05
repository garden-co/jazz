#!/usr/bin/env node
/**
 * The executable correctness/build commands shared by CI and local development.
 *
 * `--ci-equivalent` is deliberately strict: it runs every named command
 * partition invoked by .github/workflows/ci-suite.yml. GitHub schedules those
 * partitions concurrently; this local entry point serializes them so one
 * checkout can reuse its Cargo/artifact caches safely. It is command-equivalent
 * to CI, not a claim that it reproduces GitHub credentials or runner images.
 *
 * The default is `--focused`, which is intentionally smaller and prints that it
 * is NOT CI-equivalent. Never describe it as a full CI result.
 */

import { spawn } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, resolve } from "node:path";
import { performance } from "node:perf_hooks";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "../..");

export const RUST_WORKSPACE_TARGETS = Object.freeze([
  "--lib",
  "--bins",
  "--tests",
  "--examples",
  "--benches",
]);
export const RUST_CI_FEATURES =
  "jazz/testing,jazz/transport-compression-zstd,jazz-server/test,jazz-cli/test";

// CI setup supplies this one coordination input. Every other JAZZ_* variable
// is a test, fixture, benchmark, timeout, or assertion control and would make
// a local exact-partition result mean something different from GitHub CI.
export const ALLOWED_INHERITED_CI_JAZZ_ENV = Object.freeze(["JAZZ_TEST_ARTIFACT_LOCK_PATH"]);
const allowedInheritedCiJazzEnv = new Set(ALLOWED_INHERITED_CI_JAZZ_ENV);

/** The CI-suite job that invokes each named partition. */
export const ciPartitionJobs = Object.freeze({
  lint: "lint",
  "rust-workspace": "test-rust-workspace",
  "rust-differential": "test-rust-differential",
  "storage-compat": "test-storage-compat",
  typescript: "test-ts",
  "react-native": "test-react-native",
});

const command = (label, executable, args, options = {}) =>
  Object.freeze({ label, executable, args: Object.freeze(args), ...options });

const m3DifferentialCommand = command(
  "bounded maintained-vs-one-shot differential oracle",
  "bash",
  [
    "-lc",
    String.raw`set -euo pipefail
test_binary="$(cargo test -p jazz --lib --features testing,transport-compression-zstd --no-run --message-format=json | node -e '
  const readline = require("node:readline");
  let executable;
  const lines = readline.createInterface({ input: process.stdin });
  lines.on("line", (line) => {
    try {
      const message = JSON.parse(line);
      if (message.reason === "compiler-artifact" && message.target.name === "jazz" && message.executable)
        executable = message.executable;
    } catch {}
  });
  lines.on("close", () => {
    if (!executable) process.exitCode = 1;
    else console.log(executable);
  });
')"
test -x "$test_binary"
timeout 60s env \
  JAZZ_SEED=11 \
  JAZZ_DIFFERENTIAL_CHURN_DEPTHS=10,1000 \
  JAZZ_DIFFERENTIAL_STEP_COUNT=3 \
  "$test_binary" node::tests::harness::m3_maintained_one_shot_differential_oracle --exact --ignored`,
  ],
);

/**
 * CI partitions are the source of truth for both CI and `--ci-equivalent`.
 * Keep the labels stable: contracts use them to prove a selected failure stops
 * the partition before an unrelated later command can hide it.
 */
export const ciPartitions = Object.freeze({
  lint: Object.freeze([
    command("format check", "pnpm", ["format:check"]),
    command("lint", "pnpm", ["lint"]),
    command("crate publication selector contract", "bash", [
      "dev/scripts/publish-crates-alpha.test.sh",
    ]),
    command("CI workflow contracts", "pnpm", ["test:ci-workflow"]),
    command("Turbo cache-input contracts", "pnpm", ["test:turbo-cache-inputs"]),
    command("invariant registry", "bash", ["dev/gates/invariant-registry.sh"]),
    command("ignored-test inventory", "node", ["dev/gates/ignored-tests.mjs"]),
  ]),
  "rust-workspace": Object.freeze([
    command("workspace Rust tests", "node", [
      "dev/gates/run-rust-tests.mjs",
      "--require-nextest",
      // These benchmark-derived correctness tests run through the broad
      // workspace Nextest selection. Keep their exact selected inventory
      // explicit here: the standalone smoke gate remains available to callers
      // that do not already run this partition.
      "--require-nextest-test",
      "jazz::legacy_benchmark_smoke=cold_subscription_correctness_smoke",
      "--require-nextest-test",
      "jazz::legacy_benchmark_smoke=sync_correctness_smoke",
      "--require-nextest-test",
      "jazz::legacy_benchmark_smoke=validation_correctness_smoke",
      "--require-nextest-test",
      "jazz::legacy_benchmark_smoke=relation_include_delivery_correctness_smoke",
      "--require-nextest-test",
      "jazz::legacy_benchmark_smoke=route_subscription_curve_correctness_smoke",
      "--require-nextest-test",
      "jazz-sim::scenario_smoke=s1_saas_smoke",
      "--require-nextest-test",
      "jazz-sim::scenario_smoke=micro_correctness_smoke",
      "--require-nextest-test",
      "jazz-sim::scenario_smoke=s1_saas_db_surface_smoke",
      "--require-nextest-test",
      "jazz-sim::scenario_smoke=s2_canvas_smoke",
      "--require-nextest-test",
      "jazz-sim::scenario_smoke=s3_permissions_smoke",
      "--require-nextest-test",
      "jazz-sim::scenario_smoke=s4_order_processing_smoke_debug_profile",
      "--require-nextest-test",
      "jazz-sim::scenario_smoke=s5_durable_stream_smoke",
      "--require-nextest-test",
      "jazz-sim::scenario_smoke=s7_migrations_smoke",
      "--require-nextest-test",
      "jazz-sim::scenario_smoke=s8_branch_views_smoke",
      "--require-nextest-test",
      "jazz-sim::scenario_smoke=s9_durable_execution_smoke",
      "--timeout-seconds",
      "780",
      "--nextest-profile",
      "jazz-ci",
      "--",
      "--workspace",
      "--lib",
      "--bins",
      "--tests",
      "--features",
      RUST_CI_FEATURES,
    ]),
    command("Nextest partition coverage", "node", [
      "--test",
      "dev/gates/test/nextest-partitions.test.mjs",
    ]),
  ]),
  "rust-differential": Object.freeze([m3DifferentialCommand]),
  typescript: Object.freeze([
    command("all Rust workspace target classes", "cargo", [
      "check",
      "--workspace",
      ...RUST_WORKSPACE_TARGETS,
      "--features",
      RUST_CI_FEATURES,
    ]),
    command("native correctness-artifact producer", "node", [
      "dev/gates/ensure-correctness-artifacts.mjs",
    ]),
    command("preinstalled Chromium", "pnpm", [
      "exec",
      "playwright",
      "install",
      "--dry-run",
      "chromium",
    ]),
    command("TypeScript consumers", "pnpm", ["test:typescript-consumers"], {
      // The consumer runner intentionally supports command overrides for its
      // own harness tests. A CI-equivalent invocation must not inherit one.
      env: { JAZZ_REQUIRE_CI_TEST_COMMANDS: "1" },
    }),
  ]),
  "react-native": Object.freeze([
    // React Native's bridge is deliberately opt-in. This producer must be
    // separate from the ordinary TypeScript partition so its manifest proves
    // the native bridge was actually present when the RN suite ran.
    command(
      "React Native bridge correctness-artifact producer",
      "pnpm",
      ["build:correctness-artifacts"],
      { env: { JAZZ_RN_TEST_BRIDGE: "1" } },
    ),
    command(
      "admitted Jazz Tools build for React Native",
      "node",
      [
        "dev/gates/run-correctness-consumer.mjs",
        "--",
        "pnpm",
        "exec",
        "turbo",
        "run",
        "build",
        "--filter=jazz-tools",
        "--only",
      ],
      { env: { JAZZ_RN_TEST_BRIDGE: "1" } },
    ),
    command(
      "React Native bridge tests",
      "node",
      [
        "dev/gates/run-correctness-consumer.mjs",
        "--",
        "pnpm",
        "--dir",
        "packages/jazz-tools",
        "exec",
        "vitest",
        "run",
        "--config",
        "vitest.react-native.config.ts",
      ],
      { env: { JAZZ_RN_TEST_BRIDGE: "1" } },
    ),
  ]),
  "storage-compat": Object.freeze([
    command("native storage compatibility corpus", "bash", ["dev/gates/storage-compat.sh"]),
    command("native correctness-artifact producer", "node", [
      "dev/gates/ensure-correctness-artifacts.mjs",
    ]),
    command("preinstalled Chromium", "pnpm", [
      "exec",
      "playwright",
      "install",
      "--dry-run",
      "chromium",
    ]),
    command("browser storage compatibility corpus", "pnpm", [
      "--dir",
      "packages/jazz-tools",
      "test:browser:focused",
      "--",
      "tests/browser/indexeddb-jazz-compat.test.ts",
    ]),
  ]),
});

export const focusedCommands = Object.freeze([
  command("format check", "pnpm", ["format:check"]),
  command("invariant registry", "bash", ["dev/gates/invariant-registry.sh"]),
  command("ignored-test validator self-test", "node", [
    "dev/gates/ignored-tests.mjs",
    "--self-test",
  ]),
  command("core Rust library compile", "cargo", ["check", "-p", "jazz", "--lib"]),
  command("jazz-tools typecheck", "pnpm", ["--filter", "jazz-tools", "check"]),
]);

export function planFor({ mode, partition } = {}) {
  if (partition !== undefined) {
    const commands = ciPartitions[partition];
    if (!commands) throw new Error(`unknown CI partition: ${partition}`);
    return commands;
  }
  if (mode === "ci-equivalent") return Object.freeze(Object.values(ciPartitions).flat());
  if (mode === "focused") return focusedCommands;
  throw new Error(`unknown local CI mode: ${mode}`);
}

export function assertFullWorkspaceCoverage(commands) {
  const workspaceCheck = commands.find(
    ({ executable, args }) =>
      executable === "cargo" && args[0] === "check" && args.includes("--workspace"),
  );
  if (!workspaceCheck)
    throw new Error("CI-equivalent plan omits the workspace target-class check.");
  for (const target of RUST_WORKSPACE_TARGETS)
    if (!workspaceCheck.args.includes(target))
      throw new Error(`CI-equivalent workspace check omits required target class ${target}.`);
  const featureIndex = workspaceCheck.args.indexOf("--features");
  if (featureIndex < 0 || workspaceCheck.args[featureIndex + 1] !== RUST_CI_FEATURES)
    throw new Error("CI-equivalent workspace check omits the required CI feature selection.");
}

export function assertArtifactBoundary(commands) {
  const artifacts = commands.find(({ label }) => label === "native correctness-artifact producer");
  const suites = commands.find(({ label }) => label === "TypeScript consumers");
  if (!artifacts || !suites)
    throw new Error("CI-equivalent TypeScript plan omits artifacts or test suites.");
  if (commands.indexOf(artifacts) > commands.indexOf(suites))
    throw new Error("CI-equivalent TypeScript plan runs tests before correctness artifacts.");
}

export function assertReactNativeBridgeBoundary(commands) {
  const producer = commands.find(
    ({ label }) => label === "React Native bridge correctness-artifact producer",
  );
  const toolsBuild = commands.find(
    ({ label }) => label === "admitted Jazz Tools build for React Native",
  );
  const tests = commands.find(({ label }) => label === "React Native bridge tests");
  if (!producer || !toolsBuild || !tests)
    throw new Error("CI-equivalent React Native plan omits its bridge producer, build, or tests.");
  for (const item of [producer, toolsBuild, tests]) {
    if (item.env?.JAZZ_RN_TEST_BRIDGE !== "1")
      throw new Error(
        `CI-equivalent React Native plan omits JAZZ_RN_TEST_BRIDGE for ${item.label}.`,
      );
  }
  if (commands.indexOf(producer) > commands.indexOf(toolsBuild))
    throw new Error("CI-equivalent React Native plan builds Jazz Tools before bridge artifacts.");
  if (commands.indexOf(toolsBuild) > commands.indexOf(tests))
    throw new Error(
      "CI-equivalent React Native plan runs tests before the admitted Jazz Tools build.",
    );
  if (!tests.args.includes("vitest.react-native.config.ts"))
    throw new Error("CI-equivalent React Native plan omits the React Native Vitest configuration.");
  if (tests.args.includes("--passWithNoTests"))
    throw new Error("CI-equivalent React Native plan permits an empty React Native test run.");
  const configPath = resolve(root, "packages/jazz-tools/vitest.react-native.config.ts");
  if (!existsSync(configPath))
    throw new Error(
      "CI-equivalent React Native plan references a missing React Native Vitest configuration.",
    );
  const config = readFileSync(configPath, "utf8");
  if (!/include:\s*\[\s*["']tests\/react-native\/\*\*\/\*\.test\.\{ts,tsx\}["']\s*\]/.test(config))
    throw new Error(
      "CI-equivalent React Native Vitest configuration does not select React Native tests.",
    );
}

/**
 * Reject ambient Jazz controls before any exact partition begins.  Command
 * local controls are appended only after this check, so callers cannot smuggle
 * an override through a child process environment.
 */
export function exactCiEnvironment(parentEnvironment = process.env) {
  const forbidden = Object.keys(parentEnvironment)
    .filter((name) => name.startsWith("JAZZ_") && !allowedInheritedCiJazzEnv.has(name))
    .sort();
  if (forbidden.length) {
    throw new Error(
      `CI-equivalent gate refuses inherited Jazz control(s): ${forbidden.join(", ")}. ` +
        "Unset them; only JAZZ_TEST_ARTIFACT_LOCK_PATH is inherited from CI setup.",
    );
  }
  return Object.fromEntries(
    Object.entries(parentEnvironment).filter(
      ([name]) => !name.startsWith("JAZZ_") || allowedInheritedCiJazzEnv.has(name),
    ),
  );
}

export function commandEnvironment(baseEnvironment, localEnvironment = {}) {
  return { ...baseEnvironment, ...localEnvironment };
}

export async function runPlan(commands, run = runCommand, baseEnvironment = process.env) {
  for (const item of commands) await run(item, baseEnvironment);
}

export async function runCommand(
  { label, executable, args, env = {} },
  baseEnvironment = process.env,
) {
  const started = performance.now();
  console.log(`local-ci: start ${label}: ${[executable, ...args].join(" ")}`);
  const status = await new Promise((resolvePromise, reject) => {
    const child = spawn(executable, args, {
      cwd: root,
      stdio: "inherit",
      env: commandEnvironment(baseEnvironment, env),
    });
    child.once("error", reject);
    child.once("exit", (code, signal) => resolvePromise({ code, signal }));
  });
  const elapsedSeconds = ((performance.now() - started) / 1000).toFixed(1);
  if (status.code !== 0)
    throw new Error(
      `local-ci: ${label} failed with ${status.signal ?? `exit ${status.code ?? 1}`} after ${elapsedSeconds}s`,
    );
  console.log(`local-ci: done ${label} (${elapsedSeconds}s)`);
}

function usage() {
  console.error(`Usage: node dev/gates/local-ci-equivalent.mjs [--focused | --ci-equivalent | --ci-partition NAME]

Default: --focused (NOT CI-equivalent; targeted iteration only).
--ci-equivalent: all exact command partitions invoked by CI, serialized locally.
--ci-partition: one named CI partition for .github/workflows/ci-suite.yml.`);
}

async function main(argv) {
  let mode = "focused";
  let partition;
  if (argv.length === 1 && ["--help", "-h"].includes(argv[0])) {
    usage();
    return;
  } else if (argv.length === 1 && argv[0] === "--focused") mode = "focused";
  else if (argv.length === 1 && argv[0] === "--ci-equivalent") mode = "ci-equivalent";
  else if (argv.length === 2 && argv[0] === "--ci-partition") partition = argv[1];
  else if (argv.length !== 0) {
    usage();
    throw new Error("invalid local CI mode");
  }

  const commands = planFor({ mode, partition });
  const isExactPartition = mode === "ci-equivalent" || partition !== undefined;
  if (isExactPartition) {
    const baseEnvironment = exactCiEnvironment();
    if (mode === "ci-equivalent") {
      assertFullWorkspaceCoverage(commands);
      assertArtifactBoundary(commands);
      assertReactNativeBridgeBoundary(commands);
      console.log("local-ci: CI-equivalent mode; running every CI command partition serially.");
    }
    if (mode === "ci-equivalent" || partition === "react-native")
      assertReactNativeBridgeBoundary(planFor({ partition: "react-native" }));
    await runPlan(commands, runCommand, baseEnvironment);
  } else {
    console.log("local-ci: focused mode only; this is NOT CI-equivalent.");
    await runPlan(commands);
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main(process.argv.slice(2)).catch((error) => {
    console.error(error.message);
    process.exitCode = 1;
  });
}
