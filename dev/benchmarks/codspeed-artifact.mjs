// Benchmark-only handoff. These bundles are NOT correctness-artifact authority.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { execFileSync, spawnSync } from "node:child_process";
import { createReadStream } from "node:fs";
import { chmod, copyFile, mkdir, readFile, lstat, readdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

// The single table of CodSpeed wall-time workloads. `matrix` prints the names
// for the workflow's build and measurement jobs and `measure` their measurement
// settings, so adding a workload here is the only workflow change it needs.
// One Cargo invocation per workload, so feature unification across workloads
// can never change what a receipt measures. Each entry reproduces the exact
// package, benches, features, thread stack and timeout the workload was
// measured with before builds moved off the macro runner (#3174).
const mimalloc = "jazz-benchmark-guard/mimalloc";
const nativeStack = 4194304;
const nativeExample = (name) => ({
  package: `jazz-example-${name}-benchmark`,
  benches: ["walltime"],
  features: mimalloc,
  minStack: nativeStack,
  timeout: 20,
});

// Two suites share this table. `merge` (pull requests with the `benchmark`
// label and every main merge) measures each workload's `benches`. `nightly`
// (the scheduled run on main, or a manual dispatch) measures only the
// workloads' `nightly` extras: other bench targets of the same package, built
// with the same features, or the same targets with environment that selects
// their other cases. It never re-measures a merge case, so each case gets one
// point per commit in the perf timeline, and it skips workloads and groups
// without extras. Nightly names are ordinary CodSpeed names, so each keeps
// its own history across nightly runs. JAZZ_CODSPEED_SUITE selects the suite;
// unset means `merge`.
export const suites = ["merge", "nightly"];
export function currentSuite(env = process.env) {
  const suite = env.JAZZ_CODSPEED_SUITE || "merge";
  assert.ok(suites.includes(suite), `unknown suite: ${suite}`);
  return suite;
}

// Hero examples in the docs page's order, then the anonymized adopter
// workload and the engine suite. Each hero example owns the areas it measures;
// see dev/EXAMPLES_AND_BENCHMARKS_PROGRAM.md.
const workloadSpecs = {
  "stage-plan": {
    ...nativeExample("stage-plan"),
    timeout: 40,
    // Former W1 diagnostics: scaling points and memory twins.
    nightly: { benches: ["nightly"], env: {}, timeout: 40 },
  },
  "band-chat": {
    ...nativeExample("band-chat"),
    // The band after years of use: 11 cases, each seeding 303,695 messages.
    nightly: { benches: ["nightly"], env: {}, timeout: 40 },
  },
  "band-book": {
    ...nativeExample("band-book"),
    // The rest of the policy sweep: the other arms and the 10k table.
    nightly: { benches: ["nightly"], env: {}, timeout: 20 },
  },
  "world-tour": nativeExample("world-tour"),
  wequencer: nativeExample("wequencer"),
  "poster-shop": nativeExample("poster-shop"),
  "record-player": nativeExample("record-player"),
  "epic-drop": nativeExample("epic-drop"),
  "jamazon-warehouse": nativeExample("jamazon-warehouse"),
  "music-agent": nativeExample("music-agent"),
  "big-label": {
    package: "jazz-example-big-label-benchmark",
    benches: ["ingest_walltime", "loads"],
    features: null,
    minStack: null,
    timeout: 25,
  },
  "permissioned-resources": nativeExample("permissioned-resources"),
  "groove-ivm": {
    package: "groove",
    benches: ["pull_vs_snapshot", "steady_state"],
    features: null,
    minStack: null,
    timeout: 40,
    // The reference engines (SQLite, pull plans, snapshot re-runs) and the
    // smaller IVM sizes, from the same executables. GROOVE_BENCH_SWEEP=1
    // also skips the IVM cases at the largest size, which merges measure.
    nightly: {
      benches: ["pull_vs_snapshot", "steady_state"],
      env: { GROOVE_BENCH_SWEEP: "1" },
      timeout: 40,
    },
  },
};
export const workloads = Object.keys(workloadSpecs);

// CodSpeed jobs. Each group is one build job and one measurement job: the
// build job runs one `cargo codspeed build` per workload in a shared target
// directory (cargo-codspeed replaces only that package's executables, and each
// build keeps its own features, so nothing is unified across workloads), and
// the measurement job runs the workloads one after another under a single
// CodSpeed session. A group's packages enable the same `jazz` features, so
// its build compiles the Jazz stack once: `testing` plus
// `transport-compression-zstd` (docs-and-access), `testing` (stage-plan,
// live-apps, files-and-ops) or none (public-apps). StagePlan, the longest
// measurement, has its own job.
const workloadGroups = {
  "stage-plan": ["stage-plan"],
  "docs-and-access": ["band-book", "permissioned-resources"],
  "live-apps": ["band-chat", "wequencer", "record-player"],
  "files-and-ops": ["epic-drop", "music-agent", "big-label"],
  "public-apps": ["world-tour", "poster-shop", "jamazon-warehouse"],
  engine: ["groove-ivm"],
};
{
  const grouped = Object.values(workloadGroups).flat();
  assert.deepEqual([...grouped].sort(), [...workloads].sort(), "every workload in one group");
  assert.equal(new Set(grouped).size, grouped.length, "no workload in two groups");
}

// The workloads a suite measures: every workload per merge, only those with
// nightly extras at night.
export function suiteWorkloads(suite = currentSuite()) {
  assert.ok(suites.includes(suite), `unknown suite: ${suite}`);
  return workloads.filter((w) => suite === "merge" || workloadSpecs[w].nightly);
}

export function groupWorkloads(group, suite = currentSuite()) {
  assert.ok(Object.hasOwn(workloadGroups, group), "unknown group");
  const measured = suiteWorkloads(suite);
  return workloadGroups[group].filter((w) => measured.includes(w));
}

// The groups a suite builds and measures: those with at least one workload.
export function suiteGroups(suite = currentSuite()) {
  return Object.keys(workloadGroups).filter((g) => groupWorkloads(g, suite).length > 0);
}
export const groups = suiteGroups("merge");
const format = "jazz-codspeed-benchmark-artifact-v2";
// Observed codspeed-macro checkout root. Relative DWARF paths still receive
// origin=unknown; match the absolute repository root uploaded by the runner.
export const measurementWorkspace = "/actions-runner/_work/jazz/jazz";
const baseContract = {
  rust: "1.93.1",
  codspeed: "5.0.1",
  target: "aarch64-unknown-linux-gnu",
  mode: "walltime",
  profile: "bench",
  sourcePaths: { kind: "measurement-workspace-absolute", root: measurementWorkspace },
};

// A workload as the given suite builds and measures it.
function spec(workload, suite = currentSuite()) {
  assert.ok(Object.hasOwn(workloadSpecs, workload), "unknown workload");
  assert.ok(suites.includes(suite), `unknown suite: ${suite}`);
  const { nightly, ...base } = workloadSpecs[workload];
  if (suite === "merge") return { ...base, env: {} };
  assert.ok(nightly, `${workload} has no nightly extras`);
  return { ...base, benches: nightly.benches, env: nightly.env, timeout: nightly.timeout };
}

export function contractFor(workload, suite = currentSuite()) {
  const { package: pkg, benches, features } = spec(workload, suite);
  return { ...baseContract, suite, package: pkg, benches, features };
}

// Per-workload measurement settings for the macro runner. An empty
// RUST_MIN_STACK is unset to Rust std: the default thread stack.
export function measureSettings(suite = currentSuite()) {
  return Object.fromEntries(
    suiteWorkloads(suite).map((w) => [
      w,
      {
        min_stack: workloadSpecs[w].minStack ? String(workloadSpecs[w].minStack) : "",
        timeout: spec(w, suite).timeout,
      },
    ]),
  );
}

// Per-group measurement job settings: the group's workloads run in sequence,
// so its timeout is the sum of theirs.
export function groupMeasureSettings(suite = currentSuite()) {
  return Object.fromEntries(
    suiteGroups(suite).map((group) => [
      group,
      { timeout: groupWorkloads(group, suite).reduce((sum, w) => sum + spec(w, suite).timeout, 0) },
    ]),
  );
}

// Arguments after `cargo codspeed build -m walltime` / `cargo codspeed run -m walltime`.
// Features are chosen at build time only; cargo-codspeed rejects them on `run`.
export function buildArgs(workload, suite = currentSuite()) {
  const { package: pkg, benches, features } = spec(workload, suite);
  return [
    "--package",
    pkg,
    ...benches.flatMap((bench) => ["--bench", bench]),
    ...(features ? ["--features", features] : []),
  ];
}

export function runArgs(workload, suite = currentSuite()) {
  const { package: pkg, benches } = spec(workload, suite);
  return ["--package", pkg, ...benches.flatMap((bench) => ["--bench", bench])];
}

// One shell command that measures a group's workloads in order, each with its
// own measurement thread stack (none: Rust std's default) and the suite's
// environment, stopping at the first failure. The CodSpeed action runs it as
// a single session.
export function runCommand(group, suite = currentSuite()) {
  return groupWorkloads(group, suite)
    .map((workload) => {
      const { minStack, env } = spec(workload, suite);
      const stack = minStack ? `RUST_MIN_STACK=${minStack} ` : "env -u RUST_MIN_STACK ";
      const extra = Object.entries(env).map(([key, value]) => {
        assert.match(key, /^[A-Z_][A-Z0-9_]*$/);
        assert.match(value, /^[\w.-]+$/);
        return `${key}=${value} `;
      });
      return `${stack}${extra.join("")}cargo codspeed run -m walltime ${runArgs(workload, suite).join(" ")}`;
    })
    .join(" && ");
}

export function sourcePathFlags(buildWorkspace) {
  assert.ok(path.isAbsolute(buildWorkspace), "absolute build workspace required");
  // RUSTFLAGS is whitespace-separated, and '=' separates remapping operands.
  assert.doesNotMatch(buildWorkspace, /[\s=]/, "unsupported build workspace characters");
  return `--remap-path-prefix=${path.resolve(buildWorkspace)}=${measurementWorkspace}`;
}

export function verifyMeasurementWorkspace(workspace) {
  assert.equal(workspace, measurementWorkspace, "measurement checkout path changed");
}

function command(file, args) {
  return execFileSync(file, args, { encoding: "utf8", maxBuffer: 8 * 1024 * 1024 }).trim();
}

export function verifyCodspeedVersion(cli) {
  // 5.0.1 propagates Clap's DisplayVersion through anyhow: exact version on
  // stderr, exit 1. Accept only that known response (or conventional exit 0),
  // not arbitrary nonzero commands whose error happens to mention the version.
  const result = spawnSync(cli, ["--version"], { encoding: "utf8" });
  assert.ifError(result.error);
  assert.equal(result.signal, null);
  assert.ok(result.status === 0 || result.status === 1, "version command failed");
  assert.equal(`${result.stdout}${result.stderr}`.trim(), "cargo-codspeed 5.0.1");
}

function context() {
  const source = command("git", ["rev-parse", "HEAD"]);
  assert.equal(source, process.env.GITHUB_SHA, "checkout must match this workflow SHA");
  assert.equal(
    command("git", ["status", "--porcelain", "--untracked-files=no"]),
    "",
    "dirty source",
  );
  assert.match(process.env.GITHUB_RUN_ID ?? "", /^\d+$/, "workflow run ID required");
  return { source, run: process.env.GITHUB_RUN_ID, compiler: command("rustc", ["-vV"]) };
}

async function platform() {
  assert.equal(command("uname", ["-m"]), "aarch64", "ARM64 required");
  const os = await readFile("/etc/os-release", "utf8");
  assert.match(os, /^ID=ubuntu$/m, "Ubuntu required");
  assert.match(os, /^VERSION_ID="22\.04"$/m, "Ubuntu 22.04 required");
  assert.equal(command("getconf", ["GNU_LIBC_VERSION"]), "glibc 2.35", "glibc 2.35 required");
}

function validateContext(value) {
  assert.match(value.source, /^[a-f0-9]{40}$/);
  assert.match(value.run, /^\d+$/);
  assert.match(value.compiler, /^rustc 1\.93\.1 /);
  assert.match(value.compiler, /^host: aarch64-unknown-linux-gnu$/m);
}

async function digest(file) {
  assert.ok((await lstat(file)).isFile(), `expected regular file: ${file}`);
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(file)) hash.update(chunk);
  return hash.digest("hex");
}

export function artifactPaths(workload, suite = currentSuite()) {
  const { package: pkg, benches } = spec(workload, suite);
  return {
    binaries: Object.fromEntries(
      benches.map((bench) => [bench, `target/codspeed/walltime/${pkg}/${bench}`]),
    ),
    bundle: `target/codspeed-artifact-${workload}`,
  };
}

// Exported for filesystem contract tests; the CLI always obtains its own context
// and checks the real host. Hashes catch stale/corrupt artifacts, not a hostile
// workflow author who can also change this verifier.
function bundleFiles(workload, suite = currentSuite()) {
  return [...new Set([...spec(workload, suite).benches, "cargo-codspeed"])].sort();
}

export async function seal(workload, identity, cli, suite = currentSuite()) {
  validateContext(identity);
  const { binaries, bundle } = artifactPaths(workload, suite);
  await mkdir(bundle, { recursive: true });
  for (const [bench, binary] of Object.entries(binaries)) {
    await copyFile(binary, path.join(bundle, bench));
  }
  await copyFile(cli, path.join(bundle, "cargo-codspeed"));
  const files = {};
  for (const name of bundleFiles(workload, suite)) {
    files[name] = await digest(path.join(bundle, name));
  }
  const manifest = { format, ...identity, workload, contract: contractFor(workload, suite), files };
  await writeFile(path.join(bundle, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`);
  return manifest;
}

export async function verify(workload, identity, suite = currentSuite()) {
  validateContext(identity);
  const { bundle } = artifactPaths(workload, suite);
  const expected = bundleFiles(workload, suite);
  assert.deepEqual((await readdir(bundle)).sort(), [...expected, "manifest.json"].sort());
  const manifest = JSON.parse(await readFile(path.join(bundle, "manifest.json"), "utf8"));
  assert.equal(manifest.format, format, "unsupported manifest version");
  assert.equal(manifest.workload, workload, "wrong workload");
  assert.deepEqual(manifest.contract, contractFor(workload, suite), "build contract mismatch");
  for (const key of ["source", "run", "compiler"]) {
    assert.equal(manifest[key], identity[key], `${key} mismatch`);
  }
  assert.deepEqual(Object.keys(manifest.files).sort(), expected);
  for (const name of expected) {
    assert.equal(
      await digest(path.join(bundle, name)),
      manifest.files[name],
      `${name} hash mismatch`,
    );
  }
  return manifest;
}

async function checkExecutable(file, debug = false) {
  const header = command("readelf", ["-h", file]);
  assert.match(header, /Machine:\s+AArch64/, "wrong executable architecture");
  const dependencies = command("ldd", [file]);
  assert.doesNotMatch(dependencies, /not found/, "missing dynamic dependency");
  if (debug) {
    const sections = command("readelf", ["-S", file]);
    assert.match(sections, /\.debug_info\s/, "missing embedded debug info");
    assert.match(sections, /\.debug_line\s/, "missing embedded source line tables");
  }
  console.log(`${file}\n${dependencies}`);
}

async function main() {
  const [action, workload] = process.argv.slice(2);
  if (action === "rustflags") {
    console.log(sourcePathFlags(process.cwd()));
    return;
  }
  if (action === "matrix") {
    console.log(JSON.stringify(suiteWorkloads()));
    return;
  }
  if (action === "measure") {
    console.log(JSON.stringify(measureSettings()));
    return;
  }
  if (action === "groups") {
    console.log(JSON.stringify(suiteGroups()));
    return;
  }
  if (action === "suite") {
    console.log(currentSuite());
    return;
  }
  if (action === "group-measure") {
    console.log(JSON.stringify(groupMeasureSettings()));
    return;
  }
  if (action === "group-workloads") {
    console.log(groupWorkloads(workload).join(" "));
    return;
  }
  if (action === "run-command") {
    console.log(runCommand(workload));
    return;
  }
  if (action === "build-args" || action === "run-args") {
    console.log((action === "build-args" ? buildArgs : runArgs)(workload).join(" "));
    return;
  }
  const { binaries, bundle } = artifactPaths(workload);
  assert.ok(
    ["seal", "install"].includes(action),
    "usage: codspeed-artifact.mjs seal|install|build-args|run-args WORKLOAD | group-workloads|run-command GROUP | rustflags | matrix | measure | groups | group-measure | suite (JAZZ_CODSPEED_SUITE=merge|nightly)",
  );
  await platform();
  const identity = context();
  if (action === "seal") {
    // Do not silently accept a runner image's host-specific compiler overrides.
    for (const key of [
      "RUSTFLAGS",
      "CARGO_ENCODED_RUSTFLAGS",
      "CFLAGS",
      "CXXFLAGS",
      "CARGO_BUILD_TARGET",
      "CARGO_TARGET_DIR",
    ]) {
      assert.ok(!process.env[key], `unexpected build override: ${key}`);
    }
    const cli = command("which", ["cargo-codspeed"]);
    verifyCodspeedVersion(cli);
    for (const binary of Object.values(binaries)) await checkExecutable(binary, true);
    await checkExecutable(cli);
    const manifest = await seal(workload, identity, cli);
    console.log(JSON.stringify(manifest, null, 2));
  } else {
    // Fail before executing the downloaded CLI if the runner's layout drifts.
    verifyMeasurementWorkspace(process.cwd());
    await verify(workload, identity);
    // upload-artifact normalizes permissions. Restore only the verified
    // executables, at fixed paths; never execute a path supplied by a manifest.
    for (const name of bundleFiles(workload)) {
      await chmod(path.join(bundle, name), 0o755);
      await checkExecutable(path.join(bundle, name), name !== "cargo-codspeed");
    }
    for (const [bench, binary] of Object.entries(binaries)) {
      await mkdir(path.dirname(binary), { recursive: true });
      await copyFile(path.join(bundle, bench), binary);
      await chmod(binary, 0o755);
    }
    await writeFile(process.env.GITHUB_PATH, `${path.resolve(bundle)}\n`, { flag: "a" });
    console.log(
      `Verified ${workload} for ${identity.source}; no compilation on measurement runner.`,
    );
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(path.resolve(process.argv[1])).href) {
  await main();
}
