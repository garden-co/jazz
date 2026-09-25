// Benchmark-only handoff. These bundles are NOT correctness-artifact authority.
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { execFileSync, spawnSync } from "node:child_process";
import { createReadStream } from "node:fs";
import { chmod, copyFile, mkdir, readFile, lstat, readdir, writeFile } from "node:fs/promises";
import path from "node:path";
import { pathToFileURL } from "node:url";

// One Cargo invocation per workload, so feature unification across workloads
// can never change what a receipt measures. Each entry reproduces the exact
// package, benches and features the workload was measured with before builds
// moved off the macro runner (#3174).
const mimalloc = "jazz-benchmark-guard/mimalloc";
const workloadSpecs = {
  todo: { package: "jazz-example-todo-benchmark", benches: ["walltime"], features: mimalloc },
  "permissioned-resources": {
    package: "jazz-example-permissioned-resources-benchmark",
    benches: ["walltime"],
    features: mimalloc,
  },
  "policy-scoped-documents": {
    package: "jazz-example-policy-scoped-documents-benchmark",
    benches: ["walltime"],
    features: mimalloc,
  },
  "big-label-ingest": {
    package: "jazz-example-big-label-benchmark",
    benches: ["ingest_walltime"],
    features: null,
  },
  "w1-reads": {
    package: "jazz-example-benchmark-w1",
    benches: ["reads_memory_walltime", "reads_rocksdb_walltime"],
    features: null,
  },
  "route-subscription": {
    package: "jazz",
    benches: ["route_subscription_curve"],
    features: "testing",
  },
  "groove-ivm": {
    package: "groove",
    benches: ["pull_vs_snapshot", "steady_state"],
    features: null,
  },
  "selective-hydration": {
    package: "jazz",
    benches: ["selective_global_hydration"],
    features: "testing",
  },
};
export const workloads = Object.keys(workloadSpecs);
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

function spec(workload) {
  assert.ok(Object.hasOwn(workloadSpecs, workload), "unknown workload");
  return workloadSpecs[workload];
}

export function contractFor(workload) {
  return { ...baseContract, ...spec(workload) };
}

// Arguments after `cargo codspeed build -m walltime` / `cargo codspeed run -m walltime`.
// Features are chosen at build time only; cargo-codspeed rejects them on `run`.
export function buildArgs(workload) {
  const { package: pkg, benches, features } = spec(workload);
  return [
    "--package",
    pkg,
    ...benches.flatMap((bench) => ["--bench", bench]),
    ...(features ? ["--features", features] : []),
  ];
}

export function runArgs(workload) {
  const { package: pkg, benches } = spec(workload);
  return ["--package", pkg, ...benches.flatMap((bench) => ["--bench", bench])];
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

export function artifactPaths(workload) {
  const { package: pkg, benches } = spec(workload);
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
function bundleFiles(workload) {
  return [...spec(workload).benches, "cargo-codspeed"].sort();
}

export async function seal(workload, identity, cli) {
  validateContext(identity);
  const { binaries, bundle } = artifactPaths(workload);
  await mkdir(bundle, { recursive: true });
  for (const [bench, binary] of Object.entries(binaries)) {
    await copyFile(binary, path.join(bundle, bench));
  }
  await copyFile(cli, path.join(bundle, "cargo-codspeed"));
  const files = {};
  for (const name of bundleFiles(workload)) {
    files[name] = await digest(path.join(bundle, name));
  }
  const manifest = { format, ...identity, workload, contract: contractFor(workload), files };
  await writeFile(path.join(bundle, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`);
  return manifest;
}

export async function verify(workload, identity) {
  validateContext(identity);
  const { bundle } = artifactPaths(workload);
  const expected = bundleFiles(workload);
  assert.deepEqual((await readdir(bundle)).sort(), [...expected, "manifest.json"].sort());
  const manifest = JSON.parse(await readFile(path.join(bundle, "manifest.json"), "utf8"));
  assert.equal(manifest.format, format, "unsupported manifest version");
  assert.equal(manifest.workload, workload, "wrong workload");
  assert.deepEqual(manifest.contract, contractFor(workload), "build contract mismatch");
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
  if (action === "build-args" || action === "run-args") {
    console.log((action === "build-args" ? buildArgs : runArgs)(workload).join(" "));
    return;
  }
  const { binaries, bundle } = artifactPaths(workload);
  assert.ok(
    ["seal", "install"].includes(action),
    "usage: codspeed-artifact.mjs seal|install|build-args|run-args WORKLOAD",
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
