#!/usr/bin/env node
/**
 * Assemble the artifacts used exclusively by the TypeScript correctness job.
 *
 * This deliberately does not use Turbo's generic `build` task: that task is
 * the packaging graph and builds the release WASM artifact. Browser tests do
 * not observe WASM optimisation, but they do need a binding whose provenance
 * matches this checkout. Keep the fast artifact's profile explicit here so a
 * future package build cannot silently change test semantics.
 */
import { spawn } from "node:child_process";
import { performance } from "node:perf_hooks";
import { fileURLToPath } from "node:url";
import { resolve } from "node:path";

const root = resolve(fileURLToPath(new URL("../..", import.meta.url)));

export function command(command, args, label = [command, ...args].join(" "), env) {
  const started = performance.now();
  console.log(`test-artifacts: start ${label}`);
  return new Promise((resolvePromise, reject) => {
    const child = spawn(command, args, {
      cwd: root,
      stdio: "inherit",
      shell: process.platform === "win32",
      env: { ...process.env, ...env },
    });
    child.once("error", reject);
    child.once("exit", (status, signal) => {
      const seconds = ((performance.now() - started) / 1000).toFixed(1);
      if (status === 0) {
        console.log(`test-artifacts: done ${label} (${seconds}s)`);
        resolvePromise();
      } else {
        reject(
          new Error(`${label} failed with ${signal ?? `exit ${status ?? 1}`} after ${seconds}s`),
        );
      }
    });
  });
}

export async function buildTestArtifacts(run = command) {
  // These are independent Cargo invocations. In particular, neither binding
  // needs the CLI binary, and jazz-tools only needs that binary plus WASM.
  const cli = run("pnpm", ["--filter", "@jazz/rust", "build:crates"], "CLI");
  // Separate target directories avoid Cargo's artifact-directory lock while
  // preserving stable per-lane incremental caches. CI's sccache still shares
  // compiled units across the target directories.
  const wasm = run("pnpm", ["--filter", "jazz-wasm", "build:fast"], "fast WASM", {
    CARGO_BUILD_JOBS: "2",
    CARGO_TARGET_DIR: "target/test-artifacts-wasm",
  });
  const napi = run("pnpm", ["--filter", "jazz-napi", "build"], "release NAPI", {
    CARGO_BUILD_JOBS: "2",
    CARGO_TARGET_DIR: "target/test-artifacts-napi",
  });

  const tools = Promise.all([cli, wasm]).then(() =>
    run("pnpm", ["--filter", "jazz-tools", "build"], "jazz-tools"),
  );
  await Promise.all([tools, napi]);

  // A manifest is the contract that makes a cached/generated artifact safe to
  // consume. NAPI is built release because that is the loadable Linux mode;
  // the fast profile is intentionally WASM-only and correctness-only.
  await run(
    "node",
    ["dev/artifacts/provenance.mjs", "verify", "wasm", "fast"],
    "verify fast WASM provenance",
  );
  await run(
    "node",
    ["dev/artifacts/provenance.mjs", "verify", "napi", "release"],
    "verify release NAPI provenance",
  );

  try {
    await run("node", ["-e", "require('./crates/jazz-napi')"], "load release NAPI");
  } catch (error) {
    // A damaged native artifact must not make every run pay a second build.
    // Repair only after the first load proves it necessary, then prove repair.
    console.warn(`test-artifacts: release NAPI did not load; repairing (${error.message})`);
    await run("pnpm", ["--filter", "jazz-napi", "build"], "repair release NAPI", {
      CARGO_BUILD_JOBS: "2",
      CARGO_TARGET_DIR: "target/test-artifacts-napi",
    });
    await run(
      "node",
      ["dev/artifacts/provenance.mjs", "verify", "napi", "release"],
      "verify repaired NAPI provenance",
    );
    await run("node", ["-e", "require('./crates/jazz-napi')"], "load repaired release NAPI");
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  buildTestArtifacts().catch((error) => {
    console.error(`test-artifacts: ${error.message}`);
    process.exitCode = 1;
  });
}
