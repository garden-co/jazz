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
export const testArtifactTargets = {
  napi: resolve(root, "target/test-artifacts-napi"),
  wasm: resolve(root, "target/test-artifacts-wasm"),
};

export function command(command, args, label = [command, ...args].join(" "), options = {}) {
  const { env, signal } = options;
  const started = performance.now();
  console.log(`test-artifacts: start ${label}`);
  return new Promise((resolvePromise, reject) => {
    let settled = false;
    let forceKillTimer;
    const child = spawn(command, args, {
      cwd: root,
      stdio: "inherit",
      shell: process.platform === "win32",
      env: { ...process.env, ...env },
      // pnpm, wasm-pack and napi all spawn Cargo. A process group lets one
      // failed sibling terminate the complete command tree instead of leaving
      // compilers running after the pipeline has already failed.
      detached: process.platform !== "win32",
    });
    const finish = (callback) => {
      if (settled) return;
      settled = true;
      clearTimeout(forceKillTimer);
      signal?.removeEventListener("abort", abort);
      process.removeListener("SIGINT", abort);
      process.removeListener("SIGTERM", abort);
      callback();
    };
    const abort = () => {
      if (child.exitCode !== null) return;
      if (process.platform === "win32") {
        spawn("taskkill", ["/pid", String(child.pid), "/T", "/F"], { stdio: "ignore" });
      } else {
        try {
          process.kill(-child.pid, "SIGTERM");
        } catch (error) {
          if (error.code !== "ESRCH") throw error;
        }
        forceKillTimer = setTimeout(() => {
          try {
            process.kill(-child.pid, "SIGKILL");
          } catch (error) {
            if (error.code !== "ESRCH")
              console.warn(`test-artifacts: force-kill failed: ${error.message}`);
          }
        }, 5_000);
        forceKillTimer.unref();
      }
    };
    signal?.addEventListener("abort", abort, { once: true });
    process.once("SIGINT", abort);
    process.once("SIGTERM", abort);
    if (signal?.aborted) abort();
    child.once("error", (error) => finish(() => reject(error)));
    child.once("exit", (status, signal) => {
      const seconds = ((performance.now() - started) / 1000).toFixed(1);
      if (status === 0) {
        console.log(`test-artifacts: done ${label} (${seconds}s)`);
        finish(resolvePromise);
      } else {
        finish(() =>
          reject(
            new Error(`${label} failed with ${signal ?? `exit ${status ?? 1}`} after ${seconds}s`),
          ),
        );
      }
    });
  });
}

export async function buildTestArtifacts(run = command) {
  const controller = new AbortController();
  let firstBuildError;
  const guardedRun = (command, args, label, env) =>
    run(command, args, label, { env, signal: controller.signal }).catch((error) => {
      if (!firstBuildError) {
        firstBuildError = error;
        controller.abort(error);
      }
      throw error;
    });

  // These are independent Cargo invocations. In particular, neither binding
  // needs the CLI binary, and jazz-tools only needs that binary plus WASM.
  const cli = guardedRun("pnpm", ["--filter", "@jazz/rust", "build:crates"], "CLI");
  // Separate target directories avoid Cargo's artifact-directory lock while
  // preserving stable per-lane incremental caches. CI's sccache still shares
  // compiled units across the target directories.
  const wasm = guardedRun("pnpm", ["--filter", "jazz-wasm", "build:fast"], "fast WASM", {
    CARGO_TARGET_DIR: testArtifactTargets.wasm,
  });
  const napi = guardedRun("pnpm", ["--filter", "jazz-napi", "build"], "release NAPI", {
    CARGO_TARGET_DIR: testArtifactTargets.napi,
  });

  const tools = Promise.all([cli, wasm]).then(() =>
    guardedRun("pnpm", ["--filter", "jazz-tools", "build"], "jazz-tools"),
  );
  try {
    await Promise.all([tools, napi]);
  } catch (error) {
    throw firstBuildError ?? error;
  }

  // A manifest is the contract that makes a cached/generated artifact safe to
  // consume. NAPI is built release because that is the loadable Linux mode;
  // the fast profile is intentionally WASM-only and correctness-only.
  await guardedRun(
    "node",
    ["dev/artifacts/provenance.mjs", "verify", "wasm", "fast"],
    "verify fast WASM provenance",
  );

  try {
    await guardedRun("node", ["-e", "require('./crates/jazz-napi')"], "load release NAPI");
  } catch (error) {
    // A damaged native artifact must not make every run pay a second build.
    // Repair only after the first load proves it necessary, then prove repair.
    console.warn(`test-artifacts: release NAPI did not load; repairing (${error.message})`);
    // The first load failure aborts only the already-completed first phase.
    // Use a fresh controller for the bounded repair and its validation.
    controller.abort(error);
    const repairController = new AbortController();
    const repairRun = (command, args, label, env) =>
      run(command, args, label, { env, signal: repairController.signal }).catch((repairError) => {
        repairController.abort(repairError);
        throw repairError;
      });
    await repairRun("pnpm", ["--filter", "jazz-napi", "build"], "repair release NAPI", {
      CARGO_TARGET_DIR: testArtifactTargets.napi,
    });
    await repairRun("node", ["-e", "require('./crates/jazz-napi')"], "load repaired release NAPI");
  }
  await run(
    "node",
    ["dev/artifacts/provenance.mjs", "verify", "napi", "release"],
    "verify release NAPI provenance",
  );
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  buildTestArtifacts().catch((error) => {
    console.error(`test-artifacts: ${error.message}`);
    process.exitCode = 1;
  });
}
