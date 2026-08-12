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
import { execFileSync, spawn } from "node:child_process";
import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { isAbsolute, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { fileURLToPath } from "node:url";

const root = resolve(fileURLToPath(new URL("../..", import.meta.url)));

function sharedGitDirectory(cwd = root) {
  const directory = execFileSync("git", ["rev-parse", "--git-common-dir"], {
    cwd,
    encoding: "utf8",
  }).trim();
  return isAbsolute(directory) ? directory : resolve(cwd, directory);
}

function ownerIsAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    // EPERM means the process exists but belongs to another user. This is
    // common on shared CI hosts and must never be mistaken for a stale lock.
    if (error.code === "EPERM") return true;
    if (error.code === "ESRCH") return false;
    throw error;
  }
}

/**
 * The common Git directory is shared by every linked worktree in a clone.
 * Put the lock there rather than in a checkout so separately checked-out
 * branches serialize only when they can share Cargo/package state. A test
 * hook overrides the exact path without exposing a real checkout in receipts.
 */
export function artifactLockPath(cwd = root) {
  return (
    process.env.JAZZ_TEST_ARTIFACT_LOCK_PATH ??
    resolve(sharedGitDirectory(cwd), "jazz-test-artifacts.lock")
  );
}

function readLockOwner(lockPath) {
  try {
    return JSON.parse(readFileSync(resolve(lockPath, "owner.json"), "utf8"));
  } catch (error) {
    if (error.code === "ENOENT") return undefined;
    throw new Error(
      `test-artifacts: lock at ${lockPath} has unreadable owner metadata: ${error.message}`,
    );
  }
}

function lockError(lockPath, owner) {
  const started = typeof owner.startedAt === "string" ? owner.startedAt : "unknown";
  const cwd = typeof owner.cwd === "string" ? owner.cwd : "unknown";
  return new Error(
    `test-artifacts: another artifact build is active (pid ${owner.pid}, cwd ${cwd}, started ${started}). ` +
      `Wait for it to finish, or if it is no longer running retry to recover its stale lock. Lock: ${lockPath}`,
  );
}

/** Acquire an exclusive, clone-wide artifact build lock. */
export function acquireArtifactBuildLock(lockPath = artifactLockPath()) {
  const owner = {
    pid: process.pid,
    cwd: process.cwd(),
    startedAt: new Date().toISOString(),
    token: `${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2)}`,
  };
  try {
    mkdirSync(lockPath);
  } catch (error) {
    if (error.code !== "EEXIST") throw error;
    const existing = readLockOwner(lockPath);
    if (!existing || !Number.isInteger(existing.pid) || existing.pid <= 0)
      throw new Error(
        `test-artifacts: lock at ${lockPath} has no usable owner metadata; refusing to delete it.`,
      );
    if (ownerIsAlive(existing.pid)) throw lockError(lockPath, existing);
    // Only a positively dead owner is safe to recover. Compare the metadata
    // again before removal so a newly acquired lock can never be deleted.
    const unchanged = readLockOwner(lockPath);
    if (unchanged?.token !== existing.token)
      throw new Error(
        `test-artifacts: lock at ${lockPath} changed while checking it; retry safely.`,
      );
    rmSync(lockPath, { recursive: true, force: false });
    try {
      mkdirSync(lockPath);
    } catch (retryError) {
      if (retryError.code === "EEXIST") {
        const retryOwner = readLockOwner(lockPath);
        if (retryOwner) throw lockError(lockPath, retryOwner);
      }
      throw retryError;
    }
  }
  try {
    writeFileSync(resolve(lockPath, "owner.json"), `${JSON.stringify(owner)}\n`, { mode: 0o600 });
  } catch (error) {
    rmSync(lockPath, { recursive: true, force: true });
    throw error;
  }
  console.log(`test-artifacts: acquired shared artifact lock (pid ${owner.pid})`);
  let released = false;
  return {
    release() {
      if (released) return;
      released = true;
      const current = readLockOwner(lockPath);
      if (current?.token !== owner.token)
        throw new Error(`test-artifacts: lock ownership changed before release at ${lockPath}`);
      rmSync(lockPath, { recursive: true, force: false });
      console.log("test-artifacts: released shared artifact lock");
    },
  };
}

export async function withArtifactBuildLock(run, lockPath = artifactLockPath()) {
  const lock = acquireArtifactBuildLock(lockPath);
  let signal;
  const releaseForSignal = (received) => {
    signal = received;
    try {
      lock.release();
    } finally {
      // Let command()'s handler stop child process groups first, then restore
      // normal signal semantics after the lock has been made available.
      setImmediate(() => process.kill(process.pid, received));
    }
  };
  const onSigint = () => releaseForSignal("SIGINT");
  const onSigterm = () => releaseForSignal("SIGTERM");
  process.once("SIGINT", onSigint);
  process.once("SIGTERM", onSigterm);
  try {
    return await run();
  } finally {
    process.removeListener("SIGINT", onSigint);
    process.removeListener("SIGTERM", onSigterm);
    if (!signal) lock.release();
  }
}

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

  // Keep every Cargo invocation in the default target directory restored by
  // Swatinem/rust-cache. On the 4-vCPU CI runner, separate target directories
  // discarded that cache and made three cold compilers contend for the same
  // CPUs. NAPI is the long pole and benefits most from running alone. Once it
  // is complete, CLI and fast WASM can share the remaining compile window;
  // jazz-tools then consumes both generated prerequisites.
  await guardedRun("pnpm", ["--filter", "jazz-napi", "build"], "release NAPI");
  const cli = guardedRun("pnpm", ["--filter", "@jazz/rust", "build:crates"], "CLI");
  const wasm = guardedRun("pnpm", ["--filter", "jazz-wasm", "build:fast"], "fast WASM");
  try {
    await Promise.all([cli, wasm]);
  } catch (error) {
    throw firstBuildError ?? error;
  }
  await guardedRun("pnpm", ["--filter", "jazz-tools", "build"], "jazz-tools");

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
    await repairRun("pnpm", ["--filter", "jazz-napi", "build"], "repair release NAPI");
    await repairRun("node", ["-e", "require('./crates/jazz-napi')"], "load repaired release NAPI");
  }
  await run(
    "node",
    ["dev/artifacts/provenance.mjs", "verify", "napi", "release"],
    "verify release NAPI provenance",
  );
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  withArtifactBuildLock(buildTestArtifacts).catch((error) => {
    console.error(`test-artifacts: ${error.message}`);
    process.exitCode = 1;
  });
}
