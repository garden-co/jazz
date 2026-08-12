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
import { linkSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { basename, isAbsolute, resolve } from "node:path";
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

function processStartIdentity(pid = process.pid) {
  // Linux supplies a monotonic process-start tick, making PID reuse
  // distinguishable. Other platforms deliberately fall back to conservative
  // live-PID handling rather than guessing from locale-specific tooling.
  if (process.platform !== "linux") return undefined;
  try {
    return readFileSync(`/proc/${pid}/stat`, "utf8").trim().split(" ")[21];
  } catch (error) {
    if (error.code === "ENOENT") return undefined;
    throw error;
  }
}

function ownerIsAlive(owner) {
  try {
    process.kill(owner.pid, 0);
    if (owner.processStartIdentity) {
      const actual = processStartIdentity(owner.pid);
      if (actual && actual !== owner.processStartIdentity) return false;
    }
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
 * Put the lock there rather than in a checkout: linked worktrees share this
 * clone's default Cargo target and generated package outputs. Separate clones
 * do not share those resources and intentionally do not contend. A test hook
 * overrides the exact path without exposing a real checkout in receipts.
 */
export function artifactLockPath(cwd = root) {
  return (
    process.env.JAZZ_TEST_ARTIFACT_LOCK_PATH ??
    resolve(sharedGitDirectory(cwd), "jazz-test-artifacts.lock")
  );
}

function readLockOwner(lockPath) {
  try {
    return JSON.parse(readFileSync(lockPath, "utf8"));
  } catch (error) {
    if (error.code === "ENOENT") return undefined;
    throw new Error(`test-artifacts: lock has unreadable owner metadata: ${error.message}`);
  }
}

function lockError(lockPath, owner) {
  const started = typeof owner.startedAt === "string" ? owner.startedAt : "unknown";
  const cwd = typeof owner.cwd === "string" ? basename(owner.cwd) : "unknown";
  return new Error(
    `test-artifacts: another artifact build is active (pid ${owner.pid}, cwd ${cwd}, started ${started}). ` +
      "Wait for it to finish, or if it is no longer running retry to recover its stale lock.",
  );
}

/** Acquire an exclusive, clone-wide artifact build lock. */
export function acquireArtifactBuildLock(lockPath = artifactLockPath()) {
  const owner = {
    pid: process.pid,
    cwd: process.cwd(),
    startedAt: new Date().toISOString(),
    processStartIdentity: processStartIdentity(),
    token: `${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2)}`,
  };
  const staging = `${lockPath}.acquiring-${owner.token}`;
  try {
    writeFileSync(staging, `${JSON.stringify(owner)}\n`, { mode: 0o600, flag: "wx" });
  } catch (error) {
    rmSync(staging, { force: true });
    throw error;
  }
  for (;;) {
    try {
      // Hard-linking the fully-written receipt is atomic and unlike rename
      // never replaces an existing live or malformed lock on POSIX/Windows.
      linkSync(staging, lockPath);
      rmSync(staging, { force: true });
      break;
    } catch (error) {
      if (error.code !== "EEXIST" && error.code !== "ENOTEMPTY") {
        rmSync(staging, { force: true });
        throw error;
      }
    }
    const existing = readLockOwner(lockPath);
    if (!existing || !Number.isInteger(existing.pid) || existing.pid <= 0) {
      rmSync(staging, { force: true });
      throw new Error("test-artifacts: lock has no usable owner metadata; refusing to delete it.");
    }
    if (ownerIsAlive(existing)) {
      rmSync(staging, { force: true });
      throw lockError(lockPath, existing);
    }
    // rename is atomic on all supported local filesystems. Exactly one stale
    // recovery contender can quarantine the observed directory; it never
    // recursively removes the canonical name after observing it.
    const quarantine = `${lockPath}.stale-${owner.token}`;
    try {
      renameSync(lockPath, quarantine);
    } catch (renameError) {
      if (renameError.code === "ENOENT" || renameError.code === "EEXIST") continue;
      rmSync(staging, { force: true });
      throw renameError;
    }
    try {
      const quarantined = readLockOwner(quarantine);
      if (quarantined?.token !== existing.token || ownerIsAlive(quarantined))
        throw new Error(
          "test-artifacts: stale lock changed while quarantining it; refusing to remove it.",
        );
      rmSync(quarantine, { force: false });
    } catch (quarantineError) {
      // Preserve an ambiguous quarantine for inspection. Its distinct name
      // cannot block a fresh canonical acquisition.
      rmSync(staging, { force: true });
      throw quarantineError;
    }
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
      rmSync(lockPath, { force: false });
      console.log("test-artifacts: released shared artifact lock");
    },
  };
}

export async function withArtifactBuildLock(run, lockPath = artifactLockPath()) {
  const lock = acquireArtifactBuildLock(lockPath);
  const scope = createBuildScope();
  let receivedSignal;
  let signalShutdown;
  const releaseForSignal = (received) => {
    if (signalShutdown) return;
    receivedSignal = received;
    signalShutdown = (async () => {
      scope.abort(new Error(`received ${received}`));
      await scope.drain();
      lock.release();
      process.kill(process.pid, received);
    })().catch((error) => {
      console.error(`test-artifacts: failed to shut down cleanly: ${error.message}`);
      process.exitCode = 1;
    });
  };
  const onSigint = () => releaseForSignal("SIGINT");
  const onSigterm = () => releaseForSignal("SIGTERM");
  process.once("SIGINT", onSigint);
  process.once("SIGTERM", onSigterm);
  try {
    return await run(scope);
  } finally {
    process.removeListener("SIGINT", onSigint);
    process.removeListener("SIGTERM", onSigterm);
    if (receivedSignal) await signalShutdown;
    else {
      await scope.drain();
      lock.release();
    }
  }
}

export function createBuildScope() {
  const controller = new AbortController();
  const active = new Set();
  return {
    signal: controller.signal,
    abort(reason) {
      if (!controller.signal.aborted) controller.abort(reason);
    },
    track(promise) {
      active.add(promise);
      promise.finally(() => active.delete(promise)).catch(() => {});
      return promise;
    },
    async drain() {
      while (active.size) await Promise.allSettled([...active]);
    },
  };
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

export async function buildTestArtifacts(run = command, scope = createBuildScope()) {
  let firstBuildError;
  const guardedRun = (command, args, label, env) =>
    scope.track(run(command, args, label, { env, signal: scope.signal })).catch((error) => {
      if (!firstBuildError) {
        firstBuildError = error;
        scope.abort(error);
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
    await scope.drain();
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
    scope.abort(error);
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
  withArtifactBuildLock((scope) => buildTestArtifacts(command, scope)).catch((error) => {
    console.error(`test-artifacts: ${error.message}`);
    process.exitCode = 1;
  });
}
