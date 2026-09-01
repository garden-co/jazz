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
import { linkSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { basename, isAbsolute, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { fileURLToPath } from "node:url";
import { snapshotCorrectnessArtifacts } from "../artifacts/test-artifact-store.mjs";
import {
  correctnessArtifactSourceIdentity,
  verifyCorrectnessArtifactProducer,
  writeCorrectnessArtifactProducerManifest,
} from "../artifacts/correctness-artifact-producer.mjs";

const root = resolve(fileURLToPath(new URL("../..", import.meta.url)));

function worktreeGitDirectory(cwd = root) {
  try {
    const directory = execFileSync("git", ["rev-parse", "--git-dir"], {
      cwd,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    }).trim();
    return isAbsolute(directory) ? directory : resolve(cwd, directory);
  } catch (error) {
    throw lockFilesystemError("find shared Git directory", error);
  }
}

function lockFilesystemError(operation, error) {
  const code = typeof error?.code === "string" ? error.code : "unknown error";
  return new Error(`test-artifacts: ${operation} failed (${code}).`);
}

function removeQuietly(path) {
  try {
    rmSync(path, { force: true });
  } catch {
    // A cleanup failure must not replace the primary, already-redacted error.
  }
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
    throw lockFilesystemError("read process identity", error);
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
 * Generated bindings and Cargo targets are per worktree. Keep the lock beside
 * that worktree's Git metadata: independent lanes may build in parallel while
 * children of one aggregate build still inherit one verified lease. Cargo and
 * pnpm protect their genuinely shared caches themselves. A test/CI hook can
 * still select one explicit parent lock for its children.
 */
export function artifactLockPath(cwd = root) {
  return (
    process.env.JAZZ_TEST_ARTIFACT_LOCK_PATH ??
    resolve(worktreeGitDirectory(cwd), "jazz-test-artifacts.lock")
  );
}

function readLockOwner(lockPath) {
  try {
    return JSON.parse(readFileSync(lockPath, "utf8"));
  } catch (error) {
    if (error.code === "ENOENT") return undefined;
    throw lockFilesystemError("read lock receipt", error);
  }
}

function lockError(lockPath, owner) {
  const started = typeof owner.startedAt === "string" ? owner.startedAt : "unknown";
  const cwd = typeof owner.cwd === "string" ? basename(owner.cwd) : "unknown";
  const state = ownerIsAlive(owner) ? "active" : "stale";
  const action =
    state === "active"
      ? "Wait for it to finish."
      : "Run `pnpm artifacts:unlock` to verify and clear it.";
  return new Error(
    `test-artifacts: ${state} artifact lock (pid ${owner.pid}, cwd ${cwd}, started ${started}). ${action}`,
  );
}

/** Acquire an exclusive, worktree-scoped artifact build lock. */
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
    removeQuietly(staging);
    throw lockFilesystemError("create lock receipt", error);
  }
  try {
    // Hard-linking the fully-written receipt is atomic and unlike rename
    // never replaces an existing live or malformed lock on POSIX/Windows.
    //
    // A previous owner can unlink its receipt after `linkSync` reports
    // EEXIST but before we read it. That is not an unowned receipt: it is a
    // completed release, so retry the same atomic publish. In particular,
    // direct WASM producers may hand the selected CI lock from one producer
    // to the next without an intermediate coordinator. Any receipt we can
    // still read remains fail-closed below.
    for (;;) {
      try {
        linkSync(staging, lockPath);
        break;
      } catch (error) {
        if (error.code !== "EEXIST" && error.code !== "ENOTEMPTY") throw error;
        const existing = readLockOwner(lockPath);
        if (existing) {
          if (!Number.isInteger(existing.pid) || existing.pid <= 0)
            throw new Error(
              "test-artifacts: lock has no usable owner metadata; refusing to delete it.",
            );
          throw lockError(lockPath, existing);
        }
        // The collision vanished before it could be inspected. Retry the
        // atomic link rather than treating that absence as permission to
        // remove or overwrite anything.
      }
    }
    try {
      rmSync(staging, { force: true });
    } catch (error) {
      throw lockFilesystemError("remove published lock receipt", error);
    }
  } catch (error) {
    removeQuietly(staging);
    if (error.message?.startsWith("test-artifacts:")) throw error;
    throw lockFilesystemError("publish lock receipt", error);
  }
  console.log(`test-artifacts: acquired artifact lock (pid ${owner.pid})`);
  let released = false;
  return {
    lockPath,
    token: owner.token,
    release() {
      if (released) return;
      released = true;
      const current = readLockOwner(lockPath);
      if (current?.token !== owner.token)
        throw new Error("test-artifacts: lock ownership changed before release.");
      try {
        rmSync(lockPath, { force: false });
      } catch (error) {
        throw lockFilesystemError("release lock", error);
      }
      console.log("test-artifacts: released artifact lock");
    },
  };
}

/**
 * Environment inherited by an artifact producer owned by this lock.
 *
 * `JAZZ_TEST_ARTIFACT_LOCK_PATH` is deliberately the selected-lock input, not
 * merely a test convenience: the aggregate CI parent selects its runner-temp
 * lock before it spawns Turbo.  Turbo children must receive that same input so
 * `verifyArtifactBuildLease` can prove that a claimed lease belongs to the
 * parent-selected lock, rather than treating a child-provided lock path as
 * authority.
 */
export function artifactBuildLease(lock) {
  return {
    JAZZ_ARTIFACT_BUILD_LEASE: lock.token,
    JAZZ_ARTIFACT_BUILD_LOCK_PATH: lock.lockPath,
    JAZZ_TEST_ARTIFACT_LOCK_PATH: lock.lockPath,
  };
}

/** Reject forged/nested leases instead of silently racing the aggregate builder. */
export function verifyArtifactBuildLease({ token, lockPath }) {
  const expectedPath = artifactLockPath(root);
  if (!lockPath || resolve(lockPath) !== resolve(expectedPath))
    throw new Error("test-artifacts: inherited artifact lease is for a different clone lock.");
  const owner = readLockOwner(lockPath);
  if (!owner || owner.token !== token || !ownerIsAlive(owner))
    throw new Error("test-artifacts: inherited artifact lease is missing or no longer owned.");
  return { token, lockPath };
}

export function unlockArtifactBuildLock(lockPath = artifactLockPath()) {
  const guard = `${lockPath}.unlocking`;
  const receipt = `${guard}-${process.pid}-${Date.now()}`;
  try {
    writeFileSync(receipt, `${process.pid}\n`, { flag: "wx", mode: 0o600 });
    linkSync(receipt, guard);
  } catch (error) {
    removeQuietly(receipt);
    if (error.code === "EEXIST" || error.code === "ENOTEMPTY")
      throw new Error("test-artifacts: another unlock is in progress; retry shortly.");
    throw lockFilesystemError("acquire unlock guard", error);
  }
  try {
    rmSync(receipt, { force: true });
  } catch (error) {
    removeQuietly(guard);
    throw lockFilesystemError("remove unlock receipt", error);
  }
  try {
    const owner = readLockOwner(lockPath);
    if (!owner || !Number.isInteger(owner.pid) || owner.pid <= 0)
      throw new Error("test-artifacts: lock has no usable owner metadata; refusing to delete it.");
    if (ownerIsAlive(owner)) throw lockError(lockPath, owner);
    try {
      rmSync(lockPath, { force: false });
    } catch (error) {
      throw lockFilesystemError("clear stale lock", error);
    }
    console.log("test-artifacts: cleared verified stale artifact lock");
  } finally {
    try {
      rmSync(guard, { force: true });
    } catch (error) {
      throw lockFilesystemError("release unlock guard", error);
    }
  }
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
    return await run(scope, artifactBuildLease(lock));
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

export async function buildTestArtifacts(
  run = command,
  scope = createBuildScope(),
  lease = undefined,
  snapshot = () => {},
  sealProducerManifest = () => {},
) {
  // Capture before any producer starts.  A dirty checkout is acceptable only
  // when it remains byte-for-byte the same through publication; otherwise a
  // manifest could attest new sources while containing binaries built from old
  // ones.
  const sourceAtStart = correctnessArtifactSourceIdentity(root);
  const assertUnchangedSource = () => {
    if (JSON.stringify(sourceAtStart) !== JSON.stringify(correctnessArtifactSourceIdentity(root)))
      throw new Error("test-artifacts: source inputs changed during native artifact production");
  };
  let firstBuildError;
  const guardedRun = (command, args, label, env) =>
    scope
      .track(run(command, args, label, { env: { ...env, ...lease }, signal: scope.signal }))
      .catch((error) => {
        if (!firstBuildError) {
          firstBuildError = error;
          scope.abort(error);
        }
        throw error;
      });

  const preflightNapi = () =>
    scope.track(
      run(
        "node",
        [
          "-e",
          "const {nativeBinding,expectedNativeArtifactFingerprint:expected}=require('./crates/jazz-napi/native-binding.pointer.cjs'); const actual=nativeBinding.nativeArtifactFingerprint?.(); if(actual!==expected) { console.error(`Jazz NAPI artifact ABI mismatch: expected ${expected}, got ${String(actual)}`); process.exit(23); }",
        ],
        "preflight release NAPI",
        { signal: scope.signal },
      ),
    );

  // Keep every Cargo invocation in the default target directory restored by
  // Swatinem/rust-cache. On the 4-vCPU CI runner, separate target directories
  // discarded that cache and made three cold compilers contend for the same
  // CPUs. NAPI is the long pole and benefits most from running alone. Once it
  // is complete, fast WASM uses the remaining compile window; jazz-tools then
  // consumes both runtime prerequisites. CLI builds are separate because no
  // correctness consumer loads the binary at runtime.
  await guardedRun(
    "pnpm",
    ["exec", "turbo", "run", "build", "--filter=jazz-napi", "--only"],
    "release NAPI",
  );
  const wasm = guardedRun(
    "pnpm",
    ["exec", "turbo", "run", "build:fast", "--filter=jazz-wasm", "--only"],
    "fast WASM",
  );
  try {
    await wasm;
  } catch (error) {
    await scope.drain();
    throw firstBuildError ?? error;
  }
  await guardedRun(
    "node",
    ["dev/artifacts/stage-native-fingerprints.mjs", "--local"],
    "derive local artifact expectations",
  );
  try {
    // Validate the mutable producer generation before it can enter the
    // content-addressed correctness store. A bad generation must never poison the
    // fingerprint-addressed destination that its repair needs to publish.
    await preflightNapi();
  } catch (error) {
    console.warn(`test-artifacts: release NAPI failed preflight; repairing (${error.message})`);
    await guardedRun(
      "pnpm",
      ["exec", "turbo", "run", "build", "--filter=jazz-napi", "--only", "--force"],
      "repair release NAPI",
    );
    await guardedRun(
      "node",
      ["dev/artifacts/stage-native-fingerprints.mjs", "--local"],
      "refresh repaired artifact expectations",
    );
    await preflightNapi();
  }
  // Seal the exact pair before the separate TypeScript consumer builds its
  // broker worker. Mutable package publication paths remain useful to package
  // builds, but correctness consumers must never follow a later replacement.
  assertUnchangedSource();
  const correctnessSnapshot = snapshot(root);
  assertUnchangedSource();

  // A manifest is the contract that makes a cached/generated artifact safe to
  // consume. NAPI is built release because that is the loadable Linux mode;
  // the fast profile is intentionally WASM-only and correctness-only.
  await guardedRun(
    "node",
    ["dev/artifacts/provenance.mjs", "verify", "wasm", "fast"],
    "verify fast WASM provenance",
  );
  await guardedRun(
    "node",
    ["dev/artifacts/provenance.mjs", "write", "napi", "release"],
    "seal release NAPI provenance",
  );

  await guardedRun("node", ["-e", "require('./crates/jazz-napi')"], "load release NAPI");
  await guardedRun(
    "node",
    ["dev/artifacts/provenance.mjs", "verify", "napi", "release"],
    "verify release NAPI provenance",
  );
  // This is the producer/consumer boundary.  It is written only after every
  // native artifact has loaded and its provenance has been verified.  The TS
  // consumer gate validates this sealed receipt before and after it builds tools.
  sealProducerManifest(root, correctnessSnapshot, sourceAtStart);
  // The producer itself verifies the exact receipt it just published. This
  // catches a partial/incorrect write here rather than deferring it to a TS
  // consumer job that would otherwise report an unrelated build failure.
  if (correctnessSnapshot) verifyCorrectnessArtifactProducer(root);
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  if (process.argv[2] === "unlock") {
    try {
      unlockArtifactBuildLock();
    } catch (error) {
      console.error(`test-artifacts: ${error.message}`);
      process.exitCode = 1;
    }
  } else if (process.argv[2]) {
    console.error("test-artifacts: expected no argument or `unlock`");
    process.exitCode = 1;
  } else
    withArtifactBuildLock((scope, lease) =>
      buildTestArtifacts(
        command,
        scope,
        lease,
        snapshotCorrectnessArtifacts,
        writeCorrectnessArtifactProducerManifest,
      ),
    ).catch((error) => {
      console.error(`test-artifacts: ${error.message}`);
      process.exitCode = 1;
    });
}
