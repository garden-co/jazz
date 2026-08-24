#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, lstatSync, mkdtempSync, readFileSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { basename, join, resolve } from "node:path";
import { verifyManifest, writeManifest } from "./provenance.mjs";
import { acquireArtifactBuildLock, artifactLockPath, verifyArtifactBuildLease } from "../gates/build-test-artifacts.mjs";

const root = resolve(fileURLToPath(new URL(".", import.meta.url)), "../..");
const commands = {
  wasm: {
    fast: ["wasm-pack", ["build", "crates/jazz-wasm", "--target", "web", "--dev"]],
    release: ["wasm-pack", ["build", "crates/jazz-wasm", "--target", "web", "--release"]],
    profiling: ["wasm-pack", ["build", "crates/jazz-wasm", "--target", "web", "--profiling"]],
  },
  napi: {
    debug: ["pnpm", ["--dir", "crates/jazz-napi", "exec", "napi", "build", "--platform"]],
    release: ["pnpm", ["--dir", "crates/jazz-napi", "exec", "napi", "build", "--platform", "--release"]],
    perf: ["pnpm", ["--dir", "crates/jazz-napi", "exec", "napi", "build", "--platform", "--profile", "perf"]],
  },
};
export const wasmPackageFiles = ["jazz_wasm_bg.wasm", "jazz_wasm_bg.wasm.d.ts", "jazz_wasm.d.ts", "jazz_wasm.js", "package.json", ".jazz-artifact-manifest.json"];
const hash = (path) => createHash("sha256").update(readFileSync(path)).digest("hex");
const journalPath = (dir) => join(dir, ".pkg-transaction.json");

export function createWasmPackageStage(rootDir = root, profile = "release") {
  const packageDir = join(rootDir, "crates", "jazz-wasm");
  const path = mkdtempSync(join(packageDir, `.pkg-stage-${profile}-`));
  return { path, outDir: basename(path) };
}
export function assertCompleteWasmPackage(path) {
  const invalid = wasmPackageFiles.filter((file) => {
    const candidate = join(path, file);
    return !existsSync(candidate) || !lstatSync(candidate).isFile() || lstatSync(candidate).size === 0;
  });
  if (invalid.length) throw new Error(`WASM build produced an incomplete staged package (${basename(path)}; invalid ${invalid.join(", ")})`);
  return Object.fromEntries(wasmPackageFiles.map((file) => [file, hash(join(path, file))]));
}
function readJournal(path) {
  if (!existsSync(path)) return undefined;
  try { return JSON.parse(readFileSync(path, "utf8")); }
  catch (error) { throw new Error(`WASM package transaction journal is unreadable: ${error.message}`); }
}
function writeJournal(path, value) {
  const temporary = `${path}.${process.pid}.tmp`;
  writeFileSync(temporary, `${JSON.stringify(value)}\n`, { mode: 0o600 });
  renameSync(temporary, path);
}
function matches(path, hashes) {
  try { const actual = assertCompleteWasmPackage(path); return Object.entries(hashes).every(([file, value]) => actual[file] === value); }
  catch { return false; }
}
function acquireWasmLease() {
  const token = process.env.JAZZ_ARTIFACT_BUILD_LEASE;
  if (token) { verifyArtifactBuildLease({ token, lockPath: process.env.JAZZ_ARTIFACT_BUILD_LOCK_PATH }); return undefined; }
  // Direct producers are command-line entrypoints; a short synchronous wait is
  // preferable to failing a valid concurrent `pnpm build:core` invocation.
  const deadline = Date.now() + 60_000;
  for (;;) {
    try { return acquireArtifactBuildLock(artifactLockPath(root)); }
    catch (error) {
      if (!String(error.message).includes("active artifact lock") || Date.now() >= deadline) throw error;
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 25);
    }
  }
}
/** Recover an interrupted old→new directory swap. Must run while holding the clone lock. */
export function recoverWasmPackageTransaction(packageDir) {
  const journal = readJournal(journalPath(packageDir));
  if (!journal) return;
  const pkg = join(packageDir, "pkg");
  const stage = join(packageDir, journal.stage);
  const backup = journal.backup && join(packageDir, journal.backup);
  try {
    if (existsSync(pkg) && matches(pkg, journal.hashes)) { if (backup && existsSync(backup)) rmSync(backup, { recursive: true, force: true }); }
    else if (backup && existsSync(backup)) { if (existsSync(pkg)) rmSync(pkg, { recursive: true, force: true }); renameSync(backup, pkg); }
    else if (journal.hadCurrent) throw new Error("old package is absent and no recoverable backup remains");
    if (existsSync(stage)) rmSync(stage, { recursive: true, force: true });
    rmSync(journalPath(packageDir), { force: true });
  } catch (error) { throw new Error(`WASM package transaction recovery failed: ${error.message}`); }
}
/** Publish one complete generation; readers see old, missing, or new, never mixed files. */
export function publishWasmPackage(stagePath, packagePath, { profile = "release", alreadyLocked = false } = {}) {
  const packageDir = resolve(packagePath, "..");
  const lock = alreadyLocked ? undefined : acquireWasmLease();
  try {
    recoverWasmPackageTransaction(packageDir);
    const hashes = assertCompleteWasmPackage(stagePath);
    const backupPath = join(packageDir, `.pkg-backup-${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2)}`);
    const transaction = { schema: 1, profile, stage: basename(stagePath), backup: basename(backupPath), hadCurrent: existsSync(packagePath), hashes, state: "prepared" };
    writeJournal(journalPath(packageDir), transaction);
    if (transaction.hadCurrent) renameSync(packagePath, backupPath);
    transaction.state = "old-moved"; writeJournal(journalPath(packageDir), transaction);
    if (process.env.JAZZ_WASM_BUILD_FAULT === "after-old-moved") process.kill(process.pid, "SIGKILL");
    renameSync(stagePath, packagePath);
    transaction.state = "new-published"; writeJournal(journalPath(packageDir), transaction);
    if (!matches(packagePath, hashes)) throw new Error("published package does not match staged hashes");
    if (existsSync(backupPath)) rmSync(backupPath, { recursive: true, force: true });
    rmSync(journalPath(packageDir), { force: true });
  } catch (error) {
    recoverWasmPackageTransaction(packageDir);
    throw new Error(`WASM package publish transaction failed: ${error.message}`);
  } finally {
    if (existsSync(stagePath)) rmSync(stagePath, { recursive: true, force: true });
    lock?.release();
  }
}
export function writeWasmStageManifest(stagePath, profile) {
  if (process.env.JAZZ_WASM_BUILD_FAULT === "manifest-write")
    throw new Error("planted staged manifest write failure");
  writeManifest(root, "wasm", profile, undefined, { wasmPackageDir: stagePath });
}
export function buildArtifact(kind, profile = "release", extraArgs = []) {
  const selected = commands[kind]?.[profile];
  if (!selected) throw new Error("usage: build.mjs <wasm fast|release|profiling | napi debug|release>");
  if (kind !== "napi" && extraArgs.length) throw new Error("only napi builds accept extra napi CLI arguments");
  const [command, selectedArgs] = selected;
  const wasmLock = kind === "wasm" ? acquireWasmLease() : undefined;
  const wasmStage = kind === "wasm" ? createWasmPackageStage(root, profile) : undefined;
  const args = [...selectedArgs, ...extraArgs, ...(wasmStage ? ["--out-dir", wasmStage.outDir] : [])];
  try {
    const result = spawnSync(command, args, { cwd: root, stdio: "inherit", shell: process.platform === "win32" });
    if (result.error) throw result.error;
    if (result.status !== 0) { if (wasmStage) throw new Error(`WASM ${profile} build failed before publishing staged package ${basename(wasmStage.path)}; the prior package remains intact`); process.exitCode = result.status ?? 1; return; }
    if (wasmStage) {
      writeWasmStageManifest(wasmStage.path, profile);
      publishWasmPackage(wasmStage.path, join(root, "crates", "jazz-wasm", "pkg"), { profile, alreadyLocked: true });
      const problem = verifyManifest(root, kind, profile);
      if (problem) throw new Error(`published WASM manifest verification failed: ${problem}`);
    } else writeManifest(root, kind, profile);
  } finally { if (wasmStage && existsSync(wasmStage.path)) rmSync(wasmStage.path, { recursive: true, force: true }); wasmLock?.release(); }
}
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const [kind, profile = "release", ...extraArgs] = process.argv.slice(2);
  try { buildArtifact(kind, profile, extraArgs); } catch (error) { console.error(`artifact build: ${error.message}`); process.exitCode = 1; }
}
