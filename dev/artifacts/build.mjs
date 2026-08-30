#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  existsSync,
  lstatSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  renameSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { fileURLToPath } from "node:url";
import { basename, join, relative, resolve } from "node:path";
import { nativeArtifactFingerprint, verifyManifest, writeManifest } from "./provenance.mjs";
import {
  acquireArtifactBuildLock,
  artifactLockPath,
  verifyArtifactBuildLease,
} from "../gates/build-test-artifacts.mjs";

const root = resolve(fileURLToPath(new URL(".", import.meta.url)), "../..");
const commands = {
  wasm: {
    fast: ["wasm-pack", ["build", "crates/jazz-wasm", "--target", "web", "--dev"]],
    release: ["wasm-pack", ["build", "crates/jazz-wasm", "--target", "web", "--release"]],
    profiling: ["wasm-pack", ["build", "crates/jazz-wasm", "--target", "web", "--profiling"]],
  },
  napi: {
    debug: ["pnpm", ["--dir", "crates/jazz-napi", "exec", "napi", "build", "--platform"]],
    release: [
      "pnpm",
      ["--dir", "crates/jazz-napi", "exec", "napi", "build", "--platform", "--release"],
    ],
    perf: [
      "pnpm",
      ["--dir", "crates/jazz-napi", "exec", "napi", "build", "--platform", "--profile", "perf"],
    ],
  },
};
const napiBindingForTarget = {
  "x86_64-unknown-linux-gnu": "jazz-napi.linux-x64-gnu.node",
  "x86_64-pc-windows-msvc": "jazz-napi.win32-x64-msvc.node",
  "x86_64-apple-darwin": "jazz-napi.darwin-x64.node",
  "aarch64-apple-darwin": "jazz-napi.darwin-arm64.node",
};
const hostTarget = {
  "linux-x64": "x86_64-unknown-linux-gnu",
  "win32-x64": "x86_64-pc-windows-msvc",
  "darwin-x64": "x86_64-apple-darwin",
  "darwin-arm64": "aarch64-apple-darwin",
}[`${process.platform}-${process.arch}`];
export const wasmPackageFiles = [
  "jazz_wasm_bg.wasm",
  "jazz_wasm_bg.wasm.d.ts",
  "jazz_wasm.d.ts",
  "jazz_wasm.js",
  "package.json",
  ".jazz-artifact-manifest.json",
];
const hash = (path) => createHash("sha256").update(readFileSync(path)).digest("hex");
const journalPath = (dir) => join(dir, ".pkg-transaction.json");

export function createWasmPackageStage(rootDir = root, profile = "release") {
  const packageDir = join(rootDir, "crates", "jazz-wasm");
  const path = mkdtempSync(join(packageDir, `.pkg-stage-${profile}-`));
  return { path, outDir: basename(path) };
}
export function assertCompleteWasmPackage(path) {
  if (!existsSync(path) || !lstatSync(path).isDirectory() || lstatSync(path).isSymbolicLink())
    throw new Error(`WASM build package directory is not a real directory (${basename(path)})`);
  const invalid = wasmPackageFiles.filter((file) => {
    const candidate = join(path, file);
    return (
      !existsSync(candidate) || !lstatSync(candidate).isFile() || lstatSync(candidate).size === 0
    );
  });
  if (invalid.length)
    throw new Error(
      `WASM build produced an incomplete staged package (${basename(path)}; invalid ${invalid.join(", ")})`,
    );
  return Object.fromEntries(wasmPackageFiles.map((file) => [file, hash(join(path, file))]));
}
function readJournal(path) {
  if (!existsSync(path)) return undefined;
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch (error) {
    throw new Error(`WASM package transaction journal is unreadable: ${error.message}`);
  }
}
function writeJournal(path, value) {
  const temporary = `${path}.${process.pid}.tmp`;
  writeFileSync(temporary, `${JSON.stringify(value)}\n`, { mode: 0o600 });
  renameSync(temporary, path);
}
function matches(path, hashes) {
  try {
    const actual = assertCompleteWasmPackage(path);
    return Object.entries(hashes).every(([file, value]) => actual[file] === value);
  } catch {
    return false;
  }
}
function inheritedArtifactLease() {
  const token = process.env.JAZZ_ARTIFACT_BUILD_LEASE;
  const lockPath = process.env.JAZZ_ARTIFACT_BUILD_LOCK_PATH;
  if (!token && !lockPath) return undefined;
  return verifyArtifactBuildLease({ token, lockPath });
}
function acquireArtifactLease() {
  const inherited = inheritedArtifactLease();
  if (inherited) return { lease: inherited };
  // Direct producers are command-line entrypoints; a short synchronous wait is
  // preferable to failing a valid concurrent `pnpm build:core` invocation.
  const deadline = Date.now() + 60_000;
  for (;;) {
    try {
      const lock = acquireArtifactBuildLock(artifactLockPath(root));
      return {
        lease: { token: lock.token, lockPath: lock.lockPath },
        release: () => lock.release(),
      };
    } catch (error) {
      if (!String(error.message).includes("active artifact lock") || Date.now() >= deadline)
        throw error;
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 25);
    }
  }
}
function journalChild(packageDir, name, prefix) {
  if (
    typeof name !== "string" ||
    name.includes("/") ||
    name.includes("\\") ||
    !name.startsWith(prefix)
  )
    throw new Error(`WASM package transaction journal has unsafe ${prefix} path`);
  const path = resolve(packageDir, name);
  if (resolve(path, "..") !== resolve(packageDir))
    throw new Error(`WASM package transaction journal ${prefix} is outside its package directory`);
  return path;
}
function realDirectory(path, label, { required = false } = {}) {
  if (!existsSync(path)) {
    if (required) throw new Error(`WASM package transaction ${label} is missing`);
    return false;
  }
  const stat = lstatSync(path);
  if (!stat.isDirectory() || stat.isSymbolicLink())
    throw new Error(`WASM package transaction ${label} must be a real directory`);
  return true;
}
function validateJournal(journal) {
  if (
    !journal ||
    journal.schema !== 1 ||
    typeof journal.hadCurrent !== "boolean" ||
    !["prepared", "old-moved", "new-published"].includes(journal.state)
  )
    throw new Error("WASM package transaction journal has an invalid shape");
  if (
    !journal.hashes ||
    typeof journal.hashes !== "object" ||
    Object.keys(journal.hashes).length !== wasmPackageFiles.length ||
    wasmPackageFiles.some((file) => !/^[a-f0-9]{64}$/.test(journal.hashes[file] ?? ""))
  )
    throw new Error("WASM package transaction journal has invalid generation hashes");
}
/** Recover an interrupted old→new directory swap. Must run while holding the clone lock. */
export function recoverWasmPackageTransaction(packageDir) {
  const journal = readJournal(journalPath(packageDir));
  if (!journal) return;
  validateJournal(journal);
  const pkg = join(packageDir, "pkg");
  const stage = journalChild(packageDir, journal.stage, ".pkg-stage-");
  const backup = journal.backup
    ? journalChild(packageDir, journal.backup, ".pkg-backup-")
    : undefined;
  try {
    realDirectory(pkg, "pkg");
    realDirectory(stage, "stage");
    if (backup) realDirectory(backup, "backup");
    if (existsSync(pkg) && matches(pkg, journal.hashes)) {
      if (backup && existsSync(backup)) rmSync(backup, { recursive: true, force: true });
    }
    // `prepared` is journaled before the first rename. A crash here leaves the
    // complete prior pkg in place and no backup; do not mistake it for loss.
    else if (
      journal.state === "prepared" &&
      journal.hadCurrent &&
      existsSync(pkg) &&
      (!backup || !existsSync(backup))
    ) {
      // Retain old pkg; only the uncommitted stage/journal are discarded below.
    } else if (backup && existsSync(backup)) {
      if (existsSync(pkg)) rmSync(pkg, { recursive: true, force: true });
      renameSync(backup, pkg);
    } else if (journal.hadCurrent)
      throw new Error("old package is absent and no recoverable backup remains");
    if (existsSync(stage)) rmSync(stage, { recursive: true, force: true });
    rmSync(journalPath(packageDir), { force: true });
  } catch (error) {
    throw new Error(`WASM package transaction recovery failed: ${error.message}`);
  }
}
/** Publish one complete generation; readers see old, missing, or new, never mixed files. */
export function publishWasmPackage(
  stagePath,
  packagePath,
  { profile = "release", lease = undefined } = {},
) {
  const packageDir = resolve(packagePath, "..");
  const held = lease ? { lease: verifyArtifactBuildLease(lease) } : acquireArtifactLease();
  try {
    recoverWasmPackageTransaction(packageDir);
    realDirectory(stagePath, "stage", { required: true });
    realDirectory(packagePath, "pkg");
    const hashes = assertCompleteWasmPackage(stagePath);
    const backupPath = join(
      packageDir,
      `.pkg-backup-${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2)}`,
    );
    const transaction = {
      schema: 1,
      profile,
      stage: basename(stagePath),
      backup: basename(backupPath),
      hadCurrent: existsSync(packagePath),
      hashes,
      state: "prepared",
    };
    writeJournal(journalPath(packageDir), transaction);
    if (transaction.hadCurrent) renameSync(packagePath, backupPath);
    transaction.state = "old-moved";
    writeJournal(journalPath(packageDir), transaction);
    if (process.env.JAZZ_WASM_BUILD_FAULT === "after-old-moved")
      process.kill(process.pid, "SIGKILL");
    renameSync(stagePath, packagePath);
    transaction.state = "new-published";
    writeJournal(journalPath(packageDir), transaction);
    if (!matches(packagePath, hashes))
      throw new Error("published package does not match staged hashes");
    if (existsSync(backupPath)) rmSync(backupPath, { recursive: true, force: true });
    rmSync(journalPath(packageDir), { force: true });
  } catch (error) {
    recoverWasmPackageTransaction(packageDir);
    throw new Error(`WASM package publish transaction failed: ${error.message}`);
  } finally {
    if (existsSync(stagePath)) rmSync(stagePath, { recursive: true, force: true });
    held.release?.();
  }
}
export function writeWasmStageManifest(stagePath, profile) {
  if (process.env.JAZZ_WASM_BUILD_FAULT === "manifest-write")
    throw new Error("planted staged manifest write failure");
  writeManifest(root, "wasm", profile, undefined, { wasmPackageDir: stagePath });
}
function atomicWrite(path, contents) {
  const temporary = `${path}.${process.pid}.${Date.now()}.${Math.random().toString(36).slice(2)}.tmp`;
  writeFileSync(temporary, contents, { mode: 0o600 });
  renameSync(temporary, path);
}
function realRegularFile(path, label) {
  if (!existsSync(path)) throw new Error(`NAPI generation is missing ${label}`);
  const stat = lstatSync(path);
  if (!stat.isFile() || stat.isSymbolicLink())
    throw new Error(`NAPI generation ${label} must be a real regular file`);
}
function assertRealNapiGeneration(path, expectedBinding, { sealed = true } = {}) {
  if (!existsSync(path) || !lstatSync(path).isDirectory() || lstatSync(path).isSymbolicLink())
    throw new Error("NAPI generation stage is not a real directory");
  const required = [
    [join(path, expectedBinding), expectedBinding],
    [join(path, "index.js"), "generated loader"],
    [join(path, "index.d.ts"), "generated declarations"],
  ];
  if (sealed) required.push([join(path, ".jazz-artifact-manifest.json"), "sealed manifest"]);
  for (const [candidate, label] of required) realRegularFile(candidate, label);
}
function displayArtifactPath(path) {
  return relative(root, path).replaceAll("\\\\", "/");
}
function declarationMismatchDiagnostic(stablePath, generatedPath, stable, generated) {
  const stableLines = stable.split("\n");
  const generatedLines = generated.split("\n");
  const firstDifference = stableLines.findIndex((line, index) => line !== generatedLines[index]);
  const line =
    firstDifference === -1 ? Math.min(stableLines.length, generatedLines.length) : firstDifference;
  const start = Math.max(0, line - 2);
  const end = Math.min(Math.max(stableLines.length, generatedLines.length), line + 3);
  const render = (lines, prefix) =>
    lines
      .slice(start, end)
      .map((value, index) => `${prefix}${String(start + index + 1).padStart(5)} | ${value}`)
      .join("\n");
  return [
    "NAPI build generated declarations that differ from the public package type surface; update the checked-in declaration before activating.",
    `checked-in declarations: ${displayArtifactPath(stablePath)}`,
    `generated declarations: ${displayArtifactPath(generatedPath)}`,
    `first difference near line ${line + 1}:`,
    "--- checked-in",
    render(stableLines, "-"),
    "+++ generated",
    render(generatedLines, "+"),
  ].join("\n");
}
export function publishExpectedFingerprint(
  kind,
  fingerprint,
  packageDir = join(root, "crates", "jazz-napi"),
) {
  // Producer tasks never mutate jazz-tools source; release assembly derives
  // those expectations from sealed downloaded manifests. NAPI's package-local
  // fallback, however, is generated output and is used when no worktree
  // generation pointer exists (for example in an installed platform package).
  // Keep it in step with the binary before that binary can become active.
  if (kind !== "napi") return;
  const path = join(packageDir, "native-artifact-fingerprint.cjs");
  atomicWrite(
    path,
    `// Generated by dev/artifacts/build.mjs.\nmodule.exports = {\n  expectedNativeArtifactFingerprint:\n    ${JSON.stringify(fingerprint)},\n};\n`,
  );
}
function napiGenerationPointer(packageDir, generationPath, fingerprint) {
  const loader = relative(packageDir, join(generationPath, "index.js")).replaceAll("\\", "/");
  if (loader.startsWith("../") || loader === "..")
    throw new Error("staged NAPI generation escaped its package directory");
  return `// Generated by dev/artifacts/build.mjs.\nconst nativeBinding = require(${JSON.stringify(`./${loader}`)});\nmodule.exports = { nativeBinding, expectedNativeArtifactFingerprint: ${JSON.stringify(fingerprint)} };\n`;
}
/** Publish a fully validated NAPI generation by atomically changing one loader pointer. */
export function publishNapiGeneration(
  stagePath,
  packageDir,
  fingerprint,
  { lease = undefined, afterPointerCommit = undefined } = {},
) {
  const held = lease ? { lease: verifyArtifactBuildLease(lease) } : acquireArtifactLease();
  let generationPath;
  let pointerCommitted = false;
  try {
    const bindings = readdirSync(stagePath, { withFileTypes: true })
      .map((entry) => entry.name)
      .filter((name) => name.endsWith(".node"));
    if (bindings.length !== 1)
      throw new Error("NAPI generation must contain exactly one target native binding");
    assertRealNapiGeneration(stagePath, bindings[0]);
    const generationRoot = join(packageDir, ".native-artifacts");
    mkdirSync(generationRoot, { recursive: true });
    generationPath = join(
      generationRoot,
      `generation-${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2)}`,
    );
    renameSync(stagePath, generationPath);
    // The pointer is the sole reader-facing runtime switch. All generated JS,
    // d.ts, native bytes and stage manifest are already present in this directory.
    if (process.env.JAZZ_NAPI_BUILD_FAULT === "pointer-write")
      throw new Error("planted NAPI final-pointer failure");
    atomicWrite(
      join(packageDir, "native-binding.pointer.cjs"),
      napiGenerationPointer(packageDir, generationPath, fingerprint),
    );
    pointerCommitted = true;
    // A fallback marker is consulted only when no generated pointer exists.
    // Commit the pointer first so a SIGKILL can leave either the complete prior
    // state or a complete new pointer state, never a new fallback paired with
    // an old/missing pointer generation.
    if (process.env.JAZZ_NAPI_BUILD_FAULT === "after-pointer-write")
      process.kill(process.pid, "SIGKILL");
    afterPointerCommit?.();
    return generationPath;
  } catch (error) {
    if (!pointerCommitted && generationPath && existsSync(generationPath))
      rmSync(generationPath, { recursive: true, force: true });
    throw error;
  } finally {
    held.release?.();
  }
}
export function validateNapiStage(
  stagePath,
  expectedBinding,
  fingerprint,
  target,
  { stableDeclarationsPath = join(root, "crates/jazz-napi/index.d.ts") } = {},
) {
  const binding = join(stagePath, expectedBinding);
  const loader = join(stagePath, "index.js");
  const declarations = join(stagePath, "index.d.ts");
  assertRealNapiGeneration(stagePath, expectedBinding, { sealed: false });
  const generatedDeclarations = readFileSync(declarations, "utf8");
  const stableDeclarations = readFileSync(stableDeclarationsPath, "utf8");
  if (generatedDeclarations !== stableDeclarations)
    throw new Error(
      declarationMismatchDiagnostic(
        stableDeclarationsPath,
        declarations,
        stableDeclarations,
        generatedDeclarations,
      ),
    );
  if (target !== hostTarget) return;
  const receipt = spawnSync(
    process.execPath,
    [
      "--input-type=module",
      "-e",
      `import { createRequire } from "node:module"; const require=createRequire(import.meta.url); const binding=require(process.argv[1]); const { validateNapiBindingRuntimeSurface }=await import(process.argv[2]); validateNapiBindingRuntimeSurface(binding, process.argv[3]);`,
      loader,
      import.meta.url,
      fingerprint,
    ],
    { stdio: "inherit" },
  );
  if ((receipt.status ?? 1) !== 0)
    throw new Error(
      "NAPI build produced an unloadable or incompatible staged host generation; refusing to publish",
    );
}

/// Add package-wrapper APIs which deliberately do not exist in the raw native
/// declaration surface. The raw `__closePollable` ABI is hidden from generated
/// TypeScript; `index.cjs` owns it and exposes this Promise contract instead.
export function applyNapiPackageDeclarationOverlay(stagePath) {
  const declarationsPath = join(stagePath, "index.d.ts");
  const declarations = readFileSync(declarationsPath, "utf8");
  const lines = declarations.split("\n");
  const classStart = lines.indexOf("export declare class NapiDb {");
  if (classStart === -1) throw new Error("generated NAPI declarations are missing NapiDb");
  const classEnd = lines.findIndex((line, index) => index > classStart && line === "}");
  if (classEnd === -1) throw new Error("generated NAPI declarations have an unterminated NapiDb");
  if (lines.slice(classStart, classEnd).some((line) => /\bclose\s*\(/.test(line)))
    throw new Error("raw NAPI declarations unexpectedly expose close; wrapper overlay refused");
  lines.splice(classEnd, 0, "  close(): Promise<undefined>");
  writeFileSync(declarationsPath, lines.join("\n"));
}

/**
 * Runtime methods used by jazz-tools before a staged NAPI generation can
 * become the active package binding. Keep this separate from declaration
 * parity: generated type declarations can name a method that the binary does
 * not actually export.
 */
export function validateNapiBindingRuntimeSurface(binding, expectedFingerprint) {
  if (
    !binding ||
    typeof binding.nativeArtifactFingerprint !== "function" ||
    binding.nativeArtifactFingerprint() !== expectedFingerprint ||
    typeof binding.NapiDb !== "function" ||
    typeof binding.NapiDb.prototype.tick !== "function" ||
    typeof binding.NapiDb.prototype.wireFeatures !== "function"
  )
    throw new Error(
      "NAPI build is missing a required runtime surface (nativeArtifactFingerprint, NapiDb.tick, or NapiDb.wireFeatures)",
    );
}
export function buildArtifact(kind, profile = "release", extraArgs = []) {
  const selected = commands[kind]?.[profile];
  if (!selected)
    throw new Error("usage: build.mjs <wasm fast|release|profiling | napi debug|release>");
  if (kind !== "napi" && extraArgs.length)
    throw new Error("only napi builds accept extra napi CLI arguments");
  const [command, selectedArgs] = selected;
  const targetIndex = extraArgs.indexOf("--target");
  const target = targetIndex === -1 ? undefined : extraArgs[targetIndex + 1];
  const resolvedNapiTarget = target ?? hostTarget;
  const expectedNapiBinding = kind === "napi" && napiBindingForTarget[resolvedNapiTarget];
  if (kind === "napi" && !expectedNapiBinding)
    throw new Error(`unsupported NAPI target ${resolvedNapiTarget ?? "unknown"}`);
  // No reader-facing generated file is touched before holding the clone-wide
  // lease. The fingerprint is pure source/tool metadata at this point.
  const artifactLock = acquireArtifactLease();
  const fingerprint =
    kind === "napi"
      ? nativeArtifactFingerprint(root, kind, profile, resolvedNapiTarget)
      : nativeArtifactFingerprint(root, kind, profile);
  const wasmStage = kind === "wasm" ? createWasmPackageStage(root, profile) : undefined;
  const napiStage =
    kind === "napi" ? mkdtempSync(join(root, "crates", "jazz-napi", ".napi-stage-")) : undefined;
  const napiPath = expectedNapiBinding && join(napiStage, expectedNapiBinding);
  const args = [
    ...selectedArgs,
    ...extraArgs,
    ...(wasmStage ? ["--out-dir", wasmStage.outDir] : []),
    ...(napiStage ? ["--output-dir", napiStage] : []),
  ];
  try {
    if (kind === "napi" && process.env.JAZZ_NAPI_BUILD_FAULT === "producer")
      throw new Error("planted NAPI producer failure before staging");
    const result = spawnSync(command, args, {
      cwd: root,
      stdio: "inherit",
      shell: process.platform === "win32",
      env: { ...process.env, JAZZ_NATIVE_ARTIFACT_FINGERPRINT: fingerprint },
    });
    if (result.error) throw result.error;
    if (result.status !== 0) {
      if (wasmStage)
        throw new Error(
          `WASM ${profile} build failed before publishing staged package ${basename(wasmStage.path)}; the prior package remains intact`,
        );
      process.exitCode = result.status ?? 1;
      return;
    }
    if (wasmStage) {
      writeWasmStageManifest(wasmStage.path, profile);
      publishWasmPackage(wasmStage.path, join(root, "crates", "jazz-wasm", "pkg"), {
        profile,
        lease: artifactLock.lease,
      });
      publishExpectedFingerprint(kind, fingerprint);
      const problem = verifyManifest(root, kind, profile);
      if (problem) throw new Error(`published WASM manifest verification failed: ${problem}`);
    } else {
      if (process.env.JAZZ_NAPI_BUILD_FAULT === "missing") rmSync(napiPath, { force: true });
      if (process.env.JAZZ_NAPI_BUILD_FAULT === "unloadable")
        writeFileSync(napiPath, "not a native module");
      applyNapiPackageDeclarationOverlay(napiStage);
      validateNapiStage(napiStage, expectedNapiBinding, fingerprint, resolvedNapiTarget);
      // Seal a manifest inside the generation before it can become active.
      if (process.env.JAZZ_NAPI_BUILD_FAULT === "manifest-write")
        throw new Error("planted NAPI staged manifest write failure");
      writeManifest(root, kind, profile, target, {
        napiBindings: [napiPath],
        napiManifestDir: napiStage,
      });
      if (process.env.JAZZ_NAPI_BUILD_FAULT === "switch-boundary")
        throw new Error("planted NAPI publication-boundary failure");
      publishNapiGeneration(napiStage, join(root, "crates", "jazz-napi"), fingerprint, {
        lease: artifactLock.lease,
        afterPointerCommit: () => publishExpectedFingerprint(kind, fingerprint),
      });
    }
  } finally {
    if (wasmStage && existsSync(wasmStage.path))
      rmSync(wasmStage.path, { recursive: true, force: true });
    if (napiStage && existsSync(napiStage)) rmSync(napiStage, { recursive: true, force: true });
    artifactLock.release?.();
  }
}
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const [kind, profile = "release", ...extraArgs] = process.argv.slice(2);
  try {
    buildArtifact(kind, profile, extraArgs);
  } catch (error) {
    console.error(`artifact build: ${error.message}`);
    process.exitCode = 1;
  }
}
