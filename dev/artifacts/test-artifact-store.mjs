/**
 * Worktree-private, immutable snapshots for the generated bindings consumed by
 * correctness tests.  Release assembly intentionally continues to publish to
 * the package directories; this module prevents a local test from resolving a
 * subsequently replaced mutable package generation.
 */
import { execFileSync } from "node:child_process";
import { existsSync, lstatSync, mkdirSync, mkdtempSync, readFileSync, renameSync, rmSync, writeFileSync, cpSync } from "node:fs";
import { basename, isAbsolute, join, resolve } from "node:path";

const pointerName = ".jazz-correctness-test-artifacts.json";

function gitDirectory(root) {
  const directory = execFileSync("git", ["rev-parse", "--git-dir"], {
    cwd: root,
    encoding: "utf8",
    stdio: ["ignore", "pipe", "ignore"],
  }).trim();
  return isAbsolute(directory) ? directory : resolve(root, directory);
}

export function correctnessArtifactStore(root) {
  return join(gitDirectory(root), "correctness-test-artifacts");
}

export function correctnessArtifactPointer(root) {
  return join(root, "crates", "jazz-wasm", pointerName);
}

function napiCorrectnessPointer(root) {
  return join(root, "crates", "jazz-napi", "correctness-native-binding.pointer.cjs");
}

function realDirectory(path, label) {
  if (!existsSync(path) || !lstatSync(path).isDirectory() || lstatSync(path).isSymbolicLink())
    throw new Error(`correctness artifacts: ${label} is not a real directory`);
}

function readManifest(path, label) {
  try {
    const manifest = JSON.parse(readFileSync(path, "utf8"));
    if (!/^[a-f0-9]{64}$/.test(manifest.nativeArtifactFingerprint ?? ""))
      throw new Error("missing native fingerprint");
    return manifest;
  } catch (error) {
    throw new Error(`correctness artifacts: invalid ${label} manifest (${error.message})`);
  }
}

function activeNapiGeneration(root) {
  const packageDir = join(root, "crates", "jazz-napi");
  const pointer = readFileSync(join(packageDir, "native-binding.pointer.cjs"), "utf8");
  const generation = /\.native-artifacts\/(generation-[A-Za-z0-9.-]+)\/index\.js/.exec(pointer)?.[1];
  if (!generation) throw new Error("correctness artifacts: active NAPI generation pointer is invalid");
  return join(packageDir, ".native-artifacts", generation);
}

function copyTree(source, destination) {
  cpSync(source, destination, {
    recursive: true,
    dereference: false,
    filter(path) {
      return !basename(path).startsWith(".pkg-stage-") && !basename(path).startsWith(".pkg-backup-");
    },
  });
}

/**
 * Snapshot the exact fast-WASM/release-NAPI pair after their ordinary mutable
 * producers have sealed provenance.  The content-addressed directory is
 * immutable: an already existing matching pair is reused, while a collision
 * with different metadata fails closed.
 */
export function snapshotCorrectnessArtifacts(root) {
  const wasmSource = join(root, "crates", "jazz-wasm", "pkg");
  const napiSource = activeNapiGeneration(root);
  realDirectory(wasmSource, "WASM package");
  realDirectory(napiSource, "NAPI generation");
  const wasm = readManifest(join(wasmSource, ".jazz-artifact-manifest.json"), "WASM");
  const napi = readManifest(join(napiSource, ".jazz-artifact-manifest.json"), "NAPI");
  if (wasm.kind !== "wasm" || wasm.profile !== "fast")
    throw new Error("correctness artifacts: expected a sealed fast WASM package");
  if (napi.kind !== "napi" || napi.profile !== "release")
    throw new Error("correctness artifacts: expected a sealed release NAPI generation");

  const fingerprint = `${wasm.nativeArtifactFingerprint}-${napi.nativeArtifactFingerprint}`;
  const store = correctnessArtifactStore(root);
  const destination = join(store, fingerprint);
  const receipt = {
    schema: 1,
    fingerprint,
    wasmFingerprint: wasm.nativeArtifactFingerprint,
    napiFingerprint: napi.nativeArtifactFingerprint,
    wasmPackage: join(destination, "wasm"),
    napiGeneration: join(destination, "napi"),
  };
  mkdirSync(store, { recursive: true });
  if (existsSync(destination)) {
    let existing;
    try {
      existing = JSON.parse(readFileSync(join(destination, "receipt.json"), "utf8"));
    } catch (error) {
      throw new Error(`correctness artifacts: invalid stored receipt (${error.message})`);
    }
    if (existing.fingerprint !== fingerprint)
      throw new Error("correctness artifacts: fingerprint-addressed snapshot collision");
  } else {
    const stage = mkdtempSync(join(store, ".stage-"));
    try {
      copyTree(wasmSource, join(stage, "wasm"));
      copyTree(napiSource, join(stage, "napi"));
      writeFileSync(join(stage, "receipt.json"), `${JSON.stringify(receipt, null, 2)}\n`, {
        mode: 0o600,
      });
      try {
        renameSync(stage, destination);
      } catch (error) {
        if (error.code !== "EEXIST" && error.code !== "ENOTEMPTY") throw error;
        rmSync(stage, { recursive: true, force: true });
      }
    } finally {
      if (existsSync(stage)) rmSync(stage, { recursive: true, force: true });
    }
  }
  // The pointer is checkout-local and atomically replaced only after the
  // complete snapshot exists. Browser and Node correctness runners use it;
  // installed/release packages never contain this ignored file.
  const pointer = correctnessArtifactPointer(root);
  const temporary = `${pointer}.${process.pid}.${Date.now()}.tmp`;
  writeFileSync(temporary, `${JSON.stringify(receipt, null, 2)}\n`, { mode: 0o600 });
  renameSync(temporary, pointer);
  const napiPointer = napiCorrectnessPointer(root);
  const napiTemporary = `${napiPointer}.${process.pid}.${Date.now()}.tmp`;
  writeFileSync(
    napiTemporary,
    `// Generated by dev/artifacts/test-artifact-store.mjs.\nconst nativeBinding = require(${JSON.stringify(join(destination, "napi", "index.js"))});\nconst { expectedNativeArtifactFingerprint } = require("./native-artifact-fingerprint.cjs");\nmodule.exports = { nativeBinding, expectedNativeArtifactFingerprint };\n`,
    { mode: 0o600 },
  );
  renameSync(napiTemporary, napiPointer);
  return receipt;
}

export function readCorrectnessArtifactSnapshot(root) {
  const pointer = correctnessArtifactPointer(root);
  if (!existsSync(pointer)) return null;
  let receipt;
  try {
    receipt = JSON.parse(readFileSync(pointer, "utf8"));
  } catch (error) {
    throw new Error(`correctness artifacts: unreadable snapshot pointer (${error.message})`);
  }
  if (
    typeof receipt?.fingerprint !== "string" ||
    typeof receipt.wasmPackage !== "string" ||
    typeof receipt.napiGeneration !== "string"
  )
    throw new Error("correctness artifacts: invalid snapshot pointer");
  const store = correctnessArtifactStore(root);
  const expected = join(store, receipt.fingerprint);
  if (
    resolve(receipt.wasmPackage) !== resolve(expected, "wasm") ||
    resolve(receipt.napiGeneration) !== resolve(expected, "napi")
  )
    throw new Error("correctness artifacts: snapshot pointer escapes this worktree store");
  realDirectory(receipt.wasmPackage, "stored WASM package");
  realDirectory(receipt.napiGeneration, "stored NAPI generation");
  return receipt;
}
