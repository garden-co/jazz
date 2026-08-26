/** Worktree-private immutable bindings consumed only by correctness tests. */
import { createHash } from "node:crypto";
import {
  cpSync,
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
import { basename, isAbsolute, join, relative, resolve, sep } from "node:path";

const pointerName = ".jazz-correctness-test-artifacts.json";
const fingerprintPattern = /^[a-f0-9]{64}-[a-f0-9]{64}$/;
const hashPattern = /^[a-f0-9]{64}$/;

// `target` belongs to one checkout even when linked worktrees share Git data.
// Staying below the checkout also lets Vite serve validated WASM without
// widening its filesystem root.
export function correctnessArtifactStore(root) {
  return join(resolve(root), "target", "correctness-test-artifacts");
}

export function correctnessArtifactPointer(root) {
  return join(resolve(root), "crates", "jazz-wasm", pointerName);
}

function napiCorrectnessPointer(root) {
  return join(resolve(root), "crates", "jazz-napi", "correctness-native-binding.pointer.cjs");
}

function isWithin(parent, candidate) {
  const path = relative(resolve(parent), resolve(candidate));
  return path === "" || (!path.startsWith(`..${sep}`) && path !== ".." && !isAbsolute(path));
}

function rejectSymlinkAncestors(root, candidate, label) {
  if (!isWithin(root, candidate))
    throw new Error(`correctness artifacts: ${label} escapes the worktree`);
  let current = resolve(root);
  for (const part of relative(current, resolve(candidate)).split(sep).filter(Boolean)) {
    current = join(current, part);
    if (existsSync(current) && lstatSync(current).isSymbolicLink())
      throw new Error(`correctness artifacts: ${label} has a symbolic-link ancestor`);
  }
}

function realFile(path, label) {
  if (!existsSync(path)) throw new Error(`correctness artifacts: missing ${label}`);
  const stat = lstatSync(path);
  if (!stat.isFile() || stat.isSymbolicLink())
    throw new Error(`correctness artifacts: ${label} is not a real file`);
}

function realDirectory(path, label) {
  if (!existsSync(path)) throw new Error(`correctness artifacts: missing ${label}`);
  const stat = lstatSync(path);
  if (!stat.isDirectory() || stat.isSymbolicLink())
    throw new Error(`correctness artifacts: ${label} is not a real directory`);
}

function walkRealFiles(root, label) {
  realDirectory(root, label);
  const files = [];
  const visit = (directory) => {
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const path = join(directory, entry.name);
      const stat = lstatSync(path);
      if (stat.isSymbolicLink())
        throw new Error(`correctness artifacts: ${label} contains a symbolic link`);
      if (stat.isDirectory()) visit(path);
      else if (stat.isFile()) files.push(path);
      else throw new Error(`correctness artifacts: ${label} contains a non-regular entry`);
    }
  };
  visit(root);
  return files.sort();
}

const sha256 = (path) => createHash("sha256").update(readFileSync(path)).digest("hex");

function parseJsonFile(path, label) {
  realFile(path, label);
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch (error) {
    throw new Error(`correctness artifacts: invalid ${label} (${error.message})`);
  }
}

function validateManifest(directory, kind, profile, expectedFingerprint) {
  const manifest = parseJsonFile(
    join(directory, ".jazz-artifact-manifest.json"),
    `${kind} manifest`,
  );
  if (
    manifest.kind !== kind ||
    manifest.profile !== profile ||
    manifest.nativeArtifactFingerprint !== expectedFingerprint ||
    !hashPattern.test(manifest.nativeArtifactFingerprint ?? "") ||
    !Array.isArray(manifest.artifacts) ||
    manifest.artifacts.length === 0
  )
    throw new Error(`correctness artifacts: ${kind} manifest has the wrong identity`);
  const seen = new Set();
  for (const artifact of manifest.artifacts) {
    if (
      typeof artifact?.file !== "string" ||
      basename(artifact.file) !== artifact.file ||
      seen.has(artifact.file) ||
      !hashPattern.test(artifact.sha256 ?? "")
    )
      throw new Error(`correctness artifacts: ${kind} manifest has an invalid artifact entry`);
    seen.add(artifact.file);
    const path = join(directory, artifact.file);
    realFile(path, `${kind} artifact ${artifact.file}`);
    if (sha256(path) !== artifact.sha256)
      throw new Error(`correctness artifacts: ${kind} artifact hash mismatch (${artifact.file})`);
  }
  return manifest;
}

function activeNapiGeneration(root) {
  const packageDir = join(root, "crates", "jazz-napi");
  const pointerPath = join(packageDir, "native-binding.pointer.cjs");
  realFile(pointerPath, "active NAPI pointer");
  const generation = /\.native-artifacts\/(generation-[A-Za-z0-9.-]+)\/index\.js/.exec(
    readFileSync(pointerPath, "utf8"),
  )?.[1];
  if (!generation)
    throw new Error("correctness artifacts: active NAPI generation pointer is invalid");
  const path = join(packageDir, ".native-artifacts", generation);
  if (!isWithin(join(packageDir, ".native-artifacts"), path))
    throw new Error("correctness artifacts: active NAPI generation escapes its store");
  return path;
}

function fileReceipt(destination) {
  const files = {};
  for (const directory of ["wasm", "napi"])
    for (const path of walkRealFiles(join(destination, directory), `stored ${directory}`))
      files[relative(destination, path).split(sep).join("/")] = sha256(path);
  return files;
}

function validateReceiptShape(receipt, store) {
  if (
    receipt?.schema !== 1 ||
    !fingerprintPattern.test(receipt.fingerprint ?? "") ||
    !hashPattern.test(receipt.wasmFingerprint ?? "") ||
    !hashPattern.test(receipt.napiFingerprint ?? "") ||
    receipt.fingerprint !== `${receipt.wasmFingerprint}-${receipt.napiFingerprint}` ||
    typeof receipt.wasmPackage !== "string" ||
    typeof receipt.napiGeneration !== "string" ||
    !receipt.files ||
    Array.isArray(receipt.files) ||
    typeof receipt.files !== "object"
  )
    throw new Error("correctness artifacts: invalid snapshot receipt");
  const destination = join(store, receipt.fingerprint);
  if (
    !isWithin(store, destination) ||
    resolve(receipt.wasmPackage) !== resolve(destination, "wasm") ||
    resolve(receipt.napiGeneration) !== resolve(destination, "napi")
  )
    throw new Error("correctness artifacts: snapshot pointer escapes this worktree store");
  return destination;
}

function validateStoredSnapshot(root, receipt) {
  const store = correctnessArtifactStore(root);
  rejectSymlinkAncestors(root, store, "snapshot store");
  const destination = validateReceiptShape(receipt, store);
  realDirectory(destination, "stored snapshot");
  const storedReceipt = parseJsonFile(join(destination, "receipt.json"), "stored receipt");
  if (JSON.stringify(storedReceipt) !== JSON.stringify(receipt))
    throw new Error("correctness artifacts: pointer and stored receipt differ");
  validateManifest(receipt.wasmPackage, "wasm", "fast", receipt.wasmFingerprint);
  validateManifest(receipt.napiGeneration, "napi", "release", receipt.napiFingerprint);
  const actualFiles = fileReceipt(destination);
  const expectedEntries = Object.entries(receipt.files);
  if (
    expectedEntries.some(
      ([path, hash]) =>
        path === "receipt.json" ||
        isAbsolute(path) ||
        path.split("/").includes("..") ||
        !hashPattern.test(hash) ||
        actualFiles[path] !== hash,
    ) ||
    Object.keys(actualFiles).length !== expectedEntries.length
  )
    throw new Error("correctness artifacts: stored snapshot file inventory or hash differs");
  return receipt;
}

function copyRealTree(source, destination, label) {
  walkRealFiles(source, label); // Reject every symlink before copying any byte.
  cpSync(source, destination, { recursive: true, dereference: false });
  walkRealFiles(destination, `copied ${label}`);
}

/** Seal the exact fast-WASM/release-NAPI pair into an immutable checkout store. */
export function snapshotCorrectnessArtifacts(rootInput) {
  const root = resolve(rootInput);
  const wasmSource = join(root, "crates", "jazz-wasm", "pkg");
  const napiSource = activeNapiGeneration(root);
  rejectSymlinkAncestors(root, wasmSource, "WASM package");
  rejectSymlinkAncestors(root, napiSource, "NAPI generation");
  walkRealFiles(wasmSource, "WASM package");
  walkRealFiles(napiSource, "NAPI generation");
  const wasm = parseJsonFile(join(wasmSource, ".jazz-artifact-manifest.json"), "WASM manifest");
  const napi = parseJsonFile(join(napiSource, ".jazz-artifact-manifest.json"), "NAPI manifest");
  validateManifest(wasmSource, "wasm", "fast", wasm.nativeArtifactFingerprint);
  validateManifest(napiSource, "napi", "release", napi.nativeArtifactFingerprint);

  const fingerprint = `${wasm.nativeArtifactFingerprint}-${napi.nativeArtifactFingerprint}`;
  if (!fingerprintPattern.test(fingerprint))
    throw new Error("correctness artifacts: invalid combined native fingerprint");
  const store = correctnessArtifactStore(root);
  const destination = join(store, fingerprint);
  if (!isWithin(store, destination))
    throw new Error("correctness artifacts: snapshot destination escapes this worktree store");
  mkdirSync(store, { recursive: true });
  rejectSymlinkAncestors(root, store, "snapshot store");

  if (!existsSync(destination)) {
    const stage = mkdtempSync(join(store, ".stage-"));
    try {
      copyRealTree(wasmSource, join(stage, "wasm"), "WASM package");
      copyRealTree(napiSource, join(stage, "napi"), "NAPI generation");
      const receipt = {
        schema: 1,
        fingerprint,
        wasmFingerprint: wasm.nativeArtifactFingerprint,
        napiFingerprint: napi.nativeArtifactFingerprint,
        wasmPackage: join(destination, "wasm"),
        napiGeneration: join(destination, "napi"),
        files: fileReceipt(stage),
      };
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

  const receipt = parseJsonFile(join(destination, "receipt.json"), "stored receipt");
  validateStoredSnapshot(root, receipt); // Fail closed on collision or stale reuse.
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

export function readCorrectnessArtifactSnapshot(rootInput) {
  const root = resolve(rootInput);
  const pointer = correctnessArtifactPointer(root);
  if (!existsSync(pointer)) return null;
  const receipt = parseJsonFile(pointer, "snapshot pointer");
  return validateStoredSnapshot(root, receipt);
}
