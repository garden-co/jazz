/** Worktree-private sealed bindings consumed only by correctness tests. */
import { createHash } from "node:crypto";
import {
  cpSync,
  chmodSync,
  existsSync,
  lstatSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  renameSync,
  rmSync,
  statSync,
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

function assertPublishedFile(stat, label) {
  // A content-addressed snapshot is producer-owned after publication.  Do not
  // accept a hardlink (which gives a mutable producer another name for the
  // same inode), or a writable file.  `chmod` is intentionally checked at
  // every admission as well: ordinary POSIX permissions cannot stop the file
  // owner from deliberately changing modes. They do make accidental producer
  // replacement fail, while admission and end-of-run checks detect changes.
  if (stat.nlink !== 1) throw new Error(`correctness artifacts: ${label} has a hardlink`);
  if ((stat.mode & 0o222) !== 0)
    throw new Error(`correctness artifacts: ${label} is writable after publication`);
}

function assertPublishedDirectory(stat, label) {
  if ((stat.mode & 0o222) !== 0)
    throw new Error(`correctness artifacts: ${label} directory is writable after publication`);
}

function realFile(path, label, { published = false } = {}) {
  if (!existsSync(path)) throw new Error(`correctness artifacts: missing ${label}`);
  const stat = lstatSync(path);
  if (!stat.isFile() || stat.isSymbolicLink())
    throw new Error(`correctness artifacts: ${label} is not a real file`);
  if (published) assertPublishedFile(stat, label);
}

function realDirectory(path, label, { published = false } = {}) {
  if (!existsSync(path)) throw new Error(`correctness artifacts: missing ${label}`);
  const stat = lstatSync(path);
  if (!stat.isDirectory() || stat.isSymbolicLink())
    throw new Error(`correctness artifacts: ${label} is not a real directory`);
  if (published) assertPublishedDirectory(stat, label);
}

function walkRealFiles(root, label, { published = false } = {}) {
  realDirectory(root, label, { published });
  const files = [];
  const visit = (directory) => {
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const path = join(directory, entry.name);
      const stat = lstatSync(path);
      if (stat.isSymbolicLink())
        throw new Error(`correctness artifacts: ${label} contains a symbolic link`);
      if (stat.isDirectory()) {
        if (published) assertPublishedDirectory(stat, `${label} child`);
        visit(path);
      } else if (stat.isFile()) {
        if (published) assertPublishedFile(stat, `${label} child`);
        files.push(path);
      } else throw new Error(`correctness artifacts: ${label} contains a non-regular entry`);
    }
  };
  visit(root);
  return files.sort();
}

const sha256 = (path) => createHash("sha256").update(readFileSync(path)).digest("hex");

function parseJsonFile(path, label, options) {
  realFile(path, label, options);
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch (error) {
    throw new Error(`correctness artifacts: invalid ${label} (${error.message})`);
  }
}

function validateManifest(directory, kind, profile, expectedFingerprint, options) {
  const manifest = parseJsonFile(
    join(directory, ".jazz-artifact-manifest.json"),
    `${kind} manifest`,
    options,
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
    realFile(path, `${kind} artifact ${artifact.file}`, options);
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

function validateReceiptShape(receipt, store, root) {
  if (
    receipt?.schema !== 3 ||
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
  const destination = validateReceiptShape(receipt, store, root);
  // Check the entire tree before parsing any receipt/manifest from it.  This
  // catches a same-worktree chmod/replacement attempt as well as symlinks and
  // hardlinks before any consumer receives a path into the snapshot.
  walkRealFiles(destination, "stored snapshot", { published: true });
  const storedReceipt = parseJsonFile(join(destination, "receipt.json"), "stored receipt", {
    published: true,
  });
  if (JSON.stringify(storedReceipt) !== JSON.stringify(receipt))
    throw new Error("correctness artifacts: pointer and stored receipt differ");
  validateManifest(receipt.wasmPackage, "wasm", "fast", receipt.wasmFingerprint, {
    published: true,
  });
  validateManifest(receipt.napiGeneration, "napi", "release", receipt.napiFingerprint, {
    published: true,
  });
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

function sealSnapshot(destination) {
  // Seal leaf files before their directories. Existing executable bits are
  // preserved, but no snapshot entry remains writable.
  const files = walkRealFiles(destination, "staged snapshot");
  for (const path of files) {
    const mode = statSync(path).mode;
    chmodSync(path, (mode & 0o111) === 0 ? 0o444 : 0o555);
  }
  const directories = [];
  const visit = (directory) => {
    directories.push(directory);
    for (const entry of readdirSync(directory, { withFileTypes: true })) {
      const path = join(directory, entry.name);
      if (entry.isDirectory()) visit(path);
    }
  };
  visit(destination);
  for (const directory of directories.reverse()) chmodSync(directory, 0o555);
  walkRealFiles(destination, "sealed snapshot", { published: true });
}

function removeOwnedStage(stage) {
  if (!existsSync(stage)) return;
  // Only this mkdtemp-owned path is relaxed. Never make the winning
  // content-addressed destination writable while resolving a publication
  // collision.
  const visit = (directory) => {
    const stat = lstatSync(directory);
    if (!stat.isDirectory() || stat.isSymbolicLink())
      throw new Error("correctness artifacts: private stage is not a real directory");
    chmodSync(directory, 0o700);
    for (const entry of readdirSync(directory, { withFileTypes: true }))
      if (entry.isDirectory()) visit(join(directory, entry.name));
  };
  visit(stage);
  rmSync(stage, { recursive: true, force: true });
}

/** Seal the exact fast-WASM/release-NAPI pair into a content-addressed checkout store. */
export function snapshotCorrectnessArtifacts(rootInput, { beforePublish } = {}) {
  const root = resolve(rootInput);
  const store = correctnessArtifactStore(root);
  // Check the destination boundary before inspecting a source below `target`:
  // an unexpected target symlink must not redirect a later mkdir/copy operation.
  rejectSymlinkAncestors(root, store, "snapshot store");
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
  const destination = join(store, fingerprint);
  if (!isWithin(store, destination))
    throw new Error("correctness artifacts: snapshot destination escapes this worktree store");
  mkdirSync(store, { recursive: true });

  if (!existsSync(destination)) {
    const stage = mkdtempSync(join(store, ".stage-"));
    try {
      copyRealTree(wasmSource, join(stage, "wasm"), "WASM package");
      copyRealTree(napiSource, join(stage, "napi"), "NAPI generation");
      const receipt = {
        schema: 3,
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
      sealSnapshot(stage);
      beforePublish?.({ destination, stage });
      try {
        renameSync(stage, destination);
      } catch (error) {
        if (error.code !== "EEXIST" && error.code !== "ENOTEMPTY") throw error;
        removeOwnedStage(stage);
      }
    } finally {
      removeOwnedStage(stage);
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
    `// Generated by dev/artifacts/test-artifact-store.mjs.\nconst nativeBinding = require(${JSON.stringify(join(destination, "napi", "index.js"))});\nmodule.exports = { nativeBinding, expectedNativeArtifactFingerprint: ${JSON.stringify(receipt.napiFingerprint)} };\n`,
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

/**
 * Read one sealed, content-addressed generation without consulting the
 * mutable worktree pointer.  Correctness consumers use this through the
 * producer manifest, so a later producer cannot redirect an already admitted
 * consumer to a different WASM/NAPI pair.
 */
export function readCorrectnessArtifactSnapshotByFingerprint(rootInput, fingerprint) {
  const root = resolve(rootInput);
  if (!fingerprintPattern.test(fingerprint))
    throw new Error("correctness artifacts: invalid snapshot fingerprint");
  const store = correctnessArtifactStore(root);
  rejectSymlinkAncestors(root, store, "snapshot store");
  const receipt = parseJsonFile(join(store, fingerprint, "receipt.json"), "stored receipt");
  if (receipt.fingerprint !== fingerprint)
    throw new Error("correctness artifacts: stored snapshot receipt fingerprint differs");
  return validateStoredSnapshot(root, receipt);
}
