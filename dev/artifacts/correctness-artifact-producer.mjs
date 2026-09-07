#!/usr/bin/env node
/**
 * The hand-off from native correctness-artifact production to TypeScript
 * consumers.  This receipt is intentionally separate from package output:
 * consumers only trust the content-addressed snapshot named here, never
 * whichever mutable NAPI/WASM generation happens to be present after a cache
 * restore.
 */
import { existsSync, lstatSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  correctnessArtifactStore,
  readCorrectnessArtifactSnapshotByFingerprint,
} from "./test-artifact-store.mjs";
import { artifactFeatures } from "./provenance.mjs";
import { checkedOutCommit, sourceIdentity } from "../gates/source-identity.mjs";

const shaPattern = /^[a-f0-9]{40}$/;
const hashPattern = /^[a-f0-9]{64}$/;
const sourceTreePattern = /^(?:[a-f0-9]{40}|[a-f0-9]{64})$/;
const snapshotFingerprintPattern = /^[a-f0-9]{64}-[a-f0-9]{64}$/;
const generatedNativeExpectations = [
  {
    kind: "WASM",
    manifestField: "wasmFingerprint",
    path: "packages/jazz-tools/src/runtime/native-artifact-fingerprint-wasm.ts",
    exportName: "EXPECTED_WASM_ARTIFACT_FINGERPRINT",
  },
  {
    kind: "NAPI",
    manifestField: "napiFingerprint",
    path: "packages/jazz-tools/src/runtime/native-artifact-fingerprint-napi.ts",
    exportName: "EXPECTED_NAPI_ARTIFACT_FINGERPRINT",
  },
];

// These are producer-owned build products.  They must never make an otherwise
// identical source checkout appear dirty merely because a producer has run.
// Keep this list at the hand-off boundary rather than relying on .gitignore:
// fixtures, sparse checkouts, and force-added generated files must preserve the
// same source contract.
const generatedArtifactPathspecs = [
  "target/**",
  "crates/jazz-wasm/pkg/**",
  "crates/jazz-wasm/.pkg-stage-*",
  "crates/jazz-wasm/.pkg-backup-*",
  "crates/jazz-wasm/.pkg-transaction.json*",
  "crates/jazz-wasm/.jazz-correctness-test-artifacts.json",
  "crates/jazz-napi/.native-artifacts/**",
  "crates/jazz-napi/.jazz-artifact-manifest.json",
  "crates/jazz-napi/native-binding.pointer.cjs",
  "crates/jazz-napi/correctness-native-binding.pointer.cjs",
  "crates/jazz-napi/native-binding.d.ts",
  "crates/jazz-napi/native-artifact-fingerprint.cjs",
  "crates/jazz-napi/native-loader.cjs",
  "packages/jazz-tools/dist/**",
  "packages/jazz-tools/src/runtime/native-artifact-fingerprint-napi.ts",
  "packages/jazz-tools/src/runtime/native-artifact-fingerprint-wasm.ts",
];

export function correctnessArtifactProducerManifest(root) {
  return join(correctnessArtifactStore(root), "producer-manifest.json");
}

export function checkoutRevision(root) {
  try {
    const revision = checkedOutCommit(root);
    if (!shaPattern.test(revision)) throw new Error("not a full commit SHA");
    return revision;
  } catch {
    throw new Error(
      "correctness artifacts: cannot bind producer manifest to this checkout revision",
    );
  }
}

/** Opaque full source identity for a producer/consumer hand-off. */
export function correctnessArtifactSourceIdentity(root) {
  return {
    commit: checkoutRevision(root),
    ...sourceIdentity(root, { excludePathspecs: generatedArtifactPathspecs }),
  };
}

function realFile(path, label) {
  if (!existsSync(path) || !lstatSync(path).isFile() || lstatSync(path).isSymbolicLink())
    throw new Error(`correctness artifacts: missing or unsafe ${label}`);
}

function parseManifest(path) {
  realFile(path, "producer manifest");
  try {
    return JSON.parse(readFileSync(path, "utf8"));
  } catch (error) {
    throw new Error(`correctness artifacts: invalid producer manifest (${error.message})`);
  }
}

function validateShape(manifest) {
  if (
    manifest?.schema !== 3 ||
    !shaPattern.test(manifest.source?.commit ?? "") ||
    !sourceTreePattern.test(manifest.source?.headTree ?? "") ||
    !sourceTreePattern.test(manifest.source?.indexTree ?? "") ||
    !hashPattern.test(manifest.source?.fingerprint ?? "") ||
    !hashPattern.test(manifest.source?.staged ?? "") ||
    !hashPattern.test(manifest.source?.unstaged ?? "") ||
    !hashPattern.test(manifest.source?.untracked ?? "") ||
    typeof manifest.source?.dirty !== "boolean" ||
    !snapshotFingerprintPattern.test(manifest.snapshotFingerprint ?? "") ||
    !hashPattern.test(manifest.wasmFingerprint ?? "") ||
    !hashPattern.test(manifest.napiFingerprint ?? "") ||
    typeof manifest.wasmPackage !== "string" ||
    typeof manifest.napiGeneration !== "string"
  )
    throw new Error("correctness artifacts: producer manifest has an invalid identity");
}

// Jazz Tools compiles these generated modules into the consumer runtime. They
// are intentionally excluded from source identity because artifact production
// rewrites them, but that exclusion must not let a restored release value
// validate a different sealed local snapshot.
function verifyGeneratedNativeExpectations(root, manifest) {
  for (const expectation of generatedNativeExpectations) {
    const path = join(root, expectation.path);
    realFile(path, `${expectation.kind} generated fingerprint expectation`);
    const source = readFileSync(path, "utf8");
    const actual = new RegExp(
      `export const ${expectation.exportName}\\s*=\\s*\\n?\\s*"([a-f0-9]{64})"\\s+as const;`,
    ).exec(source)?.[1];
    const expected = manifest[expectation.manifestField];
    if (actual !== expected)
      throw new Error(
        `correctness artifacts: generated ${expectation.kind} fingerprint expectation differs from sealed snapshot (expected ${expected}, found ${actual ?? "missing"})`,
      );
  }
}

/** Write after every native producer has completed and the pair is snapshotted. */
export function writeCorrectnessArtifactProducerManifest(rootInput, snapshot, expectedSource) {
  const root = resolve(rootInput);
  const source = correctnessArtifactSourceIdentity(root);
  if (expectedSource && JSON.stringify(expectedSource) !== JSON.stringify(source))
    throw new Error("correctness artifacts: source inputs changed while producing artifacts");
  const manifest = {
    schema: 3,
    source,
    snapshotFingerprint: snapshot.fingerprint,
    wasmFingerprint: snapshot.wasmFingerprint,
    napiFingerprint: snapshot.napiFingerprint,
    // These exact content-addressed paths are the consumer contract.  The
    // mutable package pointers are compatibility inputs for ordinary builds,
    // never correctness-consumer authority.
    wasmPackage: snapshot.wasmPackage,
    napiGeneration: snapshot.napiGeneration,
  };
  const path = correctnessArtifactProducerManifest(root);
  const temporary = `${path}.${process.pid}.${Date.now()}.tmp`;
  writeFileSync(temporary, `${JSON.stringify(manifest, null, 2)}\n`, { mode: 0o600 });
  renameSync(temporary, path);
  return manifest;
}

/** Fail closed before a TS build/test can consume cached or mutable artifacts. */
export function verifyCorrectnessArtifactProducer(rootInput) {
  const root = resolve(rootInput);
  const manifest = verifyCorrectnessArtifactSnapshot(root);
  const source = correctnessArtifactSourceIdentity(root);
  if (JSON.stringify(manifest.source) !== JSON.stringify(source))
    throw new Error("correctness artifacts: producer manifest belongs to different source inputs");
  return manifest;
}

/** Verify the immutable producer receipt and selected bytes, without re-admitting source. */
export function verifyCorrectnessArtifactSnapshot(rootInput) {
  const root = resolve(rootInput);
  const manifestPath = correctnessArtifactProducerManifest(root);
  const manifest = parseManifest(manifestPath);
  validateShape(manifest);
  const snapshot = readCorrectnessArtifactSnapshotByFingerprint(root, manifest.snapshotFingerprint);
  if (
    manifest.snapshotFingerprint !== snapshot.fingerprint ||
    manifest.wasmFingerprint !== snapshot.wasmFingerprint ||
    manifest.napiFingerprint !== snapshot.napiFingerprint ||
    manifest.wasmPackage !== snapshot.wasmPackage ||
    manifest.napiGeneration !== snapshot.napiGeneration
  )
    throw new Error("correctness artifacts: producer manifest does not match sealed snapshot");
  const napiManifest = parseManifest(join(snapshot.napiGeneration, ".jazz-artifact-manifest.json"));
  if (napiManifest.features !== artifactFeatures("napi"))
    throw new Error(
      "correctness artifacts: NAPI feature recipe differs from selected JAZZ_RN_TEST_BRIDGE mode",
    );
  verifyGeneratedNativeExpectations(root, manifest);
  return { ...manifest, snapshot };
}

/**
 * Environment handed to one consumer process tree after sealed admission.
 * The runner revalidates it after the process exits. Each value names a
 * snapshot file, never a generated pointer.
 */
export function correctnessArtifactConsumerEnvironment(rootInput) {
  const manifest = verifyCorrectnessArtifactProducer(rootInput);
  return {
    JAZZ_CORRECTNESS_ARTIFACT_RUN: "1",
    JAZZ_CORRECTNESS_WASM_PACKAGE: manifest.snapshot.wasmPackage,
    JAZZ_CORRECTNESS_NAPI_BINDING: join(manifest.snapshot.napiGeneration, "index.js"),
    JAZZ_CORRECTNESS_NAPI_FINGERPRINT: manifest.snapshot.napiFingerprint,
  };
}

/** Refuse a direct caller that supplied mutable or mismatched sealed paths. */
export function verifyCorrectnessArtifactConsumerEnvironment(rootInput, env = process.env) {
  // Source is admitted exactly once by correctnessArtifactConsumerEnvironment
  // before the outer consumer process starts. Nested consumers and the outer
  // postflight must keep checking the immutable receipt and bytes, but test
  // processes are allowed to create or remove ordinary workspace outputs.
  const manifest = verifyCorrectnessArtifactSnapshot(rootInput);
  const expected = {
    JAZZ_CORRECTNESS_ARTIFACT_RUN: "1",
    JAZZ_CORRECTNESS_WASM_PACKAGE: manifest.snapshot.wasmPackage,
    JAZZ_CORRECTNESS_NAPI_BINDING: join(manifest.snapshot.napiGeneration, "index.js"),
    JAZZ_CORRECTNESS_NAPI_FINGERPRINT: manifest.snapshot.napiFingerprint,
  };
  for (const [name, value] of Object.entries(expected)) {
    if (env[name] !== value)
      throw new Error(
        `correctness artifacts: sealed consumer ${name} does not match producer manifest`,
      );
  }
  return expected;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  try {
    verifyCorrectnessArtifactProducer(process.cwd());
    if (process.env.JAZZ_CORRECTNESS_ARTIFACT_RUN === "1")
      verifyCorrectnessArtifactConsumerEnvironment(process.cwd());
    console.log(
      "correctness artifacts: producer manifest matches this checkout and sealed snapshot",
    );
  } catch (error) {
    console.error(`correctness-artifacts: ${error.message}`);
    console.error("Fix: pnpm build:correctness-artifacts");
    process.exitCode = 1;
  }
}
