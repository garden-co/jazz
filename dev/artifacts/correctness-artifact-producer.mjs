#!/usr/bin/env node
/**
 * The hand-off from native correctness-artifact production to TypeScript
 * consumers.  This receipt is intentionally separate from package output:
 * consumers only trust the immutable snapshot named here, never whichever
 * mutable NAPI/WASM generation happens to be present after a cache restore.
 */
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { existsSync, lstatSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  correctnessArtifactStore,
  readCorrectnessArtifactSnapshot,
} from "./test-artifact-store.mjs";

const shaPattern = /^[a-f0-9]{40}$/;
const hashPattern = /^[a-f0-9]{64}$/;
const snapshotFingerprintPattern = /^[a-f0-9]{64}-[a-f0-9]{64}$/;

export function correctnessArtifactProducerManifest(root) {
  return join(correctnessArtifactStore(root), "producer-manifest.json");
}

function sha256(path) {
  return createHash("sha256").update(readFileSync(path)).digest("hex");
}

export function checkoutRevision(root) {
  try {
    const revision = execFileSync("git", ["rev-parse", "HEAD"], {
      cwd: root,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    }).trim();
    if (!shaPattern.test(revision)) throw new Error("not a full commit SHA");
    return revision;
  } catch {
    throw new Error(
      "correctness artifacts: cannot bind producer manifest to this checkout revision",
    );
  }
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
    manifest?.schema !== 1 ||
    !shaPattern.test(manifest.checkoutSha ?? "") ||
    !snapshotFingerprintPattern.test(manifest.snapshotFingerprint ?? "") ||
    !hashPattern.test(manifest.wasmFingerprint ?? "") ||
    !hashPattern.test(manifest.napiFingerprint ?? "") ||
    !hashPattern.test(manifest.cliSha256 ?? "")
  )
    throw new Error("correctness artifacts: producer manifest has an invalid identity");
}

/** Write after every native producer has completed and the pair is snapshotted. */
export function writeCorrectnessArtifactProducerManifest(rootInput, snapshot) {
  const root = resolve(rootInput);
  const cli = join(root, "target", "debug", "jazz-tools");
  realFile(cli, "CLI correctness artifact");
  const manifest = {
    schema: 1,
    checkoutSha: checkoutRevision(root),
    snapshotFingerprint: snapshot.fingerprint,
    wasmFingerprint: snapshot.wasmFingerprint,
    napiFingerprint: snapshot.napiFingerprint,
    cliSha256: sha256(cli),
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
  const manifestPath = correctnessArtifactProducerManifest(root);
  const manifest = parseManifest(manifestPath);
  validateShape(manifest);
  if (manifest.checkoutSha !== checkoutRevision(root))
    throw new Error(
      "correctness artifacts: producer manifest belongs to a different checkout revision",
    );
  const snapshot = readCorrectnessArtifactSnapshot(root);
  if (!snapshot)
    throw new Error("correctness artifacts: missing immutable correctness artifact snapshot");
  if (
    manifest.snapshotFingerprint !== snapshot.fingerprint ||
    manifest.wasmFingerprint !== snapshot.wasmFingerprint ||
    manifest.napiFingerprint !== snapshot.napiFingerprint
  )
    throw new Error("correctness artifacts: producer manifest does not match immutable snapshot");
  const cli = join(root, "target", "debug", "jazz-tools");
  realFile(cli, "CLI correctness artifact");
  if (manifest.cliSha256 !== sha256(cli))
    throw new Error("correctness artifacts: producer CLI fingerprint is stale");
  return manifest;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  try {
    verifyCorrectnessArtifactProducer(process.cwd());
    console.log(
      "correctness artifacts: producer manifest matches this checkout and immutable snapshot",
    );
  } catch (error) {
    console.error(`correctness-artifacts: ${error.message}`);
    console.error("Fix: pnpm build:correctness-artifacts");
    process.exitCode = 1;
  }
}
