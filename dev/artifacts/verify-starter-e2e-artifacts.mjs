#!/usr/bin/env node
/**
 * The starter E2E prepare job packs jazz-tools and jazz-wasm separately.  A
 * Jazz Tools build must therefore be compiled after the generated runtime
 * fingerprint sources are staged from the exact native artifacts being packed.
 * Check that hand-off before the matrix can turn it into twelve opaque browser
 * timeouts.
 */
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const repositoryRoot = resolve(fileURLToPath(new URL("../..", import.meta.url)));

function manifestFingerprint(path, kind) {
  const manifest = JSON.parse(readFileSync(path, "utf8"));
  if (manifest.kind !== kind || manifest.profile !== "release")
    throw new Error(`${kind} manifest has an unexpected kind or profile`);
  if (!/^[a-f0-9]{64}$/.test(manifest.nativeArtifactFingerprint ?? ""))
    throw new Error(`${kind} manifest lacks a native artifact fingerprint`);
  return manifest.nativeArtifactFingerprint;
}

function compiledFingerprint(path, symbol) {
  const source = readFileSync(path, "utf8");
  const match = source.match(new RegExp(`export const ${symbol} = "([a-f0-9]{64})"`));
  if (!match) throw new Error(`${symbol} is missing from ${path}`);
  return match[1];
}

function activeNapiManifest(root) {
  const pointer = readFileSync(join(root, "crates/jazz-napi/native-binding.pointer.cjs"), "utf8");
  const generation = /generation-[A-Za-z0-9.-]+/.exec(pointer)?.[0];
  if (!generation) throw new Error("NAPI build has no active generation pointer");
  return join(
    root,
    "crates/jazz-napi/.native-artifacts",
    generation,
    ".jazz-artifact-manifest.json",
  );
}

async function wasmFingerprint(root) {
  const pkg = join(root, "crates/jazz-wasm/pkg");
  const bindings = await import(pathToFileURL(join(pkg, "jazz_wasm.js")).href);
  bindings.initSync({ module: readFileSync(join(pkg, "jazz_wasm_bg.wasm")) });
  const actual = bindings.nativeArtifactFingerprint();
  if (!/^[a-f0-9]{64}$/.test(actual))
    throw new Error("WASM module lacks a native artifact fingerprint");
  return actual;
}
function napiFingerprints(root) {
  const pointer = join(root, "crates/jazz-napi/native-binding.pointer.cjs");
  const { nativeBinding, expectedNativeArtifactFingerprint } = createRequire(pointer)(pointer);
  return {
    actual: nativeBinding.nativeArtifactFingerprint(),
    expected: expectedNativeArtifactFingerprint,
  };
}

export async function verifyStarterE2EArtifacts(
  root = repositoryRoot,
  loadWasmFingerprint = wasmFingerprint,
  loadNapiFingerprints = napiFingerprints,
) {
  const expected = {
    wasm: manifestFingerprint(
      join(root, "crates/jazz-wasm/pkg/.jazz-artifact-manifest.json"),
      "wasm",
    ),
    napi: manifestFingerprint(activeNapiManifest(root), "napi"),
  };
  const compiled = {
    wasm: compiledFingerprint(
      join(root, "packages/jazz-tools/dist/runtime/native-artifact-fingerprint-wasm.js"),
      "EXPECTED_WASM_ARTIFACT_FINGERPRINT",
    ),
    napi: compiledFingerprint(
      join(root, "packages/jazz-tools/dist/runtime/native-artifact-fingerprint-napi.js"),
      "EXPECTED_NAPI_ARTIFACT_FINGERPRINT",
    ),
  };
  for (const kind of ["wasm", "napi"]) {
    if (compiled[kind] !== expected[kind])
      throw new Error(
        `starter E2E ${kind.toUpperCase()} package hand-off mismatch: jazz-tools expects ${compiled[kind]}, native artifact is ${expected[kind]}. Re-stage native fingerprints and rebuild jazz-tools.`,
      );
  }
  const actualWasm = await loadWasmFingerprint(root);
  if (actualWasm !== expected.wasm)
    throw new Error(
      `starter E2E WASM package hand-off mismatch: manifest expects ${expected.wasm}, module returns ${actualWasm}.`,
    );
  const napi = await loadNapiFingerprints(root);
  if (
    napi.actual !== expected.napi ||
    napi.expected !== expected.napi ||
    napi.actual !== compiled.napi
  )
    throw new Error(
      `starter E2E NAPI package hand-off mismatch: manifest and jazz-tools expect ${expected.napi}, pointer expects ${napi.expected}, module returns ${napi.actual}.`,
    );
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  await verifyStarterE2EArtifacts();
  console.log("starter E2E package hand-off: jazz-tools matches release WASM and NAPI artifacts");
}
