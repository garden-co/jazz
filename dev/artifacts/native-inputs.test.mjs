import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { ensureNativeInputs, isCI, missingNativeInputs } from "./native-inputs.mjs";

const fingerprint = "b".repeat(64);
const wasmFiles = ["jazz_wasm_bg.wasm", "jazz_wasm_bg.wasm.d.ts", "jazz_wasm.d.ts", "jazz_wasm.js"];
const fingerprintFile = (root, kind) =>
  join(root, `packages/jazz-tools/src/runtime/native-artifact-fingerprint-${kind}.ts`);

function fixture() {
  const root = mkdtempSync(join(tmpdir(), "jazz-native-inputs-"));
  mkdirSync(join(root, "packages/jazz-tools/src/runtime"), { recursive: true });
  return root;
}

function writeWasm(root, profile = "release") {
  const pkg = join(root, "crates/jazz-wasm/pkg");
  mkdirSync(pkg, { recursive: true });
  const artifacts = wasmFiles.map((file) => {
    writeFileSync(join(pkg, file), `bytes of ${file}`);
    return { file, sha256: createHash("sha256").update(`bytes of ${file}`).digest("hex") };
  });
  writeFileSync(
    join(pkg, ".jazz-artifact-manifest.json"),
    JSON.stringify({ kind: "wasm", profile, nativeArtifactFingerprint: fingerprint, artifacts }),
  );
}

function writeNapi(root) {
  const generation = join(root, "crates/jazz-napi/.native-artifacts/generation-host");
  mkdirSync(generation, { recursive: true });
  writeFileSync(
    join(generation, ".jazz-artifact-manifest.json"),
    JSON.stringify({ kind: "napi", profile: "release", nativeArtifactFingerprint: fingerprint }),
  );
  writeFileSync(
    join(root, "crates/jazz-napi/native-binding.pointer.cjs"),
    'require("./.native-artifacts/generation-host/index.js");',
  );
}

// Stands in for `node dev/artifacts/build.mjs <kind> release`.
function fakeBuild(root, calls, { status = 0 } = {}) {
  return (_command, args) => {
    const kind = args[1];
    calls.push(`${kind}:${args[2]}`);
    if (status === 0) (kind === "wasm" ? writeWasm : writeNapi)(root);
    return { status };
  };
}

const quiet = () => {};

test("a fresh checkout builds the host's WASM and NAPI inputs and stages them with --workspace", () => {
  const root = fixture();
  try {
    const calls = [];
    const steps = ensureNativeInputs(root, { env: {}, run: fakeBuild(root, calls), log: quiet });
    assert.deepEqual(calls, ["wasm:release", "napi:release"]);
    assert.deepEqual(steps, ["wasm", "napi", "stage:workspace"]);
    for (const kind of ["napi", "wasm"])
      assert.match(readFileSync(fingerprintFile(root, kind), "utf8"), new RegExp(fingerprint));
    assert.deepEqual(missingNativeInputs(root), { wasm: false, napi: false, fingerprints: false });
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("present inputs are reused and only the missing ones are produced", () => {
  const root = fixture();
  try {
    writeWasm(root);
    const calls = [];
    assert.deepEqual(
      ensureNativeInputs(root, { env: {}, run: fakeBuild(root, calls), log: quiet }),
      ["napi", "stage:workspace"],
    );
    assert.deepEqual(calls, ["napi:release"]);
    // Everything present: nothing runs, nothing is restaged.
    assert.deepEqual(
      ensureNativeInputs(root, {
        env: {},
        run: () => assert.fail("must not build"),
        log: quiet,
      }),
      [],
    );
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("only missing fingerprints restage from the WASM profile already built", () => {
  const root = fixture();
  try {
    writeWasm(root, "fast");
    writeNapi(root);
    const steps = ensureNativeInputs(root, {
      env: {},
      run: () => assert.fail("must not build"),
      log: quiet,
    });
    assert.deepEqual(steps, ["stage:local"]);
    assert.ok(existsSync(fingerprintFile(root, "wasm")));
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

// What "Assemble npm packages" has when it builds jazz-tools: the downloaded
// WASM package, the release NAPI manifests, and fingerprints staged from them.
// There is no local build pointer.
test("a CI release assembly counts as having every native input", () => {
  const root = fixture();
  try {
    writeWasm(root);
    mkdirSync(join(root, "crates/jazz-napi/provenance"), { recursive: true });
    writeFileSync(
      join(root, "crates/jazz-napi/provenance/jazz-napi.linux-x64-gnu.manifest.json"),
      JSON.stringify({ kind: "napi", profile: "release", nativeArtifactFingerprint: fingerprint }),
    );
    for (const kind of ["napi", "wasm"]) writeFileSync(fingerprintFile(root, kind), "staged");
    assert.deepEqual(missingNativeInputs(root), { wasm: false, napi: false, fingerprints: false });
    assert.deepEqual(
      ensureNativeInputs(root, {
        env: { CI: "true" },
        run: () => assert.fail("must not build"),
        log: quiet,
      }),
      [],
    );
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("CI never builds native inputs implicitly", () => {
  const root = fixture();
  try {
    assert.throws(
      () =>
        ensureNativeInputs(root, {
          env: { CI: "true" },
          run: () => assert.fail("must not build"),
          log: quiet,
        }),
      /missing native inputs in CI \(jazz-wasm package, jazz-napi binding, native fingerprints\)/,
    );
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("a failed native build stops before staging", () => {
  const root = fixture();
  try {
    const calls = [];
    assert.throws(
      () =>
        ensureNativeInputs(root, {
          env: {},
          run: fakeBuild(root, calls, { status: 2 }),
          log: quiet,
        }),
      /the wasm build failed \(exit 2\)/,
    );
    assert.deepEqual(calls, ["wasm:release"]);
    assert.equal(existsSync(fingerprintFile(root, "napi")), false);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("CI detection ignores explicit false values", () => {
  for (const value of ["true", "1", "yes"]) assert.equal(isCI({ CI: value }), true, value);
  for (const value of [undefined, "", "false", "FALSE", "0"])
    assert.equal(isCI({ CI: value }), false, String(value));
});
