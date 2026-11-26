import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { isProhibitedSymbol } from "../check-no-lattice-sha3-symbols.mjs";
import { planFor } from "../local-ci-equivalent.mjs";

const root = path.resolve(import.meta.dirname, "../../..");
const script = path.join(root, "dev/gates/check-no-lattice-sha3-symbols.mjs");
const label = "no lattice or SHA-3 symbols";

const encoder = new TextEncoder();
const u32 = (value) => {
  const out = [];
  do {
    let byte = value & 0x7f;
    value >>>= 7;
    if (value) byte |= 0x80;
    out.push(byte);
  } while (value);
  return out;
};
const string = (text) => {
  const bytes = [...encoder.encode(text)];
  return [...u32(bytes.length), ...bytes];
};
const section = (id, payload) => [id, ...u32(payload.length), ...payload];

// Functions `() -> ()` with empty bodies and a name section naming them.
function moduleWithNames(names) {
  const functionNames = [
    ...u32(names.length),
    ...names.flatMap((name, i) => [...u32(i), ...string(name)]),
  ];
  return Uint8Array.from([
    ...[0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00],
    ...section(1, [1, 0x60, 0, 0]),
    ...section(3, [names.length, ...names.map(() => 0)]),
    ...section(10, [names.length, ...names.flatMap(() => [2, 0, 0x0b])]),
    ...section(0, [...string("name"), ...section(1, functionNames)]),
  ]);
}

test("lattice and SHA-3 symbols are prohibited; SHA-2, SHA-384, TLS handshakes and SLH-DSA are not", () => {
  for (const symbol of [
    "keccak::f1600",
    "sha3::Sha3_256",
    "sha_3_digest",
    "slh_dsa::hashes::shake::Shake128f",
    "ml_dsa::sign",
    "ml-kem-768",
    "x_wing::encapsulate",
  ])
    assert.ok(isProhibitedSymbol(symbol), symbol);
  for (const symbol of [
    "sha2::sha256::compress256",
    "SHA384_Update",
    "ssl3_do_handshake",
    "slh_dsa::hashes::sha2::Sha2L1",
  ])
    assert.ok(!isProhibitedSymbol(symbol), symbol);
});

test("the check fails on a prohibited WASM function name or native symbol", (t) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "symbols-"));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const clean = path.join(dir, "clean.wasm");
  const planted = path.join(dir, "planted.wasm");
  fs.writeFileSync(clean, moduleWithNames(["jazz_wasm::run", "sha2::sha256::compress256"]));
  fs.writeFileSync(planted, moduleWithNames(["jazz_wasm::run", "keccak::f1600"]));
  // Node's own OpenSSL defines SHA-3 symbols: a native library that links them.
  const run = (wasm) => spawnSync(process.execPath, [script, wasm, process.execPath]);

  const wasm = run(planted);
  assert.equal(wasm.status, 1);
  assert.match(String(wasm.stderr), /Prohibited symbol: keccak::f1600/);

  const native = run(clean);
  assert.equal(native.status, 1);
  assert.doesNotMatch(String(native.stderr), /jazz_wasm|sha2::/);
  assert.match(String(native.stderr), /Prohibited symbol: .*sha3/i);
});

test("the check of a WASM module alone fails on a prohibited function name", (t) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "symbols-"));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const clean = path.join(dir, "clean.wasm");
  const planted = path.join(dir, "planted.wasm");
  fs.writeFileSync(clean, moduleWithNames(["jazz_wasm::run", "sha2::sha256::compress256"]));
  fs.writeFileSync(planted, moduleWithNames(["jazz_wasm::run", "keccak::f1600"]));
  const run = (wasm) => spawnSync(process.execPath, [script, wasm]);

  assert.equal(run(clean).status, 0);
  const wasm = run(planted);
  assert.equal(wasm.status, 1);
  assert.match(String(wasm.stderr), /Prohibited symbol: keccak::f1600/);
});

test("the package build checks the published WASM module after building it", () => {
  const workflow = fs.readFileSync(
    path.join(root, ".github/workflows/build-jazz-packages.yml"),
    "utf8",
  );
  const job = workflow.slice(workflow.indexOf("\n  build-wasm:\n"));
  const end = job.indexOf("\n  build-", 1);
  const steps = end === -1 ? job : job.slice(0, end);
  const build = steps.indexOf("run: pnpm --filter jazz-wasm build\n");
  const check = steps.indexOf(
    "run: node dev/gates/check-no-lattice-sha3-symbols.mjs crates/jazz-wasm/pkg/jazz_wasm_bg.wasm\n",
  );
  const upload = steps.indexOf("name: jazz-wasm-pkg");
  assert.ok(build >= 0, "the build-wasm job builds the release package");
  assert.ok(check > build, "the build-wasm job checks the module after building it");
  assert.ok(upload > check, "before uploading it");
});

test("the package build checks the published backend module before wasm-opt strips its names", () => {
  const workflow = fs.readFileSync(
    path.join(root, ".github/workflows/build-jazz-packages.yml"),
    "utf8",
  );
  const job = workflow.slice(workflow.indexOf("\n  build-wasm-backend:\n"));
  const steps = job.slice(0, job.indexOf("\n  test-wasm-durable-object:\n"));
  const build = steps.indexOf("run: pnpm --filter jazz-wasm build:backend\n");
  const check = steps.indexOf(
    "run: node dev/gates/check-no-lattice-sha3-symbols.mjs crates/jazz-wasm/backend/unstripped/jazz_wasm_backend_bg.wasm\n",
  );
  const upload = steps.indexOf("name: jazz-wasm-backend-pkg");
  assert.ok(build >= 0, "the build-wasm-backend job builds the backend module");
  assert.ok(check > build, "the build-wasm-backend job checks the module after building it");
  assert.ok(upload > check, "before uploading it");
});

test("CI checks the symbols of the artifacts the TypeScript examples partition produced", () => {
  const partition = planFor({ partition: "typescript-examples" });
  const check = partition.find((item) => item.label === label);
  assert.ok(check, "the partition must run the symbol check");
  assert.equal(check.executable, "node");
  assert.deepEqual(check.args, [
    "dev/gates/check-no-lattice-sha3-symbols.mjs",
    "--correctness-artifacts",
  ]);
  const producer = partition.findIndex(
    (item) => item.label === "native correctness-artifact producer",
  );
  assert.ok(producer >= 0 && producer < partition.indexOf(check), "after the producer");
});

test("the check reads several files, WASM modules and native libraries alike", (t) => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "symbols-"));
  t.after(() => fs.rmSync(dir, { recursive: true, force: true }));
  const clean = path.join(dir, "clean.wasm");
  const planted = path.join(dir, "planted.wasm");
  fs.writeFileSync(clean, moduleWithNames(["jazz_wasm::run"]));
  fs.writeFileSync(planted, moduleWithNames(["keccak::f1600"]));
  const run = (...files) => spawnSync(process.execPath, [script, ...files]);

  assert.equal(run(clean, clean).status, 0);
  // A prohibited name in a later file fails the run as in the first.
  const late = run(clean, clean, planted);
  assert.equal(late.status, 1);
  assert.match(String(late.stderr), /Prohibited symbol: keccak::f1600/);
});

test("CI checks the native libraries the bindings partition builds", () => {
  const partition = planFor({ partition: "jazz-ffi-bindings" });
  const check = partition.find((item) => item.label === "no lattice or SHA-3 native symbols");
  assert.ok(check, "the partition must check the native libraries");
  assert.deepEqual(
    [check.executable, ...check.args],
    ["bash", "dev/gates/native-crypto-symbols.sh"],
  );
  const gate = fs.readFileSync(path.join(root, "dev/gates/native-crypto-symbols.sh"), "utf8");
  // jazz-kotlin's and the Swift package's library, and the React Native relay
  // linked as an app links it (with --gc-sections: the staticlib alone keeps
  // every object of its dependencies).
  assert.match(gate, /-p jazz-ffi --lib/);
  assert.match(gate, /-p jazz-native-relay --lib/);
  assert.match(gate, /libjazz_native_relay\.a"[\s\\]*-Wl,--gc-sections/);
  assert.match(
    gate,
    /check-no-lattice-sha3-symbols\.mjs" \\\n\s+"\$root\/target\/debug\/libjazz_ffi\.so" \\\n\s+"\$output_dir\/jazz-native-relay"\n/,
  );
});
