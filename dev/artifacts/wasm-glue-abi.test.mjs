import assert from "node:assert/strict";
import test from "node:test";
import { assertWasmGlueInstantiates, verifyWasmGlueAbi } from "./wasm-glue-abi.mjs";

// A minimal module importing `wbg.__wbg_wasmwrite_new`. This is the import
// that exposed the real stale worker-glue failure; keep the fixture binary so
// the check exercises the browser's actual WebAssembly import parser.
const wasmWithWasmWriteImport = new Uint8Array([
  0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x04, 0x01, 0x60, 0x00, 0x00, 0x02, 0x1b,
  0x01, 0x03, 0x77, 0x62, 0x67, 0x13, 0x5f, 0x5f, 0x77, 0x62, 0x67, 0x5f, 0x77, 0x61, 0x73, 0x6d,
  0x77, 0x72, 0x69, 0x74, 0x65, 0x5f, 0x6e, 0x65, 0x77, 0x00, 0x00,
]);

function generatedGlue(imports) {
  return `function __wbg_get_imports() {
    const import0 = { ${imports} };
    return { wbg: import0 };
  }`;
}

test("accepts callable glue in wasm-bindgen's generated import object", () => {
  assert.equal(
    verifyWasmGlueAbi(wasmWithWasmWriteImport, generatedGlue("__wbg_wasmwrite_new: () => {}")),
    null,
  );
});

test("rejects a planted stale glue/WASM pair", () => {
  assert.match(
    verifyWasmGlueAbi(wasmWithWasmWriteImport, generatedGlue("")),
    /wbg\.__wbg_wasmwrite_new/,
  );
});

for (const [name, glue] of [
  ["a comment", generatedGlue("/*\n__wbg_wasmwrite_new: () => {}\n*/")],
  [
    "an unrelated dead object",
    `const dead = { __wbg_wasmwrite_new: () => {} }; ${generatedGlue("")}`,
  ],
  ["a nested dead object", generatedGlue("nested: { __wbg_wasmwrite_new: () => {} }")],
  ["a prefix-only property", generatedGlue("__wbg_wasmwrite_newer: () => {}")],
]) {
  test(`rejects ${name}`, () => {
    assert.match(verifyWasmGlueAbi(wasmWithWasmWriteImport, glue), /wbg\.__wbg_wasmwrite_new/);
  });
}

test("instantiates the generated import object against its binary", async () => {
  await assertWasmGlueInstantiates(
    wasmWithWasmWriteImport,
    generatedGlue("__wbg_wasmwrite_new: () => {}"),
  );
});
