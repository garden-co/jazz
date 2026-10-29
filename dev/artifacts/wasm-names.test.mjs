import assert from "node:assert/strict";
import test from "node:test";
import { stripSymbolHashes } from "./wasm-names.mjs";

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
const MAGIC_AND_VERSION = [0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00];
const section = (id, payload) => [id, ...u32(payload.length), ...payload];

// Two functions `() -> ()` with empty bodies, followed by a name section with
// a module-name subsection (0), a function-name subsection (1) and a
// local-name subsection (2), so the rewrite must keep untouched subsections.
function moduleWithNames(names) {
  const functionNames = [
    ...u32(names.length),
    ...names.flatMap((name, i) => [...u32(i), ...string(name)]),
  ];
  const nameSection = [
    ...string("name"),
    ...section(0, string("jazz_wasm")),
    ...section(1, functionNames),
    ...section(2, [...u32(1), ...u32(0), ...u32(1), ...u32(0), ...string("arg")]),
  ];
  return Uint8Array.from([
    ...MAGIC_AND_VERSION,
    ...section(1, [1, 0x60, 0, 0]),
    ...section(3, [names.length, ...names.map(() => 0)]),
    ...section(10, [names.length, ...names.flatMap(() => [2, 0, 0x0b])]),
    ...section(0, nameSection),
  ]);
}

test("drops only the legacy mangling hash from function names", () => {
  const before = moduleWithNames([
    "jazz_db::db::Db::insert::h1f0c2a3b4c5d6e7f",
    "core::ptr::drop_in_place<jazz_types::ids::RowUuid>::h0123456789abcdef",
    "__wbindgen_malloc",
    "not_a_hash::h12345",
  ]);
  const after = moduleWithNames([
    "jazz_db::db::Db::insert",
    "core::ptr::drop_in_place<jazz_types::ids::RowUuid>",
    "__wbindgen_malloc",
    "not_a_hash::h12345",
  ]);
  assert.deepEqual(stripSymbolHashes(before), after);
  assert.ok(WebAssembly.validate(stripSymbolHashes(before)));
});

test("leaves a module without hashes byte-identical", () => {
  const module = moduleWithNames(["a", "b::c"]);
  assert.deepEqual(stripSymbolHashes(module), module);
});

test("keeps names identical when two monomorphizations collapse to one name", () => {
  const module = stripSymbolHashes(
    moduleWithNames([
      "core::slice::sort::quicksort::h955e656589f57941",
      "core::slice::sort::quicksort::h1cb94767b1bf9af1",
    ]),
  );
  assert.deepEqual(
    module,
    moduleWithNames(["core::slice::sort::quicksort", "core::slice::sort::quicksort"]),
  );
  assert.ok(WebAssembly.validate(module));
});

test("rejects a truncated module instead of writing a broken one", () => {
  const module = moduleWithNames(["f::h0123456789abcdef"]);
  assert.throws(() => stripSymbolHashes(module.subarray(0, module.length - 3)), /truncated/);
});

// A module with no functions whose name section is `subsections` verbatim.
const moduleWithNameSection = (subsections) =>
  Uint8Array.from([...MAGIC_AND_VERSION, ...section(0, [...string("name"), ...subsections])]);

// `WebAssembly.validate` ignores custom sections, so these check the rewrite
// itself rejects malformed names instead of emitting a different module.
test("rejects a name subsection that claims more bytes than the section has", () => {
  const module = moduleWithNameSection([0, 0x7f, ...string("jazz_wasm")]);
  assert.throws(() => stripSymbolHashes(module), /truncated name subsection/);
});

test("rejects a function name longer than its subsection", () => {
  const functionNames = [...u32(1), ...u32(0), 0x7f, ...encoder.encode("f::h0123456789abcdef")];
  const module = moduleWithNameSection(section(1, functionNames));
  assert.throws(() => stripSymbolHashes(module), /truncated function name/);
});
