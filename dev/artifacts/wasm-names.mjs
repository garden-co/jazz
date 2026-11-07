#!/usr/bin/env node
// Shrink the browser WASM's name section without losing function names.
//
// Release builds keep the name section (`wasm-opt -g`) so stack traces and
// profiles show Rust function names. wasm-bindgen writes those names with the
// legacy mangling hash still attached, e.g.
// `jazz_db::db::Db::insert::h1f0c…`. The hash is
// random hex that names nothing a reader can use, and since gzip and Brotli
// cannot compress it, it costs about 2% of the shipped bundle. This drops only
// that suffix; every name stays readable. Names may then repeat (one per
// monomorphization), which the name section allows.
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const LEGACY_HASH_SUFFIX = /::h[0-9a-f]{16}$/;
const CUSTOM_SECTION = 0;
const FUNCTION_NAMES = 1;

function readU32(bytes, offset) {
  let value = 0;
  let shift = 0;
  for (;;) {
    if (offset >= bytes.length) throw new Error("truncated LEB128 in WASM module");
    const byte = bytes[offset++];
    value += (byte & 0x7f) * 2 ** shift;
    shift += 7;
    if (byte < 0x80) return [value, offset];
    if (shift > 28) throw new Error("LEB128 value exceeds u32 in WASM module");
  }
}

function writeU32(value) {
  const out = [];
  do {
    let byte = value & 0x7f;
    value = Math.floor(value / 128);
    if (value) byte |= 0x80;
    out.push(byte);
  } while (value);
  return Uint8Array.from(out);
}

function concat(parts) {
  const out = new Uint8Array(parts.reduce((total, part) => total + part.length, 0));
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
}

function stripFunctionNames(subsection) {
  const decoder = new TextDecoder("utf-8", { fatal: true });
  const encoder = new TextEncoder();
  let [count, offset] = readU32(subsection, 0);
  const parts = [writeU32(count)];
  while (count--) {
    let index, length;
    [index, offset] = readU32(subsection, offset);
    [length, offset] = readU32(subsection, offset);
    if (offset + length > subsection.length) throw new Error("truncated function name");
    const name = decoder.decode(subsection.subarray(offset, offset + length));
    offset += length;
    const stripped = encoder.encode(name.replace(LEGACY_HASH_SUFFIX, ""));
    parts.push(writeU32(index), writeU32(stripped.length), stripped);
  }
  if (offset !== subsection.length) throw new Error("malformed function-name subsection");
  return concat(parts);
}

function stripNameSection(payload, nameEnd) {
  const parts = [payload.subarray(0, nameEnd)];
  let offset = nameEnd;
  while (offset < payload.length) {
    const id = payload[offset];
    const [size, start] = readU32(payload, offset + 1);
    if (start + size > payload.length) throw new Error("truncated name subsection");
    let body = payload.subarray(start, start + size);
    if (id === FUNCTION_NAMES) body = stripFunctionNames(body);
    parts.push(Uint8Array.of(id), writeU32(body.length), body);
    offset = start + size;
  }
  return concat(parts);
}

/** Return `module` with legacy Rust symbol hashes removed from function names. */
export function stripSymbolHashes(module) {
  const bytes = module instanceof Uint8Array ? module : new Uint8Array(module);
  const parts = [bytes.subarray(0, 8)];
  let offset = 8;
  while (offset < bytes.length) {
    const id = bytes[offset];
    const [size, start] = readU32(bytes, offset + 1);
    if (start + size > bytes.length) throw new Error("truncated section in WASM module");
    let payload = bytes.subarray(start, start + size);
    if (id === CUSTOM_SECTION) {
      const [nameLength, nameStart] = readU32(payload, 0);
      const nameEnd = nameStart + nameLength;
      if (new TextDecoder().decode(payload.subarray(nameStart, nameEnd)) === "name")
        payload = stripNameSection(payload, nameEnd);
    }
    parts.push(Uint8Array.of(id), writeU32(payload.length), payload);
    offset = start + size;
  }
  return concat(parts);
}

/** Rewrite the WASM file at `path` in place. */
export function stripSymbolHashesInFile(path) {
  writeFileSync(path, stripSymbolHashes(readFileSync(path)));
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const paths = process.argv.slice(2);
  if (!paths.length) throw new Error("usage: wasm-names.mjs FILE.wasm...");
  for (const path of paths) stripSymbolHashesInFile(path);
}
