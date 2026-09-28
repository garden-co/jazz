#!/usr/bin/env node
// Function-name prefixes are byte attribution, not a retained-size call graph.
import { readFileSync, writeFileSync } from "node:fs";
import { createHash } from "node:crypto";
import { gzipSync, brotliCompressSync, constants } from "node:zlib";
const file = process.argv[2],
  dest = process.argv[3];
if (!file)
  throw Error(
    "Usage: inspect.mjs module.wasm [receipt.json]; WASM_SIZE_SKIP_BROTLI=1 for quick iteration",
  );
const b = readFileSync(file);
let p = 8;
if (b.length < 8 || b.readUInt32LE(0) !== 0x6d736100 || b.readUInt32LE(4) !== 1)
  throw Error("Expected a WebAssembly v1 binary");
function u() {
  let n = 0,
    s = 0,
    c;
  do {
    if (p >= b.length) throw Error("Truncated LEB");
    c = b[p++];
    n += (c & 127) * 2 ** s;
    s += 7;
    if (s > 49) throw Error("bad LEB");
  } while (c & 128);
  return n;
}
function str() {
  const n = u(),
    s = b.subarray(p, p + n).toString();
  p += n;
  return s;
}
const sections = [],
  names = new Map(),
  sizes = [];
let imports = 0;
function limits() {
  const f = u();
  u();
  if (f & 1) u();
}
while (p < b.length) {
  const start = p,
    id = b[p++],
    len = u(),
    end = p + len;
  let name;
  if (end > b.length) throw Error("Section exceeds module length");
  if (id === 0) {
    name = str();
    if (name === "name") {
      while (p < end) {
        const kind = b[p++],
          n = u(),
          stop = p + n;
        if (kind === 1) {
          const count = u();
          for (let i = 0; i < count; i++) {
            const idx = u();
            names.set(idx, str());
          }
        }
        p = stop;
      }
    }
  }
  if (id === 2) {
    const n = u();
    for (let i = 0; i < n; i++) {
      str();
      str();
      const kind = b[p++];
      if (kind === 0) {
        u();
        imports++;
      } else if (kind === 1) {
        p++;
        limits();
      } else if (kind === 2) {
        limits();
      } else if (kind === 3) {
        p += 2;
      } else if (kind === 4) {
        p++;
        u();
      } else throw Error("import " + kind);
    }
  }
  if (id === 10) {
    const n = u();
    for (let i = 0; i < n; i++) {
      const bytes = u();
      sizes.push({ index: imports + i, bytes });
      p += bytes;
    }
  }
  sections.push({ id, name, bytes: end - start, payload: len, offset: start });
  p = end;
}
const functions = sizes
  .map((x) => ({ ...x, name: names.get(x.index) ?? `unnamed[${x.index}]` }))
  .sort((a, b) => b.bytes - a.bytes);
const groups = new Map();
for (const f of functions) {
  const raw = f.name.replace(/^</, "");
  const group = raw.split("::")[0];
  groups.set(group, (groups.get(group) ?? 0) + f.bytes);
}
const receipt = {
  file,
  sha256: createHash("sha256").update(b).digest("hex"),
  bytes: b.length,
  gzip9: gzipSync(b, { level: 9 }).length,
  brotli11: process.env.WASM_SIZE_SKIP_BROTLI
    ? null
    : brotliCompressSync(b, { params: { [constants.BROTLI_PARAM_QUALITY]: 11 } }).length,
  imports,
  function_count: sizes.length,
  sections,
  groups: [...groups.entries()].sort((a, b) => b[1] - a[1]),
  functions,
};
if (dest) writeFileSync(dest, JSON.stringify(receipt, null, 2) + "\n");
console.log(
  JSON.stringify(
    { ...receipt, functions: receipt.functions.slice(0, 25), groups: receipt.groups.slice(0, 30) },
    null,
    2,
  ),
);
