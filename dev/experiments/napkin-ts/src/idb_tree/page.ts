// IDBTree v1 page encoding (fixed little-endian storage format).
import { compareBytes, equalBytes } from "../support/bytes.js";
import { xxh3_64 } from "./xxh3.js";

export type PageId = number;

const MAGIC = new Uint8Array([73, 68, 66, 84, 82, 69, 69, 0]); // "IDBTREE\0"
const FORMAT_VERSION = 1;
const HEADER_LEN = MAGIC.length + 1 + 8;

export const LEAF_BASE_LEN = HEADER_LEN + 1 + 4;
export const INTERNAL_BASE_LEN = HEADER_LEN + 1 + 4 + 4 + 8;

export type ValueCell =
  | { kind: "Inline"; bytes: Uint8Array }
  | { kind: "Overflow"; head: PageId; len: number };

export type Page =
  | { kind: "Leaf"; entries: [Uint8Array, ValueCell][] }
  | { kind: "Internal"; keys: Uint8Array[]; children: PageId[] }
  | { kind: "Overflow"; next: PageId | undefined; bytes: Uint8Array };

export const leafPage = (): Page => ({ kind: "Leaf", entries: [] });

export const leafEntryLen = (key: Uint8Array, value: ValueCell) =>
  4 + key.length + (value.kind === "Inline" ? 1 + 4 + value.bytes.length : 1 + 8 + 8);
export const internalKeyLen = (key: Uint8Array) => 4 + key.length + 8;

/** The exact v1 encoded length of `page`, without encoding it. */
export function encodedLen(page: Page): number {
  switch (page.kind) {
    case "Leaf":
      return page.entries.reduce((n, [k, v]) => n + leafEntryLen(k, v), LEAF_BASE_LEN);
    case "Internal":
      return (
        page.keys.reduce((n, k) => n + 4 + k.length, HEADER_LEN + 1 + 4) +
        4 +
        8 * page.children.length
      );
    case "Overflow":
      return HEADER_LEN + 1 + (page.next !== undefined ? 9 : 1) + 4 + page.bytes.length;
  }
}

const PAGE_LEAF_TAG = 0;
const PAGE_INTERNAL_TAG = 1;
const PAGE_OVERFLOW_TAG = 2;
const VALUE_INLINE_TAG = 0;
const VALUE_OVERFLOW_TAG = 1;
const OPTION_NONE_TAG = 0;
const OPTION_SOME_TAG = 1;

class Out {
  parts: number[] = [];
  u8(v: number) {
    this.parts.push(v & 0xff);
  }
  u32(v: number) {
    for (let i = 0; i < 4; i++) this.parts.push((v >>> (8 * i)) & 0xff);
  }
  u64(v: number | bigint) {
    let b = BigInt(v);
    for (let i = 0; i < 8; i++, b >>= 8n) this.parts.push(Number(b & 0xffn));
  }
  len(n: number, name: string) {
    if (n > 0xffffffff) throw `${name} exceeds u32`;
    this.u32(n);
  }
  bytes(b: Uint8Array, name: string) {
    this.len(b.length, name);
    for (const x of b) this.parts.push(x);
  }
}

/** Encode one page using the fixed IDBTree v1 storage format. */
export function encodePage(page: Page): Uint8Array {
  const out = new Out();
  switch (page.kind) {
    case "Leaf":
      validateStrictlyIncreasing(
        page.entries.map(([k]) => k),
        "leaf keys",
      );
      out.u8(PAGE_LEAF_TAG);
      out.len(page.entries.length, "leaf entry count");
      for (const [key, value] of page.entries) {
        out.bytes(key, "leaf key");
        if (value.kind === "Inline") {
          out.u8(VALUE_INLINE_TAG);
          out.bytes(value.bytes, "inline value");
        } else {
          if (value.len === 0) throw "overflow value length must not be zero";
          out.u8(VALUE_OVERFLOW_TAG);
          out.u64(value.head);
          out.u64(value.len);
        }
      }
      break;
    case "Internal":
      validateInternalShape(page.keys, page.children);
      out.u8(PAGE_INTERNAL_TAG);
      out.len(page.keys.length, "internal keys");
      for (const k of page.keys) out.bytes(k, "internal keys");
      out.len(page.children.length, "internal child count");
      for (const c of page.children) out.u64(c);
      break;
    case "Overflow":
      if (!page.bytes.length) throw "overflow page bytes must not be empty";
      out.u8(PAGE_OVERFLOW_TAG);
      if (page.next === undefined) out.u8(OPTION_NONE_TAG);
      else {
        out.u8(OPTION_SOME_TAG);
        out.u64(page.next);
      }
      out.bytes(page.bytes, "overflow page bytes");
      break;
  }
  const payload = Uint8Array.from(out.parts);
  const header = new Out();
  for (const b of MAGIC) header.u8(b);
  header.u8(FORMAT_VERSION);
  header.u64(xxh3_64(payload));
  const encoded = new Uint8Array(HEADER_LEN + payload.length);
  encoded.set(header.parts);
  encoded.set(payload, HEADER_LEN);
  return encoded;
}

export function decodePage(bytes: Uint8Array): Page {
  if (bytes.length < HEADER_LEN || !equalBytes(bytes.subarray(0, MAGIC.length), MAGIC))
    throw "bad magic";
  if (bytes[MAGIC.length] !== FORMAT_VERSION)
    throw `unsupported format version ${bytes[MAGIC.length]}`;
  const dv = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const expected = dv.getBigUint64(MAGIC.length + 1, true);
  const payload = bytes.subarray(HEADER_LEN);
  if (xxh3_64(payload) !== expected) throw "checksum mismatch";
  const d = new Decoder(payload);
  let page: Page;
  const tag = d.u8("page tag");
  switch (tag) {
    case PAGE_LEAF_TAG: {
      const count = d.count("leaf entry count", 9);
      const entries: [Uint8Array, ValueCell][] = [];
      for (let i = 0; i < count; i++) {
        const key = d.bytes("leaf key");
        const vtag = d.u8("value-cell tag");
        let value: ValueCell;
        if (vtag === VALUE_INLINE_TAG) value = { kind: "Inline", bytes: d.bytes("inline value") };
        else if (vtag === VALUE_OVERFLOW_TAG) {
          const head = d.u64("overflow value head");
          const len = d.u64("overflow value length");
          if (len === 0) throw "overflow value length must not be zero";
          value = { kind: "Overflow", head, len };
        } else throw `unknown value-cell tag ${vtag}`;
        entries.push([key, value]);
      }
      validateStrictlyIncreasing(
        entries.map(([k]) => k),
        "leaf keys",
      );
      page = { kind: "Leaf", entries };
      break;
    }
    case PAGE_INTERNAL_TAG: {
      const keys = d.byteStrings("internal keys");
      const childCount = d.count("internal child count", 8);
      const children: PageId[] = [];
      for (let i = 0; i < childCount; i++) children.push(d.u64("internal child page ID"));
      validateInternalShape(keys, children);
      page = { kind: "Internal", keys, children };
      break;
    }
    case PAGE_OVERFLOW_TAG: {
      const ntag = d.u8("overflow next tag");
      let next: PageId | undefined;
      if (ntag === OPTION_SOME_TAG) next = d.u64("overflow next page ID");
      else if (ntag !== OPTION_NONE_TAG) throw `unknown overflow next tag ${ntag}`;
      const b = d.bytes("overflow page bytes");
      if (!b.length) throw "overflow page bytes must not be empty";
      page = { kind: "Overflow", next, bytes: b };
      break;
    }
    default:
      throw `unknown page tag ${tag}`;
  }
  if (d.remaining()) throw "trailing page payload bytes";
  return page;
}

function validateInternalShape(keys: Uint8Array[], children: PageId[]): void {
  validateStrictlyIncreasing(keys, "internal keys");
  if (children.length !== keys.length + 1)
    throw `internal page has ${keys.length} keys but ${children.length} children`;
  if (new Set(children).size !== children.length) throw "internal page has a shared child";
}

function validateStrictlyIncreasing(values: Uint8Array[], name: string): void {
  for (let i = 1; i < values.length; i++)
    if (compareBytes(values[i - 1], values[i]) >= 0) throw `${name} must be strictly increasing`;
}

class Decoder {
  private pos = 0;
  constructor(private readonly buf: Uint8Array) {}
  remaining(): number {
    return this.buf.length - this.pos;
  }
  take(len: number, name: string): Uint8Array {
    if (len > this.remaining()) throw `truncated ${name}`;
    const out = this.buf.subarray(this.pos, this.pos + len);
    this.pos += len;
    return out;
  }
  u8(name: string): number {
    return this.take(1, name)[0];
  }
  u32(name: string): number {
    const b = this.take(4, name);
    return (b[0] | (b[1] << 8) | (b[2] << 16) | (b[3] << 24)) >>> 0;
  }
  u64(name: string): number {
    const b = this.take(8, name);
    return (
      new DataView(b.buffer, b.byteOffset, 8).getUint32(0, true) +
      new DataView(b.buffer, b.byteOffset, 8).getUint32(4, true) * 2 ** 32
    );
  }
  count(name: string, minimumItemBytes: number): number {
    const count = this.u32(name);
    if (count > Math.floor(this.remaining() / minimumItemBytes))
      throw `${name} exceeds the remaining payload`;
    return count;
  }
  bytes(name: string): Uint8Array {
    return this.take(this.u32(`${name} length`), name).slice();
  }
  byteStrings(name: string): Uint8Array[] {
    const n = this.count(`${name} count`, 4);
    const out: Uint8Array[] = [];
    for (let i = 0; i < n; i++) out.push(this.bytes(name));
    return out;
  }
}
