// postcard wire format (varint/zigzag, length-prefixed bytes and strings).
// Rust gets this from serde derives + the postcard crate; here each type writes
// explicit encode/decode functions against this Writer/Reader.
import { type Bytes, utf8, fromUtf8 } from "./bytes.js";

export class PostcardError extends Error {}

export class Writer {
  private buf = new Uint8Array(64);
  private len = 0;
  private reserve(n: number) {
    if (this.len + n <= this.buf.length) return;
    let cap = this.buf.length * 2;
    while (cap < this.len + n) cap *= 2;
    const next = new Uint8Array(cap);
    next.set(this.buf.subarray(0, this.len));
    this.buf = next;
  }
  u8(v: number): this {
    this.reserve(1);
    this.buf[this.len++] = v & 0xff;
    return this;
  }
  bool(v: boolean): this {
    return this.u8(v ? 1 : 0);
  }
  varint(v: number): this {
    if (v < 0 || !Number.isInteger(v)) throw new PostcardError(`varint out of range: ${v}`);
    while (v >= 0x80) {
      this.u8((v % 0x80) | 0x80);
      v = Math.floor(v / 0x80);
    }
    return this.u8(v);
  }
  varint64(v: bigint): this {
    if (v < 0n) throw new PostcardError(`varint64 out of range: ${v}`);
    while (v >= 0x80n) {
      this.u8(Number(v & 0x7fn) | 0x80);
      v >>= 7n;
    }
    return this.u8(Number(v));
  }
  zigzag(v: number): this {
    return this.varint(v >= 0 ? v * 2 : -v * 2 - 1);
  }
  zigzag64(v: bigint): this {
    return this.varint64(v >= 0n ? v << 1n : (-v << 1n) - 1n);
  }
  f64(v: number): this {
    this.reserve(8);
    new DataView(this.buf.buffer).setFloat64(this.len, v, true);
    this.len += 8;
    return this;
  }
  f32(v: number): this {
    this.reserve(4);
    new DataView(this.buf.buffer).setFloat32(this.len, v, true);
    this.len += 4;
    return this;
  }
  raw(bytes: Bytes): this {
    this.reserve(bytes.length);
    this.buf.set(bytes, this.len);
    this.len += bytes.length;
    return this;
  }
  bytes(bytes: Bytes): this {
    return this.varint(bytes.length).raw(bytes);
  }
  str(s: string): this {
    return this.bytes(utf8(s));
  }
  option<T>(v: T | undefined | null, f: (w: this, v: T) => void): this {
    if (v === undefined || v === null) return this.u8(0);
    this.u8(1);
    f(this, v);
    return this;
  }
  seq<T>(
    items: Iterable<T> & { length?: number; size?: number },
    f: (w: this, v: T) => void,
  ): this {
    const arr = Array.isArray(items) ? items : [...items];
    this.varint(arr.length);
    for (const it of arr) f(this, it);
    return this;
  }
  variant(index: number): this {
    return this.varint(index);
  }
  finish(): Bytes {
    return this.buf.slice(0, this.len);
  }
}

export class Reader {
  private pos = 0;
  constructor(private readonly buf: Bytes) {}
  get remaining(): number {
    return this.buf.length - this.pos;
  }
  u8(): number {
    if (this.pos >= this.buf.length) throw new PostcardError("unexpected end of postcard input");
    return this.buf[this.pos++];
  }
  bool(): boolean {
    const b = this.u8();
    if (b > 1) throw new PostcardError(`invalid bool byte ${b}`);
    return b === 1;
  }
  varint(): number {
    let out = 0;
    let mul = 1;
    for (let i = 0; i < 8; i++) {
      const b = this.u8();
      out += (b & 0x7f) * mul;
      if ((b & 0x80) === 0) return out;
      mul *= 0x80;
    }
    throw new PostcardError("varint too long");
  }
  varint64(): bigint {
    let out = 0n;
    let shift = 0n;
    for (let i = 0; i < 10; i++) {
      const b = this.u8();
      out |= BigInt(b & 0x7f) << shift;
      if ((b & 0x80) === 0) return out;
      shift += 7n;
    }
    throw new PostcardError("varint64 too long");
  }
  zigzag(): number {
    const v = this.varint();
    return v % 2 === 0 ? v / 2 : -(v + 1) / 2;
  }
  zigzag64(): bigint {
    const v = this.varint64();
    return v & 1n ? -((v + 1n) >> 1n) : v >> 1n;
  }
  f64(): number {
    const v = new DataView(this.buf.buffer, this.buf.byteOffset).getFloat64(this.pos, true);
    this.pos += 8;
    return v;
  }
  f32(): number {
    const v = new DataView(this.buf.buffer, this.buf.byteOffset).getFloat32(this.pos, true);
    this.pos += 4;
    return v;
  }
  raw(n: number): Bytes {
    if (this.pos + n > this.buf.length) throw new PostcardError("unexpected end of postcard input");
    const out = this.buf.subarray(this.pos, this.pos + n);
    this.pos += n;
    return out;
  }
  bytes(): Bytes {
    return this.raw(this.varint());
  }
  str(): string {
    return fromUtf8(this.bytes());
  }
  option<T>(f: (r: this) => T): T | undefined {
    return this.bool() ? f(this) : undefined;
  }
  seq<T>(f: (r: this) => T): T[] {
    const n = this.varint();
    const out: T[] = [];
    for (let i = 0; i < n; i++) out.push(f(this));
    return out;
  }
  variant(max: number, what: string): number {
    const v = this.varint();
    if (v >= max) throw new PostcardError(`invalid ${what} variant ${v}`);
    return v;
  }
  finish(): void {
    if (this.pos !== this.buf.length)
      throw new PostcardError(`${this.remaining} trailing bytes after postcard value`);
  }
}

export function encode<T>(v: T, f: (w: Writer, v: T) => void): Bytes {
  const w = new Writer();
  f(w, v);
  return w.finish();
}
export function decode<T>(bytes: Bytes, f: (r: Reader) => T): T {
  const r = new Reader(bytes);
  const v = f(r);
  r.finish();
  return v;
}
