/**
 * `vector(n)` column values (SPEC 20.1): `n` IEEE-754 binary32 values.
 *
 * At the JS edge a vector is a `Float32Array` (a `number[]` is accepted on
 * write). Inside the runtime it is carried as a `Bytea` value holding its
 * `f32le/1` bytes: each element's bits little-endian, `4n` bytes, no header.
 * Those are also its record payload and SQLite cell, so a vector crosses
 * every binding (NAPI, WASM, React Native) as the same bytes.
 */

export const VECTOR_ENCODING = "f32le/1";
export const VECTOR_MAX_DIMENSIONS = 4096;

function where(column?: string): string {
  return column ? ` for column ${column}` : "";
}

/**
 * Encode a JS vector as `f32le/1` bytes, refusing a wrong length or a NaN or
 * infinite element (before the write reaches Rust, which refuses them too).
 */
export function vectorToBytes(value: unknown, dimensions: number, column?: string): Uint8Array {
  if (isVectorLiteralJson(value)) {
    const bytes = hexToBytes(value.$f32le);
    assertVectorBytes(bytes, dimensions, column);
    return bytes;
  }
  let elements: ArrayLike<unknown>;
  if (value instanceof Float32Array || Array.isArray(value)) {
    elements = value;
  } else if (value instanceof Float64Array) {
    elements = value;
  } else {
    throw new Error(
      `Expected a Float32Array or an array of numbers${where(column)} (vector(${dimensions}))`,
    );
  }
  if (elements.length !== dimensions) {
    throw new Error(
      `Vector${where(column)} has ${elements.length} elements, expected ${dimensions}`,
    );
  }
  const bytes = new Uint8Array(dimensions * 4);
  const view = new DataView(bytes.buffer);
  for (let index = 0; index < dimensions; index += 1) {
    const element = elements[index];
    if (typeof element !== "number") {
      throw new Error(`Vector element ${index}${where(column)} is not a number`);
    }
    view.setFloat32(index * 4, element, true);
    // Checked after rounding: a finite f64 beyond f32 range becomes infinite.
    if (!Number.isFinite(view.getFloat32(index * 4, true))) {
      throw new Error(
        `Vector element ${index}${where(column)} is NaN or infinite (or outside float32 range)`,
      );
    }
  }
  return bytes;
}

/** Check `f32le/1` bytes of a `vector(dimensions)` without decoding them. */
export function assertVectorBytes(bytes: Uint8Array, dimensions: number, column?: string): void {
  if (bytes.byteLength !== dimensions * 4) {
    throw new Error(
      `Vector${where(column)} has ${bytes.byteLength} bytes, expected ${dimensions * 4} (${dimensions} elements)`,
    );
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  for (let index = 0; index < dimensions; index += 1) {
    if (!Number.isFinite(view.getFloat32(index * 4, true))) {
      throw new Error(`Vector element ${index}${where(column)} is NaN or infinite`);
    }
  }
}

/** Decode `f32le/1` bytes into a fresh `Float32Array` (any host endianness). */
export function bytesToVector(bytes: Uint8Array): Float32Array {
  if (bytes.byteLength % 4 !== 0) {
    throw new Error(`Vector bytes have length ${bytes.byteLength}, not a multiple of 4`);
  }
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  const out = new Float32Array(bytes.byteLength / 4);
  for (let index = 0; index < out.length; index += 1) {
    out[index] = view.getFloat32(index * 4, true);
  }
  return out;
}

/**
 * A vector literal as query-builder JSON: the hex of its `f32le/1` bytes.
 * JSON numbers can't carry `-0`, and vectors compare bit for bit.
 */
export type VectorLiteralJson = { $f32le: string };

export function vectorLiteralJson(value: Float32Array): VectorLiteralJson {
  const bytes = vectorToBytes(value, value.length);
  let hex = "";
  for (const byte of bytes) hex += byte.toString(16).padStart(2, "0");
  return { $f32le: hex };
}

function isVectorLiteralJson(value: unknown): value is VectorLiteralJson {
  return (
    typeof value === "object" &&
    value !== null &&
    Object.keys(value).length === 1 &&
    typeof (value as { $f32le?: unknown }).$f32le === "string"
  );
}

function hexToBytes(hex: string): Uint8Array {
  if (hex.length % 2 !== 0 || !/^[0-9a-fA-F]*$/.test(hex)) {
    throw new Error("Invalid $f32le vector literal: expected hex of f32le bytes");
  }
  const bytes = new Uint8Array(hex.length / 2);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
  }
  return bytes;
}
