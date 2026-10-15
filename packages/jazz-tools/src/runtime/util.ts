import { Utf8Decoder } from "./utf8.js";

/** Return `value` if it is an Error, otherwise wrap its string form in one. */
export function toError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}

/** A non-null, non-array object. */
export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/** Concatenate byte chunks into one new buffer. */
export function concatBytes(chunks: readonly Uint8Array[]): Uint8Array {
  const out = new Uint8Array(chunks.reduce((length, chunk) => length + chunk.length, 0));
  let offset = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}

const fatalUtf8Decoder = new Utf8Decoder({ fatal: true });

export function isValidUtf8(bytes: Uint8Array): boolean {
  try {
    fatalUtf8Decoder.decode(bytes);
    return true;
  } catch {
    return false;
  }
}
