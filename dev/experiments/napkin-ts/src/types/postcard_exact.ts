import { Reader, Writer, PostcardError } from "../support/postcard.js";
import { equalBytes } from "../support/bytes.js";

/**
 * Decode one canonical postcard value only when it consumes the complete input
 * and re-encodes to exactly the same bytes (rejects overlong varints).
 */
export function decodePostcardExact<T>(
  bytes: Uint8Array,
  read: (r: Reader) => T,
  write: (w: Writer, v: T) => void,
): T {
  const r = new Reader(bytes);
  const value = read(r);
  if (r.remaining !== 0 || !encodesExactly(value, write, bytes))
    throw new PostcardError("The original data was not well encoded");
  return value;
}

export function encodesExactly<T>(
  value: T,
  write: (w: Writer, v: T) => void,
  expected: Uint8Array,
): boolean {
  const w = new Writer();
  try {
    write(w, value);
  } catch {
    return false;
  }
  return equalBytes(w.finish(), expected);
}
