/**
 * Lowercase hex formatting shared by runtime hot paths.
 *
 * `Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("")`
 * allocates a string per byte plus an intermediate array; a 256-entry lookup
 * table with string concatenation produces the same text an order of
 * magnitude faster, which matters for per-row result keys and UUID decoding.
 */

const BYTE_HEX: readonly string[] = Array.from({ length: 256 }, (_, byte) =>
  byte.toString(16).padStart(2, "0"),
);

/** Lowercase hex of every byte, without separators. */
export function bytesToHex(bytes: ArrayLike<number>): string {
  let hex = "";
  for (let index = 0; index < bytes.length; index++) {
    hex += BYTE_HEX[bytes[index]! & 0xff]!;
  }
  return hex;
}

/** Canonical lowercase `8-4-4-4-12` UUID text for the 16 bytes at `offset`. */
export function formatUuidAt(bytes: ArrayLike<number>, offset: number): string {
  return (
    BYTE_HEX[bytes[offset]! & 0xff]! +
    BYTE_HEX[bytes[offset + 1]! & 0xff]! +
    BYTE_HEX[bytes[offset + 2]! & 0xff]! +
    BYTE_HEX[bytes[offset + 3]! & 0xff]! +
    "-" +
    BYTE_HEX[bytes[offset + 4]! & 0xff]! +
    BYTE_HEX[bytes[offset + 5]! & 0xff]! +
    "-" +
    BYTE_HEX[bytes[offset + 6]! & 0xff]! +
    BYTE_HEX[bytes[offset + 7]! & 0xff]! +
    "-" +
    BYTE_HEX[bytes[offset + 8]! & 0xff]! +
    BYTE_HEX[bytes[offset + 9]! & 0xff]! +
    "-" +
    BYTE_HEX[bytes[offset + 10]! & 0xff]! +
    BYTE_HEX[bytes[offset + 11]! & 0xff]! +
    BYTE_HEX[bytes[offset + 12]! & 0xff]! +
    BYTE_HEX[bytes[offset + 13]! & 0xff]! +
    BYTE_HEX[bytes[offset + 14]! & 0xff]! +
    BYTE_HEX[bytes[offset + 15]! & 0xff]!
  );
}
