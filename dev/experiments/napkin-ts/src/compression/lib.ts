// Browser build: lz4 and native zstd are not compiled in; zstd decode uses
// the pure decoder (Rust: ruzstd, here: fzstd).
import { decompress as zstdDecompress } from "fzstd";

export * as stream from "./stream.js";

export function compressLz4(_payload: Uint8Array): Uint8Array {
  throw new Error("lz4 transport compression feature is not compiled in");
}

export function decompressLz4(_payload: Uint8Array, _maxDecodedLen: number): Uint8Array {
  throw new Error("lz4 transport compression feature is not compiled in");
}

export function compressZstd(_payload: Uint8Array): Uint8Array {
  throw new Error("zstd transport compression feature is not compiled in");
}

export function decompressZstd(payload: Uint8Array, maxDecodedLen: number): Uint8Array {
  let output: Uint8Array;
  try {
    output = zstdDecompress(payload);
  } catch (e) {
    throw new Error(`failed to decompress ruzstd payload: ${(e as Error).message}`);
  }
  validateDecodedLen(output.length, maxDecodedLen);
  return output;
}

function validateDecodedLen(len: number, maxDecodedLen: number): void {
  if (len > maxDecodedLen)
    throw new Error(`logical message payload size ${len} exceeds max ${maxDecodedLen}`);
}
