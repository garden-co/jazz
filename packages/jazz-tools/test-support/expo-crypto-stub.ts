import { randomBytes } from "node:crypto";

export function getRandomBytes(byteCount: number): Uint8Array {
  return new Uint8Array(randomBytes(byteCount));
}

export function getRandomValues<T extends Uint8Array>(typedArray: T): T {
  typedArray.set(randomBytes(typedArray.byteLength));
  return typedArray;
}
