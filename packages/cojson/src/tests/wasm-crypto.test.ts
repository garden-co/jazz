import { add } from "jazz-crypto-rs/pkg-node/jazz_crypto_rs";
import { describe, expect, test } from "vitest";

describe("WASM Crypto", () => {
  test("should add numbers correctly", () => {
    const result = add(2n, 3n);
    expect(result).toBe(5n);
  });
});
