import { afterEach, describe, expect, it } from "vitest";

const originalCryptoDescriptor = Object.getOwnPropertyDescriptor(globalThis, "crypto");

function restoreCrypto() {
  if (originalCryptoDescriptor) {
    Object.defineProperty(globalThis, "crypto", originalCryptoDescriptor);
  } else {
    Reflect.deleteProperty(globalThis, "crypto");
  }
}

describe("expo polyfills", () => {
  afterEach(() => {
    restoreCrypto();
  });

  it("installs crypto.getRandomValues when React Native does not provide crypto", async () => {
    Reflect.deleteProperty(globalThis, "crypto");

    await import("./polyfills.js");

    const bytes = new Uint8Array(8);
    const result = globalThis.crypto.getRandomValues(bytes);

    expect(result).toBe(bytes);
    expect(bytes.some((byte) => byte !== 0)).toBe(true);
  });
});
