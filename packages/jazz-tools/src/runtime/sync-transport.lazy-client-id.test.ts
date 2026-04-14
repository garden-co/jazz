import { afterEach, describe, expect, it, vi } from "vitest";

const originalCryptoDescriptor = Object.getOwnPropertyDescriptor(globalThis, "crypto");

function restoreGlobalProperty(name: "crypto", descriptor?: PropertyDescriptor): void {
  if (descriptor) {
    Object.defineProperty(globalThis, name, descriptor);
    return;
  }

  delete (globalThis as Record<string, unknown>)[name];
}

afterEach(() => {
  vi.resetModules();
  vi.restoreAllMocks();
  restoreGlobalProperty("crypto", originalCryptoDescriptor);
});

describe("sync-transport lazy client id", () => {
  it("does not generate a client id at import time", async () => {
    const randomUUID = vi.fn(() => "11111111-2222-4333-8444-555555555555");
    Object.defineProperty(globalThis, "crypto", {
      configurable: true,
      value: {
        randomUUID,
      } as unknown as Crypto,
    });

    await import("./sync-transport.js");

    expect(randomUUID).not.toHaveBeenCalled();
  });

  it("generates a client id when generateClientId() is called", async () => {
    const randomUUID = vi.fn(() => "11111111-2222-4333-8444-555555555555");
    Object.defineProperty(globalThis, "crypto", {
      configurable: true,
      value: {
        randomUUID,
      } as unknown as Crypto,
    });

    const { generateClientId } = await import("./sync-transport.js");
    const id = generateClientId();

    expect(randomUUID).toHaveBeenCalledTimes(1);
    expect(id).toBe("11111111-2222-4333-8444-555555555555");
  });
});
