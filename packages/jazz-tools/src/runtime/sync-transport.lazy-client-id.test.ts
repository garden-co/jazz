import { afterEach, describe, expect, it, vi } from "vitest";

const originalCryptoDescriptor = Object.getOwnPropertyDescriptor(globalThis, "crypto");
const originalFetchDescriptor = Object.getOwnPropertyDescriptor(globalThis, "fetch");

function restoreGlobalProperty(name: "crypto" | "fetch", descriptor?: PropertyDescriptor): void {
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
  restoreGlobalProperty("fetch", originalFetchDescriptor);
});

describe("sync-transport fallback client id", () => {
  it("does not generate a fallback client id at import time", async () => {
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

  it("generates the fallback client id lazily on first sync send", async () => {
    const randomUUID = vi.fn(() => "11111111-2222-4333-8444-555555555555");
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, statusText: "OK" });

    Object.defineProperty(globalThis, "crypto", {
      configurable: true,
      value: {
        randomUUID,
      } as unknown as Crypto,
    });
    Object.defineProperty(globalThis, "fetch", {
      configurable: true,
      value: fetchMock,
    });

    const { sendSyncPayload } = await import("./sync-transport.js");

    await sendSyncPayload("http://localhost:3000", JSON.stringify({ Ping: {} }), false, {});

    expect(randomUUID).toHaveBeenCalledTimes(1);
    expect(JSON.parse(fetchMock.mock.calls[0]![1].body as string).client_id).toBe(
      "11111111-2222-4333-8444-555555555555",
    );
  });
});
