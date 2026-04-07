import { afterEach, describe, expect, it, vi } from "vitest";

const wasmDefaultInit = vi.fn<() => Promise<void>>().mockResolvedValue(undefined);
const wasmInitSync = vi.fn();

vi.mock("jazz-wasm", () => ({
  default: wasmDefaultInit,
  initSync: wasmInitSync,
}));

import { loadWasmModule } from "./client.js";

const originalLocation = globalThis.location;

afterEach(() => {
  wasmDefaultInit.mockClear();
  wasmInitSync.mockClear();

  if (originalLocation === undefined) {
    delete (globalThis as Record<string, unknown>).location;
  } else {
    (globalThis as Record<string, unknown>).location = originalLocation;
  }
});

describe("loadWasmModule node packaged bootstrap", () => {
  it("prefers packaged wasm bytes over fetch-based init in node", async () => {
    if (originalLocation !== undefined) {
      delete (globalThis as Record<string, unknown>).location;
    }

    await loadWasmModule();

    expect(wasmInitSync).toHaveBeenCalledTimes(1);
    expect(wasmInitSync).toHaveBeenCalledWith({
      module: expect.any(Uint8Array),
    });
    expect(wasmDefaultInit).not.toHaveBeenCalled();
  });
});
