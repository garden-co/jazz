import { beforeEach, describe, expect, it, vi } from "vitest";

const { mockImportJazzWasm } = vi.hoisted(() => ({ mockImportJazzWasm: vi.fn() }));

vi.mock("./wasm-importer.js", () => ({ importJazzWasm: mockImportJazzWasm }));

import { loadWasmModule } from "./client.js";

beforeEach(() => {
  mockImportJazzWasm.mockReset();
});

function moduleNotFound(code: string): Error & { code: string } {
  const err = new Error("Cannot find module 'jazz-wasm'") as Error & { code: string };
  err.code = code;
  return err;
}

describe("loadWasmModule peer-dep handling", () => {
  it("WASM-U01 throws a friendly peer-dep error when jazz-wasm cannot be resolved", async () => {
    mockImportJazzWasm.mockImplementation(() =>
      Promise.reject(moduleNotFound("ERR_MODULE_NOT_FOUND")),
    );

    await expect(loadWasmModule()).rejects.toThrow(/jazz-wasm.*peer dependency/s);
    await expect(loadWasmModule()).rejects.toThrow(/npm install jazz-wasm/);
  });

  it("WASM-U02 attaches the original error as cause when peer dep is missing", async () => {
    const original = moduleNotFound("MODULE_NOT_FOUND");
    mockImportJazzWasm.mockImplementation(() => Promise.reject(original));

    try {
      await loadWasmModule();
      expect.fail("expected loadWasmModule to throw");
    } catch (err) {
      expect((err as Error & { cause?: unknown }).cause).toBe(original);
    }
  });

  it("WASM-U03 rethrows non-resolution errors without rebranding", async () => {
    const initFailure = new Error("wasm-bindgen glue failed to evaluate");
    mockImportJazzWasm.mockImplementation(() => Promise.reject(initFailure));

    await expect(loadWasmModule()).rejects.toBe(initFailure);
  });
});
