import { beforeEach, describe, expect, it, vi } from "vitest";

const { mockImportJazzRn } = vi.hoisted(() => ({ mockImportJazzRn: vi.fn() }));

vi.mock("./jazz-rn-importer.js", () => ({ importJazzRn: mockImportJazzRn }));

beforeEach(() => {
  mockImportJazzRn.mockReset();
  vi.resetModules();
});

function moduleNotFound(code: string, specifier = "jazz-rn"): Error & { code: string } {
  const err = new Error(`Cannot find module '${specifier}'`) as Error & {
    code: string;
  };
  err.code = code;
  return err;
}

describe("loadJazzRn", () => {
  it("RNLD-U01 throws a friendly peer-dep error when jazz-rn cannot be resolved", async () => {
    mockImportJazzRn.mockImplementation(() =>
      Promise.reject(moduleNotFound("ERR_MODULE_NOT_FOUND")),
    );

    const { loadJazzRn } = await import("./jazz-rn-loader.js");
    await expect(loadJazzRn()).rejects.toThrow(/jazz-rn.*peer dependency/s);
    await expect(loadJazzRn()).rejects.toThrow(/npm install jazz-rn/);
  });

  it("RNLD-U02 attaches the original error as cause when peer dep is missing", async () => {
    const original = moduleNotFound("MODULE_NOT_FOUND");
    mockImportJazzRn.mockImplementation(() => Promise.reject(original));

    const { loadJazzRn } = await import("./jazz-rn-loader.js");
    try {
      await loadJazzRn();
      expect.fail("expected loadJazzRn to throw");
    } catch (err) {
      expect((err as Error & { cause?: unknown }).cause).toBe(original);
    }
  });

  it("RNLD-U03 rethrows non-resolution errors without rebranding", async () => {
    const initFailure = new Error("native binding crashed during evaluation");
    mockImportJazzRn.mockImplementation(() => Promise.reject(initFailure));

    const { loadJazzRn } = await import("./jazz-rn-loader.js");
    await expect(loadJazzRn()).rejects.toBe(initFailure);
  });

  it("RNLD-U03b rethrows when a transitive dep of jazz-rn is missing", async () => {
    // jazz-rn itself resolved, but it imports something that doesn't exist.
    // The original error must surface so users see the real missing module.
    const transitive = moduleNotFound("ERR_MODULE_NOT_FOUND", "react-native-mmkv");
    mockImportJazzRn.mockImplementation(() => Promise.reject(transitive));

    const { loadJazzRn } = await import("./jazz-rn-loader.js");
    await expect(loadJazzRn()).rejects.toBe(transitive);
  });

  it("RNLD-U04 caches and returns the default export on success", async () => {
    const fakeJazzRn = { jazz_rn: { mintLocalFirstToken: vi.fn(() => "fake-token") } };
    mockImportJazzRn.mockResolvedValue({ default: fakeJazzRn } as never);

    const { loadJazzRn } = await import("./jazz-rn-loader.js");
    const first = await loadJazzRn();
    const second = await loadJazzRn();
    expect(first).toBe(fakeJazzRn);
    expect(second).toBe(fakeJazzRn);
    expect(mockImportJazzRn).toHaveBeenCalledTimes(1);
  });
});

describe("getJazzRnSync", () => {
  it("RNLD-U05 throws a clear error when called before loadJazzRn populates the cache", async () => {
    const { getJazzRnSync } = await import("./jazz-rn-loader.js");
    expect(() => getJazzRnSync()).toThrow(/accessed before it was loaded/);
    expect(() => getJazzRnSync()).toThrow(/createDb\(\)|createJazzClient\(\)|loadJazzRn/);
  });

  it("RNLD-U06 returns the cached module after loadJazzRn succeeds", async () => {
    const fakeJazzRn = { jazz_rn: { mintLocalFirstToken: vi.fn() } };
    mockImportJazzRn.mockResolvedValue({ default: fakeJazzRn } as never);

    const { loadJazzRn, getJazzRnSync } = await import("./jazz-rn-loader.js");
    await loadJazzRn();
    expect(getJazzRnSync()).toBe(fakeJazzRn);
  });
});
