import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const { initialize, assertCompatible } = vi.hoisted(() => ({
  initialize: vi.fn(),
  assertCompatible: vi.fn(),
}));

vi.mock("jazz-wasm", () => ({ default: initialize }));
vi.mock("./native-artifact-compatibility.js", () => ({
  assertNativeArtifactCompatibility: assertCompatible,
}));

const wasmBinary = new Uint8Array([0, 0x61, 0x73, 0x6d, 1, 0, 0, 0]);
const runtime = { baseUrl: "/assets/jazz/", wasmVersion: "test-build" };
let loadWasmModule: typeof import("./wasm-loader.js").loadWasmModule;
let fetchWasm: ReturnType<typeof vi.fn<typeof fetch>>;

beforeEach(async () => {
  vi.resetModules();
  initialize.mockReset().mockResolvedValue(undefined);
  assertCompatible.mockReset();
  ({ loadWasmModule } = await import("./wasm-loader.js"));
  vi.stubGlobal("process", { ...process, versions: {} });
  vi.stubGlobal("location", { href: "https://example.com/app/" });
  fetchWasm = vi.fn<typeof fetch>().mockImplementation(async () => new Response(wasmBinary));
  vi.stubGlobal("fetch", fetchWasm);
});

afterEach(() => vi.unstubAllGlobals());

describe("browser WASM URL initialization", () => {
  it("shares one fetch and initialization across concurrent and later callers", async () => {
    const [first, second] = await Promise.all([loadWasmModule(runtime), loadWasmModule(runtime)]);
    const third = await loadWasmModule(runtime);

    expect(first).toBe(second);
    expect(third).toBe(first);
    expect(fetchWasm).toHaveBeenCalledTimes(1);
    expect(initialize).toHaveBeenCalledTimes(1);
    expect(initialize).toHaveBeenCalledWith({ module_or_path: wasmBinary });
    expect(assertCompatible).toHaveBeenCalledTimes(3);
  });

  it("deduplicates equivalent resolved URLs", async () => {
    await loadWasmModule(runtime);
    await loadWasmModule({
      wasmUrl: "https://example.com/assets/jazz/jazz_wasm_bg.wasm",
      wasmVersion: runtime.wasmVersion,
    });
    expect(fetchWasm).toHaveBeenCalledTimes(1);
  });

  it("still loads changed URLs and versions", async () => {
    await loadWasmModule(runtime);
    await loadWasmModule({ ...runtime, wasmVersion: "next-build" });
    await loadWasmModule({ ...runtime, baseUrl: "/other-assets/" });
    expect(fetchWasm).toHaveBeenCalledTimes(3);
    expect(initialize).toHaveBeenCalledTimes(3);
  });

  it.each(["network", "http", "html", "initialization"])(
    "retries after a %s failure, including an already queued caller",
    async (failure) => {
      if (failure === "network") fetchWasm.mockRejectedValueOnce(new Error("Network unavailable"));
      if (failure === "http")
        fetchWasm.mockResolvedValueOnce(new Response(null, { status: 503 }));
      if (failure === "html") fetchWasm.mockResolvedValueOnce(new Response("<!doctype html>"));
      if (failure === "initialization") initialize.mockRejectedValueOnce(new Error("Init failed"));

      const first = loadWasmModule(runtime);
      const retry = loadWasmModule(runtime);
      await expect(first).rejects.toThrow();
      await expect(retry).resolves.toBeDefined();
      await loadWasmModule(runtime);
      expect(fetchWasm).toHaveBeenCalledTimes(2);
      expect(initialize).toHaveBeenCalledTimes(failure === "initialization" ? 2 : 1);
    },
  );

  it("still validates artifact compatibility on a cache hit", async () => {
    await loadWasmModule(runtime);
    assertCompatible.mockImplementationOnce(() => {
      throw new Error("Incompatible artifact");
    });
    await expect(loadWasmModule(runtime)).rejects.toThrow("Incompatible artifact");
    expect(fetchWasm).toHaveBeenCalledTimes(1);
  });
});
