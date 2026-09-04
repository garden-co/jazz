import { afterEach, describe, expect, it, vi } from "vitest";

const wasmDefaultInit = vi.fn<(input?: unknown) => Promise<void>>().mockResolvedValue(undefined);
const wasmInitSync = vi.fn();
const wasmBinary = new Uint8Array([0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]);
const compiledWasmModule = new WebAssembly.Module(wasmBinary);

vi.mock("jazz-wasm", async () => {
  const { EXPECTED_NATIVE_ARTIFACT_FINGERPRINTS } =
    await import("./native-artifact-fingerprints.js");
  return {
    default: wasmDefaultInit,
    initSync: wasmInitSync,
    nativeArtifactFingerprint: () => EXPECTED_NATIVE_ARTIFACT_FINGERPRINTS.wasm,
    WasmDb: class {},
  };
});

import { loadWasmModule } from "./wasm-loader.js";

const originalProcess = globalThis.process;
const originalLocation = globalThis.location;
const originalFetch = globalThis.fetch;

function setBrowserLikeProcess(): void {
  (globalThis as Record<string, unknown>).process = {
    versions: {},
  };
}

afterEach(() => {
  wasmDefaultInit.mockClear();
  wasmInitSync.mockClear();

  if (originalProcess === undefined) {
    delete (globalThis as Record<string, unknown>).process;
  } else {
    (globalThis as Record<string, unknown>).process = originalProcess;
  }

  if (originalLocation === undefined) {
    delete (globalThis as Record<string, unknown>).location;
  } else {
    (globalThis as Record<string, unknown>).location = originalLocation;
  }

  if (originalFetch === undefined) {
    delete (globalThis as Record<string, unknown>).fetch;
  } else {
    globalThis.fetch = originalFetch;
  }
});

function serveWasm(bytes = wasmBinary): void {
  globalThis.fetch = vi.fn(
    async () => new Response(bytes, { headers: { "content-type": "application/wasm" } }),
  );
}

describe("loadWasmModule runtimeSources bootstrap", () => {
  it("prefers runtimeSources.wasmModule over URL-based init", async () => {
    setBrowserLikeProcess();
    (globalThis as Record<string, unknown>).location = {
      href: "http://localhost:3000/app/",
    };

    await loadWasmModule({
      wasmModule: compiledWasmModule,
      wasmUrl: "/ignored/jazz_wasm_bg.wasm",
    });

    expect(wasmInitSync).toHaveBeenCalledTimes(1);
    expect(wasmInitSync).toHaveBeenCalledWith({ module: compiledWasmModule });
    expect(wasmDefaultInit).not.toHaveBeenCalled();
  });

  it("prefers runtimeSources.wasmSource over URL-based init when wasmModule is absent", async () => {
    setBrowserLikeProcess();
    (globalThis as Record<string, unknown>).location = {
      href: "http://localhost:3000/app/",
    };

    await loadWasmModule({
      wasmSource: wasmBinary,
      wasmUrl: "/ignored/jazz_wasm_bg.wasm",
    });

    expect(wasmInitSync).toHaveBeenCalledTimes(1);
    expect(wasmInitSync).toHaveBeenCalledWith({ module: wasmBinary });
    expect(wasmDefaultInit).not.toHaveBeenCalled();
  });

  it("prefers an explicit runtimeSources.wasmUrl over the root-relative fallback", async () => {
    setBrowserLikeProcess();
    (globalThis as Record<string, unknown>).location = {
      href: "http://localhost:3000/app/",
    };
    serveWasm();

    await loadWasmModule({
      wasmUrl: "/custom/jazz/jazz_wasm_bg.wasm",
      baseUrl: "/ignored/",
      wasmVersion: "deploy-42",
    });

    expect(wasmDefaultInit).toHaveBeenCalledTimes(1);
    expect(globalThis.fetch).toHaveBeenCalledWith(
      "http://localhost:3000/custom/jazz/jazz_wasm_bg.wasm?jazz-runtime-version=deploy-42",
    );
    expect(wasmDefaultInit).toHaveBeenCalledWith({ module_or_path: expect.any(Uint8Array) });
  });

  it("derives the wasm URL from runtimeSources.baseUrl when wasmUrl is omitted", async () => {
    setBrowserLikeProcess();
    (globalThis as Record<string, unknown>).location = {
      href: "http://localhost:3000/app/",
    };
    serveWasm();

    await loadWasmModule({
      baseUrl: "/assets/jazz/",
      wasmVersion: "deploy-42",
    });

    expect(wasmDefaultInit).toHaveBeenCalledTimes(1);
    expect(globalThis.fetch).toHaveBeenCalledWith(
      "http://localhost:3000/assets/jazz/jazz_wasm_bg.wasm?jazz-runtime-version=deploy-42",
    );
    expect(wasmDefaultInit).toHaveBeenCalledWith({ module_or_path: expect.any(Uint8Array) });
  });

  it("rejects an HTML fallback before wasm-bindgen attempts instantiation", async () => {
    setBrowserLikeProcess();
    (globalThis as Record<string, unknown>).location = {
      href: "http://localhost:3000/app/",
    };
    globalThis.fetch = vi.fn(
      async () =>
        new Response("<!doctype html><title>Vite fallback</title>", {
          headers: { "content-type": "text/html" },
        }),
    );

    await expect(loadWasmModule({ wasmUrl: "/assets/jazz_wasm_bg.wasm" })).rejects.toThrow(
      "WASM asset response is not a WebAssembly binary for http://localhost:3000/assets/jazz_wasm_bg.wasm (text/html)",
    );
    expect(wasmDefaultInit).not.toHaveBeenCalled();
  });

  it("lets wasm-bindgen self-resolve the URL when the page is web-hosted and module is file://", async () => {
    // Covers bundlers (Turbopack, webpack) that compile import.meta.url of client.ts
    // to a file:// URL in the browser bundle. The bundler already bakes the correct
    // asset URL into jazz_wasm.js, so we must not override it.
    setBrowserLikeProcess();
    (globalThis as Record<string, unknown>).location = {
      href: "http://localhost:3000/",
    };

    await loadWasmModule();

    expect(wasmDefaultInit).toHaveBeenCalledTimes(1);
    expect(wasmDefaultInit).toHaveBeenCalledWith();
  });
});
