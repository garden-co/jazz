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
    setJsonSchemaValidator: () => {},
  };
});

// The URL a bundler names for the binary; null for bundlers that leave it to
// wasm-bindgen (browser-worker-assets.ts), set for Metro (its .metro.ts).
const bundler = vi.hoisted(() => ({ wasmUrl: null as string | null, moduleUrlReads: 0 }));
vi.mock("./browser-worker-assets.js", async (importOriginal) => {
  const assets = await importOriginal<typeof import("./browser-worker-assets.js")>();
  return {
    ...assets,
    bundledWasmUrl: () => bundler.wasmUrl,
    browserRuntimeModuleUrl: () => {
      bundler.moduleUrlReads++;
      return assets.browserRuntimeModuleUrl();
    },
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
  bundler.wasmUrl = null;
  bundler.moduleUrlReads = 0;
  delete (globalThis as Record<string, unknown>).__METRO_GLOBAL_PREFIX__;

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
    // An application/wasm response is compiled while streaming; wasm-bindgen
    // then instantiates the compiled module.
    expect(wasmDefaultInit).toHaveBeenCalledWith({
      module_or_path: expect.any(WebAssembly.Module),
    });
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
    // An application/wasm response is compiled while streaming; wasm-bindgen
    // then instantiates the compiled module.
    expect(wasmDefaultInit).toHaveBeenCalledWith({
      module_or_path: expect.any(WebAssembly.Module),
    });
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

  it("loads the binary from the URL a bundler names for it", async () => {
    // Metro with withJazz: the binary is a Metro asset, and wasm-bindgen's
    // own URL for it would be a 404.
    setBrowserLikeProcess();
    (globalThis as Record<string, unknown>).location = { href: "http://localhost:8081/" };
    bundler.wasmUrl = "http://localhost:8081/assets/jazz_wasm_bg.0f1e.wasm";
    serveWasm();

    await loadWasmModule();

    expect(globalThis.fetch).toHaveBeenCalledWith(
      "http://localhost:8081/assets/jazz_wasm_bg.0f1e.wasm",
    );
    expect(wasmDefaultInit).toHaveBeenCalledWith({
      module_or_path: expect.any(WebAssembly.Module),
    });
  });

  it("prefers the app's own runtimeSources over the bundler's URL", async () => {
    setBrowserLikeProcess();
    (globalThis as Record<string, unknown>).location = { href: "http://localhost:8081/" };
    bundler.wasmUrl = "http://localhost:8081/assets/jazz_wasm_bg.0f1e.wasm";
    serveWasm();

    await loadWasmModule({ wasmUrl: "/own/jazz_wasm_bg.wasm", wasmVersion: "deploy-42" });

    expect(globalThis.fetch).toHaveBeenCalledWith(
      "http://localhost:8081/own/jazz_wasm_bg.wasm?jazz-runtime-version=deploy-42",
    );
  });

  it("does not ask where its own module is when it was handed the binary's URL", async () => {
    // The broker worker on Expo web: Expo compiles `import.meta` to a registry
    // that worker chunks do not have, so reading `import.meta.url` there throws.
    setBrowserLikeProcess();
    (globalThis as Record<string, unknown>).location = { href: "http://localhost:8081/" };
    serveWasm();

    await loadWasmModule({ wasmUrl: "http://localhost:8081/assets/jazz_wasm_bg.0f1e.wasm" });
    expect(bundler.moduleUrlReads).toBe(0);

    // Left to find the binary itself, it does need its module's URL.
    await loadWasmModule();
    expect(bundler.moduleUrlReads).toBe(1);
  });

  it("names the Metro setup instead of requesting a binary Metro did not bundle", async () => {
    // Metro without withJazz: wasm-bindgen would ask for the binary beside
    // the bundle, where nothing serves it.
    setBrowserLikeProcess();
    (globalThis as Record<string, unknown>).location = { href: "http://localhost:8081/" };
    (globalThis as Record<string, unknown>).__METRO_GLOBAL_PREFIX__ = "";

    await expect(loadWasmModule()).rejects.toThrow(
      "wrap your Metro config in withJazz from jazz-tools/expo/metro",
    );
    expect(wasmDefaultInit).not.toHaveBeenCalled();
  });
});
