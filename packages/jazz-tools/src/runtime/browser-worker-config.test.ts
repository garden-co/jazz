import { afterEach, describe, expect, it, vi } from "vitest";

import {
  createBrowserWorkerAssetScope,
  resolveBrowserWorkerUrl,
  resolveBrowserWorkerRuntimeSources,
} from "./browser-worker-config.js";

const originalLocation = globalThis.location;

afterEach(() => {
  if (originalLocation === undefined) {
    delete (globalThis as Record<string, unknown>).location;
  } else {
    (globalThis as Record<string, unknown>).location = originalLocation;
  }
});

describe("browser SharedWorker asset handoff", () => {
  it("keeps page-relative runtime assets isolated across Vite origins", () => {
    const runtimeSources = { baseUrl: "/assets/jazz/", wasmVersion: "vite-build" };

    (globalThis as Record<string, unknown>).location = {
      href: "http://vite-first.test:5173/specimen/",
    };
    const first = resolveBrowserWorkerRuntimeSources(runtimeSources);
    const firstScope = createBrowserWorkerAssetScope(runtimeSources);

    (globalThis as Record<string, unknown>).location = {
      href: "http://vite-second.test:5174/specimen/",
    };
    const second = resolveBrowserWorkerRuntimeSources(runtimeSources);
    const secondScope = createBrowserWorkerAssetScope(runtimeSources);

    expect(first?.wasmUrl).toBe(
      "http://vite-first.test:5173/assets/jazz/jazz_wasm_bg.wasm?jazz-runtime-version=vite-build",
    );
    expect(second?.wasmUrl).toBe(
      "http://vite-second.test:5174/assets/jazz/jazz_wasm_bg.wasm?jazz-runtime-version=vite-build",
    );
    expect(firstScope).not.toBe(secondScope);
  });

  it("gives distinct supplied byte arrays distinct worker identities", () => {
    const firstBytes = new Uint8Array([0, 97, 115, 109]);
    const secondBytes = new Uint8Array([0, 97, 115, 109]);

    const first = resolveBrowserWorkerRuntimeSources({ wasmSource: firstBytes });
    const repeatedFirst = resolveBrowserWorkerRuntimeSources({ wasmSource: firstBytes });
    const second = resolveBrowserWorkerRuntimeSources({ wasmSource: secondBytes });

    expect(first?.workerWasmAssetIdentity).toBe(repeatedFirst?.workerWasmAssetIdentity);
    expect(first?.workerWasmAssetIdentity).not.toBe(second?.workerWasmAssetIdentity);
    expect(createBrowserWorkerAssetScope({ wasmSource: firstBytes })).not.toBe(
      createBrowserWorkerAssetScope({ wasmSource: secondBytes }),
    );
  });

  it("uses random opaque identities for source and module inputs across realms", async () => {
    const wasmBytes = new Uint8Array([0, 97, 115, 109, 1, 0, 0, 0]);
    const wasmModule = new WebAssembly.Module(wasmBytes);

    const sourceIdentity = resolveBrowserWorkerRuntimeSources({
      wasmSource: wasmBytes,
    })?.workerWasmAssetIdentity;
    const repeatedSourceIdentity = resolveBrowserWorkerRuntimeSources({
      wasmSource: wasmBytes,
    })?.workerWasmAssetIdentity;
    const moduleIdentity = resolveBrowserWorkerRuntimeSources({
      wasmModule,
    })?.workerWasmAssetIdentity;

    expect(sourceIdentity).toMatch(/^source:[0-9a-f-]{36}$/);
    expect(sourceIdentity).toBe(repeatedSourceIdentity);
    expect(moduleIdentity).toMatch(/^module:[0-9a-f-]{36}$/);

    vi.resetModules();
    const freshRealm = await import("./browser-worker-config.js");
    const freshRealmIdentity = freshRealm.resolveBrowserWorkerRuntimeSources({
      wasmSource: wasmBytes,
    })?.workerWasmAssetIdentity;

    expect(freshRealmIdentity).toMatch(/^source:[0-9a-f-]{36}$/);
    expect(freshRealmIdentity).not.toBe(sourceIdentity);
  });

  it("does not collapse distinct assets that collide under the retired 32-bit scope hash", () => {
    const first = {
      wasmUrl: "https://assets.test/jazz_wasm_bg.wasm",
      brokerWorkerUrl: "https://assets.test/worker/jazz-broker-worker.js",
      wasmVersion: "build-2826",
    };
    const second = {
      wasmUrl: "https://assets.test/jazz_wasm_bg.wasm",
      brokerWorkerUrl: "https://assets.test/worker/jazz-broker-worker.js",
      wasmVersion: "build-290d",
    };

    // These full canonical identities have the same FNV-1a 32-bit hash. The
    // worker scope retains the identity itself, so the collision cannot alias
    // their process-global wasm-bindgen initialization.
    expect(retiredScopeHash(createBrowserWorkerAssetScope(first))).toBe(
      retiredScopeHash(createBrowserWorkerAssetScope(second)),
    );
    expect(createBrowserWorkerAssetScope(first)).not.toBe(createBrowserWorkerAssetScope(second));
  });

  it("requires an immutable version when browser asset URLs are configured", () => {
    expect(() => resolveBrowserWorkerRuntimeSources({ wasmUrl: "/assets/jazz.wasm" })).toThrow(
      "runtimeSources.wasmVersion",
    );
  });

  it("uses the immutable version for both configured WASM and worker URLs", () => {
    const runtimeSources = {
      baseUrl: "https://assets.test/jazz/",
      wasmVersion: "deploy-42",
    };

    expect(resolveBrowserWorkerRuntimeSources(runtimeSources)?.wasmUrl).toBe(
      "https://assets.test/jazz/jazz_wasm_bg.wasm?jazz-runtime-version=deploy-42",
    );
    expect(resolveBrowserWorkerUrl(runtimeSources)).toBe(
      "https://assets.test/jazz/worker/jazz-broker-worker.js?jazz-runtime-version=deploy-42",
    );
  });

  it("versions the bundled worker when only an explicit WASM URL is configured", () => {
    const workerUrl = resolveBrowserWorkerUrl({
      wasmUrl: "https://assets.test/jazz_wasm_bg.wasm",
      wasmVersion: "deploy-42",
    });

    expect(new URL(workerUrl).searchParams.get("jazz-runtime-version")).toBe("deploy-42");
  });
});

function retiredScopeHash(value: string): string {
  let hash = 0x811c9dc5;
  for (let index = 0; index < value.length; index += 1) {
    hash ^= value.charCodeAt(index);
    hash = Math.imul(hash, 0x01000193);
  }
  return (hash >>> 0).toString(16).padStart(8, "0");
}
