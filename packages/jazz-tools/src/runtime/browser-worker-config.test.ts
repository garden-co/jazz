import { afterEach, describe, expect, it, vi } from "vitest";

import {
  createBrowserWorkerAssetScope,
  resolveBrowserWorkerUrl,
  resolveBrowserWorkerRuntimeSources,
  vitePrebundledBrokerWorkerUrl,
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

describe("account storage and principal runtime isolation", () => {
  it("reopens the same account root without sharing live authorization between linked identities", async () => {
    const {
      createBrowserPhysicalDatabaseName,
      createBrowserStorageOwner,
      createBrowserAuthSessionKey,
      createBrowserWorkerFingerprint,
      assertBrowserStorageOwnerUnchanged,
    } = await import("./browser-worker-config.js");
    const jwtToken = (sub: string) =>
      `e30.${btoa(JSON.stringify({ iss: "https://issuer.example", sub }))}.sig`;
    const alice = {
      appId: "account-test",
      accountId: "00000000-0000-4000-8000-000000000001",
      accountRegistryAuthority: "https://core.example/apps/account-test/accounts",
      jwtToken: jwtToken("alice"),
    };
    const linked = { ...alice, jwtToken: jwtToken("alice-linked") };
    const other = { ...alice, accountId: "00000000-0000-4000-8000-000000000002" };
    const otherRegistry = {
      ...alice,
      accountRegistryAuthority: "https://other-core.example/apps/account-test/accounts",
    };
    const name = createBrowserPhysicalDatabaseName(alice, "test");
    expect(createBrowserPhysicalDatabaseName(otherRegistry, "test")).not.toBe(name);
    expect(createBrowserStorageOwner(otherRegistry)).not.toBe(createBrowserStorageOwner(alice));
    expect(createBrowserAuthSessionKey(otherRegistry)).not.toBe(createBrowserAuthSessionKey(alice));
    expect(createBrowserPhysicalDatabaseName(linked, "test")).toBe(name);
    expect(createBrowserStorageOwner(linked)).toBe(createBrowserStorageOwner(alice));
    expect(createBrowserPhysicalDatabaseName(other, "test")).not.toBe(name);
    expect(createBrowserAuthSessionKey(linked)).not.toBe(createBrowserAuthSessionKey(alice));
    expect(createBrowserWorkerFingerprint(linked, name, "schema")).not.toBe(
      createBrowserWorkerFingerprint(alice, name, "schema"),
    );
    expect(() => assertBrowserStorageOwnerUnchanged(alice, linked)).toThrow("Cannot change");
    expect(() => assertBrowserStorageOwnerUnchanged(alice, { ...alice })).not.toThrow();
  });
});

describe("browser storage scope", () => {
  it("opens the same database for `scope: X` as for `appId: X`, and one per endpoint", async () => {
    const {
      createBrowserAuthSessionKey,
      createBrowserPhysicalDatabaseName,
      createBrowserStorageOwner,
    } = await import("./browser-worker-config.js");
    const keys = (config: Record<string, unknown>) => [
      createBrowserPhysicalDatabaseName(config as never, "app"),
      createBrowserStorageOwner(config as never),
      createBrowserAuthSessionKey(config as never),
    ];
    expect(keys({ scope: "legacy-app" })).toEqual(keys({ appId: "legacy-app" }));
    expect(keys({ serverUrl: "https://a.example/jazz" })).not.toEqual(
      keys({ serverUrl: "https://b.example/jazz" }),
    );
  });
});

describe("broker worker URL under the Vite dev server's pre-bundle", () => {
  it.each([
    [
      "the default cache directory",
      "http://localhost:5173/node_modules/.vite/deps/chunk-7ATA2QW3.js?v=7d09ffa4",
      "http://localhost:5173/@id/jazz-tools/_worker/broker",
    ],
    [
      "a configured base",
      "http://localhost:5173/myapp/node_modules/.vite/deps/jazz-tools_local-first.js?v=1",
      "http://localhost:5173/myapp/@id/jazz-tools/_worker/broker",
    ],
    [
      "a per-environment deps directory",
      "https://dev.example:8443/node_modules/.vite/deps_client/chunk.js",
      "https://dev.example:8443/@id/jazz-tools/_worker/broker",
    ],
    [
      "a nested cache directory",
      "http://localhost:63315/node_modules/.vite/vitest/da39a3ee/deps/jazz-tools.js?v=2",
      "http://localhost:63315/@id/jazz-tools/_worker/broker",
    ],
  ])("asks the dev server to resolve the package from %s", (_case, moduleUrl, workerUrl) => {
    expect(vitePrebundledBrokerWorkerUrl(moduleUrl)).toBe(workerUrl);
  });

  it.each([
    // The package's own files: Vite dev with jazz-tools excluded, or unbundled.
    "http://localhost:5173/node_modules/jazz-tools/dist/runtime/browser-worker-assets.js",
    // A workspace-linked package, which Vite serves without pre-bundling.
    "http://localhost:5173/@fs/repo/packages/jazz-tools/dist/runtime/browser-worker-assets.js",
    // Production chunks of any bundler.
    "https://app.example/assets/index-Wj-y1hYG.js",
    "https://app.example/_next/static/chunks/app-pages-browser.js",
    // Only Vite's deps directory, not a file that merely mentions it.
    "https://app.example/node_modules/.vite/not-deps/chunk.js",
    "https://app.example/assets/chunk.js?from=/node_modules/.vite/deps/x.js",
  ])("leaves %s to the bundler's worker URL", (moduleUrl) => {
    expect(vitePrebundledBrokerWorkerUrl(moduleUrl)).toBeUndefined();
  });

  it("names the pre-bundle worker URL in the worker realm's identity", async () => {
    vi.resetModules();
    vi.doMock("./browser-worker-assets.js", () => ({
      browserRuntimeModuleUrl: () =>
        "http://localhost:5173/node_modules/.vite/deps/chunk-7ATA2QW3.js?v=7d09ffa4",
      bundledBrowserWorkerUrl: () =>
        "http://localhost:5173/node_modules/worker/jazz-broker-worker.js",
    }));
    try {
      const config = await import("./browser-worker-config.js");
      expect(config.resolveBrowserWorkerUrl()).toBe(
        "http://localhost:5173/@id/jazz-tools/_worker/broker",
      );
      expect(config.resolveBrowserWorkerUrl({ wasmVersion: "build-1" })).toBe(
        "http://localhost:5173/@id/jazz-tools/_worker/broker?jazz-runtime-version=build-1",
      );
      // Explicitly configured assets still win.
      expect(
        config.resolveBrowserWorkerUrl({
          brokerWorkerUrl: "https://cdn.example/jazz-broker-worker.js",
          wasmVersion: "build-1",
        }),
      ).toBe("https://cdn.example/jazz-broker-worker.js?jazz-runtime-version=build-1");
    } finally {
      vi.doUnmock("./browser-worker-assets.js");
      vi.resetModules();
    }
  });
});
