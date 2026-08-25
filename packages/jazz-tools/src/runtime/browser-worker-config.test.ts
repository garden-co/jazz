import { afterEach, describe, expect, it } from "vitest";

import {
  createBrowserWorkerAssetScope,
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
    const runtimeSources = { baseUrl: "/assets/jazz/" };

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

    expect(first?.wasmUrl).toBe("http://vite-first.test:5173/assets/jazz/jazz_wasm_bg.wasm");
    expect(second?.wasmUrl).toBe("http://vite-second.test:5174/assets/jazz/jazz_wasm_bg.wasm");
    expect(firstScope).not.toBe(secondScope);
  });
});
