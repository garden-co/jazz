import { afterEach, describe, expect, it, vi } from "vitest";

const wasmDefaultInit = vi.fn<() => Promise<void>>().mockResolvedValue(undefined);
const wasmInitSync = vi.fn();
const wasmBinary = new Uint8Array([0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00]);
const compiledWasmModule = new WebAssembly.Module(wasmBinary);

vi.mock("jazz-wasm", () => ({
  default: wasmDefaultInit,
  initSync: wasmInitSync,
}));

import { loadWasmModule } from "./client.js";

const originalProcess = globalThis.process;
const originalLocation = globalThis.location;

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
});

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

    await loadWasmModule({
      wasmUrl: "/custom/jazz/jazz_wasm_bg.wasm",
      baseUrl: "/ignored/",
    });

    expect(wasmDefaultInit).toHaveBeenCalledTimes(1);
    expect(wasmDefaultInit).toHaveBeenCalledWith({
      module_or_path: "http://localhost:3000/custom/jazz/jazz_wasm_bg.wasm",
    });
  });

  it("derives the wasm URL from runtimeSources.baseUrl when wasmUrl is omitted", async () => {
    setBrowserLikeProcess();
    (globalThis as Record<string, unknown>).location = {
      href: "http://localhost:3000/app/",
    };

    await loadWasmModule({
      baseUrl: "/assets/jazz/",
    });

    expect(wasmDefaultInit).toHaveBeenCalledTimes(1);
    expect(wasmDefaultInit).toHaveBeenCalledWith({
      module_or_path: "http://localhost:3000/assets/jazz/jazz_wasm_bg.wasm",
    });
  });

  it("uses an HTTP wasm URL when the runtime module itself is loaded from file://", async () => {
    setBrowserLikeProcess();
    (globalThis as Record<string, unknown>).location = {
      href: "http://localhost:3000/",
    };

    await loadWasmModule();

    expect(wasmDefaultInit).toHaveBeenCalledTimes(1);
    expect(wasmDefaultInit).toHaveBeenCalledWith({
      module_or_path: "http://localhost:3000/jazz_wasm_bg.wasm",
    });
  });
});
