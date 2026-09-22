import type { RuntimeSourcesConfig } from "./context.js";
import {
  resolveRuntimeConfigSyncInitInput,
  resolveRuntimeConfigWasmUrl,
} from "./runtime-config.js";
import { assertNativeArtifactCompatibility } from "./native-artifact-compatibility.js";

/** WASM-only loader, deliberately outside the shared client implementation. */
export type WasmModule = typeof import("jazz-wasm");

async function tryLoadNodePackagedWasmBinary(): Promise<Uint8Array | null> {
  const moduleBuiltin = process.getBuiltinModule?.("module");
  const fsBuiltin = process.getBuiltinModule?.("fs");
  const pathBuiltin = process.getBuiltinModule?.("path");

  if (!moduleBuiltin || !fsBuiltin || !pathBuiltin) return null;

  const { createRequire } = moduleBuiltin;
  const { existsSync, readFileSync } = fsBuiltin;
  const { dirname, resolve } = pathBuiltin;

  const sealedWasmPackage = process.env.JAZZ_CORRECTNESS_WASM_PACKAGE;
  if (process.env.JAZZ_CORRECTNESS_ARTIFACT_RUN === "1" && !sealedWasmPackage)
    throw new Error("sealed correctness consumer is missing its admitted WASM package");
  if (sealedWasmPackage) {
    const wasmPath = resolve(sealedWasmPackage, "jazz_wasm_bg.wasm");
    return existsSync(wasmPath) ? readFileSync(wasmPath) : null;
  }

  const require = createRequire(import.meta.url);
  const packageJsonPath = require.resolve("jazz-wasm/package.json");
  const wasmPath = resolve(dirname(packageJsonPath), "pkg/jazz_wasm_bg.wasm");
  return existsSync(wasmPath) ? readFileSync(wasmPath) : null;
}

let wasmInitializationTail: Promise<void> = Promise.resolve();

/** Load and initialize the browser/Node WASM runtime. */
export function loadWasmModule(runtime?: RuntimeSourcesConfig): Promise<WasmModule> {
  const initialization = wasmInitializationTail.then(() => initializeWasmModule(runtime));
  wasmInitializationTail = initialization.then(
    () => undefined,
    () => undefined,
  );
  return initialization;
}

async function initializeWasmModule(runtime?: RuntimeSourcesConfig): Promise<WasmModule> {
  const wasmModule: any = await import("jazz-wasm");
  const syncInitInput = resolveRuntimeConfigSyncInitInput(runtime);
  if (syncInitInput) {
    wasmModule.initSync(syncInitInput);
    assertNativeArtifactCompatibility(wasmModule, "WASM", ["initSync", "WasmDb"]);
    return wasmModule;
  }

  let nodeInitDone = false;
  if (typeof process !== "undefined" && process.versions?.node) {
    try {
      const wasmBinary = await tryLoadNodePackagedWasmBinary();
      if (wasmBinary) {
        wasmModule.initSync({ module: wasmBinary });
        nodeInitDone = true;
      }
    } catch {
      // Node builtins can be polyfilled but unavailable in browser-like hosts.
    }
  }
  if (!nodeInitDone && typeof wasmModule.default === "function") {
    const wasmUrl =
      typeof location !== "undefined"
        ? resolveRuntimeConfigWasmUrl(import.meta.url, location.href, runtime)
        : null;
    if (wasmUrl) await initializeWasmFromUrl(wasmModule, wasmUrl);
    else await wasmModule.default();
  }

  assertNativeArtifactCompatibility(wasmModule, "WASM", ["initSync", "WasmDb"]);
  return wasmModule;
}

async function initializeWasmFromUrl(wasmModule: any, wasmUrl: string): Promise<void> {
  const response = await fetch(wasmUrl);
  if (!response.ok) {
    throw new Error(
      `WASM asset request failed (${response.status} ${response.statusText}) for ${wasmUrl}`,
    );
  }
  const bytes = new Uint8Array(await response.arrayBuffer());
  if (
    bytes.length < 4 ||
    bytes[0] !== 0x00 ||
    bytes[1] !== 0x61 ||
    bytes[2] !== 0x73 ||
    bytes[3] !== 0x6d
  ) {
    const contentType = response.headers.get("content-type") ?? "unknown content type";
    throw new Error(
      `WASM asset response is not a WebAssembly binary for ${wasmUrl} (${contentType})`,
    );
  }
  await wasmModule.default({ module_or_path: bytes });
}
