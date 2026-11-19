/**
 * `jazz-tools/backend` in a Cloudflare Worker (the `workerd` export
 * condition, which wrangler sets): the same API as under Node, on jazz-wasm
 * instead of jazz-napi, which workerd can't load. The wasm module is
 * imported statically, so wrangler bundles it as a CompiledWasm module.
 * Only `durableObjects()` stores run here; `createJazz` runs in the app's
 * backend Durable Object (`jazzBackend`).
 */
import * as wasm from "jazz-wasm";
import wasmModule from "jazz-wasm/pkg/jazz_wasm_bg.wasm";
import { provideBackendNative, type BackendNative } from "./backend-native.js";

wasm.initSync({ module: wasmModule });
provideBackendNative(wasmBackendNative(wasm));

export * from "./api.js";
export {
  jazzBackend,
  routeToJazzBackend,
  serveJazz,
  type JazzBackendApp,
} from "./workerd-backend.js";

/** jazz-wasm as a backend's engine. */
function wasmBackendNative(module: typeof wasm): BackendNative {
  // Exports jazz-wasm grows for backends; typed loosely until its typings have them.
  const loose = module as unknown as Record<string, unknown>;
  const unsupported = (what: string) => () => {
    throw new Error(`${what} is not available in a Cloudflare Worker`);
  };
  return {
    host: "workerd",
    Db: module.WasmDb as unknown as BackendNative["Db"],
    ThinRouter: loose.WasmThinRouter as BackendNative["ThinRouter"],
    captureTextAnchor: module.captureTextAnchor as unknown as BackendNative["captureTextAnchor"],
    captureTextRange: module.captureTextRange as unknown as BackendNative["captureTextRange"],
    resolveTextAnchor: module.resolveTextAnchor as unknown as BackendNative["resolveTextAnchor"],
    resolveTextRange: module.resolveTextRange as unknown as BackendNative["resolveTextRange"],
    codegen: {
      loadBackendApp: loose.loadBackendApp,
      // Code generation is a build step: `jazz-tools generate`, not a Worker's.
      generate: unsupported("Code generation"),
    },
    importPeer: async (peer) => {
      throw new Error(`${peer} is not available in a Cloudflare Worker`);
    },
  };
}
