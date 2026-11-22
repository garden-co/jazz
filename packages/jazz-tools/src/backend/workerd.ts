/**
 * `jazz-tools/backend` in a Cloudflare Worker (the `workerd` export
 * condition, which wrangler sets): the same API as under Node, on jazz-wasm
 * instead of jazz-napi, which workerd can't load: its backend build
 * (`jazz-wasm/backend`, which the browser build leaves out), initialised
 * from its statically imported module, which wrangler bundles as a
 * CompiledWasm module.
 * Only `durableObjects()` stores run here; `createJazz` runs in the app's
 * backend Durable Object (`jazzBackend`).
 */
import * as wasm from "jazz-wasm/backend";
import { provideBackendNative, type BackendNative } from "./backend-native.js";

provideBackendNative(wasmBackendNative(wasm));

export * from "./api.js";
export {
  jazzBackend,
  routeToJazzBackend,
  serveJazz,
  type JazzBackendApp,
} from "./workerd-backend.js";

/** jazz-wasm's backend build as a backend's engine. */
function wasmBackendNative(module: typeof wasm): BackendNative {
  const unsupported = (what: string) => () => {
    throw new Error(`${what} is not available in a Cloudflare Worker`);
  };
  return {
    host: "workerd",
    Db: module.WasmDb as unknown as BackendNative["Db"],
    ThinRouter: module.WasmThinRouter as BackendNative["ThinRouter"],
    admitPrelude: module.admitPrelude as BackendNative["admitPrelude"],
    // jazz-wasm answers undefined where jazz-napi answers null.
    pinPreludeBundle: (prelude, bundle) =>
      module.pinPreludeBundle(prelude, bundle ?? undefined) ?? null,
    captureTextAnchor: module.captureTextAnchor as unknown as BackendNative["captureTextAnchor"],
    captureTextRange: module.captureTextRange as unknown as BackendNative["captureTextRange"],
    resolveTextAnchor: module.resolveTextAnchor as unknown as BackendNative["resolveTextAnchor"],
    resolveTextRange: module.resolveTextRange as unknown as BackendNative["resolveTextRange"],
    codegen: {
      loadBackendApp: module.loadBackendApp,
      // Code generation is a build step: `jazz-tools generate`, not a Worker's.
      generate: unsupported("Code generation"),
    },
    importPeer: async (peer) => {
      throw new Error(`${peer} is not available in a Cloudflare Worker`);
    },
  };
}
