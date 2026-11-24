/**
 * `jazz-tools/backend` in a Cloudflare Worker (the `workerd` export
 * condition, which wrangler sets): the same API as under Node, on jazz-wasm
 * instead of jazz-napi, which workerd can't load: its backend build
 * (`jazz-wasm/backend`, which the browser build leaves out), initialised
 * from its statically imported module, which wrangler bundles as a
 * CompiledWasm module.
 * A Worker backend uses a Durable Object or remote core. `createJazz` runs in the app's
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
