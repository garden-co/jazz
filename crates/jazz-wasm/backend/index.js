// `jazz-wasm/backend` (the `workerd` condition): jazz-wasm's backend build
// (the default build plus `src/backend.rs`: `serveThinTransaction`,
// `WasmThinRouter`, `loadBackendApp`, ...), which a backend's `createJazz`
// runs on in a Cloudflare Worker. Initialised here from its statically
// imported module (Wrangler bundles it as CompiledWasm). The browser build
// (`jazz-wasm`) leaves the backend out, so browsers never download it.
import { initSync } from "./pkg/jazz_wasm_backend.js";
import wasm from "./pkg/jazz_wasm_backend_bg.wasm";

initSync({ module: wasm });

export * from "./pkg/jazz_wasm_backend.js";
