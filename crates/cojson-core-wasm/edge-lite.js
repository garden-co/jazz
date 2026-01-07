export * from "./public/cojson_core_wasm.js";

import __wbg_init, { initSync } from "./public/cojson_core_wasm.js";
// ?module is to support the vercel edge runtime
import wasm from "./public/cojson_core_wasm.wasm?module";

export async function initialize() {
  return await __wbg_init({ module_or_path: wasm });
}

export function initializeSync() {
  return initSync({ module: wasm });  
}
