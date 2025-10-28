export * from "./public/cojson_core_wasm.js";

import __wbg_init from "./public/cojson_core_wasm.js";
// ?module is to support the vercel edge runtime
import wasm from "./public/cojson_core_wasm.wasm?module";

export async function initialize() {
  return await __wbg_init({ module_or_path: wasm });
}
