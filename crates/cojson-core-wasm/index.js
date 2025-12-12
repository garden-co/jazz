export * from "./public/cojson_core_wasm.js";

import __wbg_init, { initSync } from "./public/cojson_core_wasm.js";
import { data } from "./public/cojson_core_wasm.wasm.js";

export async function initialize() {
  const response = await fetch(data);

  const arrayBuffer = await response.arrayBuffer();

  return await __wbg_init({ module_or_path: arrayBuffer });
}


export function initializeSync() {
  const module = data.split(',')[1];

  return initSync({module: Buffer.from(module, 'base64').buffer});
}
