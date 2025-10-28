export * from "./public/cojson_core_wasm.js";

import __wbg_init from "./public/cojson_core_wasm.js";
import { data } from "./public/cojson_core_wasm.wasm.js";

export async function initialize() {
  const response = await fetch(data);

  const arrayBuffer = await response.arrayBuffer();

  return await __wbg_init({ module_or_path: arrayBuffer });
}
