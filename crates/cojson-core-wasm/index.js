export * from "./public/cojson_core_wasm.js";

import __wbg_init, { initSync } from "./public/cojson_core_wasm.js";
import { data } from "./public/cojson_core_wasm.wasm.js";

export async function initialize() {
  const response = await fetch(data);

  const arrayBuffer = await response.arrayBuffer();

  return await __wbg_init({ module_or_path: arrayBuffer });
}

function base64ToArrayBuffer(base64) {
  // Node.js environment
  if (typeof Buffer !== "undefined") {
    return Buffer.from(base64, "base64").buffer;
  }

  // Browser / edge-like environment
  if (typeof globalThis !== "undefined" && typeof globalThis.atob === "function") {
    const binary = globalThis.atob(base64);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
    return bytes.buffer;
  }

  throw new Error("Unable to decode base64 WASM payload: no atob() or Buffer available.");
}

export function initializeSync() {
  // `data` is a data URL produced by bundlers (e.g. `data:application/wasm;base64,...`).
  // wasm-bindgen's `initSync` accepts raw bytes (BufferSource) or a WebAssembly.Module.
  const base64 = data.split(",")[1];
  const bytes = base64ToArrayBuffer(base64);
  return initSync({ module: bytes });
}
