// Import and initialize the appropriate build based on environment
const wasmModule = await (typeof window === "undefined"
  ? import("./dist/node/jazz_crypto_rs.js")
  : import("./dist/web/jazz_crypto_rs.js"));

// Initialize if needed (web environment)
// https://webassembly.org/getting-started/js-api/
if ("default" in wasmModule && typeof wasmModule.default === "function") {
  await wasmModule.default();
}

// Handle both CommonJS and ES module exports
interface WasmExports {
  generate_nonce: (nonce_material: Uint8Array) => Uint8Array;
  blake3_hash_once: (data: Uint8Array) => Uint8Array;
  blake3_hash_once_with_context: (
    data: Uint8Array,
    context: Uint8Array,
  ) => Uint8Array;
  blake3_empty_state: () => Uint8Array;
  blake3_update_state: (state: Uint8Array, data: Uint8Array) => Uint8Array;
  blake3_digest_for_state: (state: Uint8Array) => Uint8Array;
}

const moduleExports = ("default" in wasmModule
  ? wasmModule.default
  : wasmModule) as unknown as WasmExports;

// Export all functions from the module
export const generate_nonce = moduleExports.generate_nonce;
export const blake3_hash_once = moduleExports.blake3_hash_once;
export const blake3_hash_once_with_context =
  moduleExports.blake3_hash_once_with_context;
export const blake3_empty_state = moduleExports.blake3_empty_state;
export const blake3_update_state = moduleExports.blake3_update_state;
export const blake3_digest_for_state = moduleExports.blake3_digest_for_state;
