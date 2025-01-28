// Import and initialize the appropriate build based on environment
const wasmModule = await (typeof window === "undefined"
  ? import("./dist/node/jazz_crypto_rs.js")
  : import("./dist/web/jazz_crypto_rs.js"));

// Initialize if needed (node environment)
if ("default" in wasmModule && typeof wasmModule.default === "function") {
  await wasmModule.default();
}

// Export the functions
export const { generate_nonce } = wasmModule;
