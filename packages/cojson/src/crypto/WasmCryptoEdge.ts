import { initialize, initializeSync } from "cojson-core-wasm/edge-lite";
import { WasmCrypto } from "./WasmCrypto.js";

/**
 * Initializes the WASM crypto module for edge runtimes.
 *
 * This function must be called before creating an instance of {@link WasmCrypto}.
 * It prepares the WASM crypto environment for use in edge runtimes such as Vercel functions or Cloudflare Workers.
 *
 * @returns A promise that resolves when the WASM crypto module is successfully initialized.
 */
export const init = async () => {
  return initialize();
};

export const initSync = () => {
  return initializeSync();
};
