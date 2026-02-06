/**
 * Native storage integration for Jazz.
 *
 * Provides high-performance Rust-based storage backends that work across
 * Node.js, browsers (WASM), and React Native.
 *
 * @example
 * ```typescript
 * import { getNativeStorage, isNativeStorageAvailable } from "cojson/storage/native";
 *
 * // Check if native storage is available
 * if (isNativeStorageAvailable()) {
 *   const storage = await getNativeStorage({ path: "./data" });
 *   node.setStorage(storage);
 * }
 * ```
 */

import { StorageApiSync } from "../storageSync.js";
import { NativeClient } from "./client.js";
import type { NativeStorageNapi, NativeStorageWasm } from "./types.js";

export type { NativeStorageNapi, NativeStorageWasm } from "./types.js";
export { NativeClient } from "./client.js";

/**
 * Options for creating native storage.
 */
export interface NativeStorageOptions {
  /**
   * File path for storage (Node.js and React Native only).
   * If not provided, uses in-memory storage.
   */
  path?: string;

  /**
   * Database name for OPFS storage (browser only).
   * Defaults to "jazz-storage".
   */
  dbName?: string;
}

/**
 * Create a native storage instance.
 *
 * Uses the native Rust storage backend for high performance.
 * Automatically selects the appropriate backend based on the platform:
 * - Node.js: NAPI with file-based persistence
 * - Browser: WASM with OPFS or in-memory storage
 * - React Native: UniFFI with file-based persistence
 *
 * @param storage - The native storage driver (NAPI or WASM)
 * @returns StorageApiSync instance wrapping the native storage
 *
 * @example
 * ```typescript
 * // Node.js (using NAPI)
 * import { NativeStorage } from "cojson-core-napi";
 * const nativeStorage = NativeStorage.withPath("./jazz-data");
 * const storage = getNativeStorage(nativeStorage);
 *
 * // Browser (using WASM)
 * import { NativeStorage } from "cojson-core-wasm";
 * const nativeStorage = NativeStorage.inMemory();
 * const storage = getNativeStorage(nativeStorage);
 * ```
 */
export function getNativeStorage(
  storage: NativeStorageNapi | NativeStorageWasm,
) {
  const client = new NativeClient(storage);
  return new StorageApiSync(client);
}

/**
 * Check if native storage is available in the current environment.
 *
 * Returns true if the native storage bindings can be loaded.
 * This is a synchronous check that doesn't actually load the bindings.
 *
 * @returns true if native storage is available
 */
export function isNativeStorageAvailable(): boolean {
  // Check if we're in Node.js and can potentially load NAPI
  if (typeof process !== "undefined" && process.versions?.node) {
    // Node.js environment - NAPI should be available
    return true;
  }

  // Check if we're in a browser with WASM support
  // @ts-ignore - WebAssembly is a browser global
  if (typeof WebAssembly !== "undefined") {
    return true;
  }

  return false;
}

/**
 * Check if OPFS is available in the current browser environment.
 *
 * OPFS (Origin Private File System) provides persistent storage for browsers.
 * It's available in modern browsers and Web Workers.
 *
 * @returns true if OPFS is available
 */
export function isOpfsAvailable(): boolean {
  // @ts-ignore - navigator is a browser global
  if (typeof navigator === "undefined") {
    return false;
  }

  return (
    // @ts-ignore - navigator is a browser global
    "storage" in navigator &&
    // @ts-ignore - navigator is a browser global
    typeof navigator.storage.getDirectory === "function"
  );
}

/**
 * Check if running in a Web Worker context.
 *
 * Web Workers can use OPFS Synchronous Access Handles for better performance.
 *
 * @returns true if running in a Web Worker
 */
export function isInWorker(): boolean {
  // @ts-ignore - self is a browser/worker global
  if (typeof self === "undefined") {
    return false;
  }

  // @ts-ignore - WorkerGlobalScope is a worker global
  return typeof self.WorkerGlobalScope !== "undefined";
}
