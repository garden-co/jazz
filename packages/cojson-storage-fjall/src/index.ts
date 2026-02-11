import { StorageApiAsync } from "cojson";
import { FjallStorageNapi } from "cojson-core-napi";
import { FjallClient } from "./client.js";
import type { FjallStorageNapiTyped } from "./types.js";

export { FjallClient } from "./client.js";
export type { FjallStorageNapiTyped } from "./types.js";

/**
 * Create a fjall-backed storage engine for Jazz.
 *
 * This uses the fjall LSM-tree storage engine via NAPI bindings,
 * with all I/O offloaded to libuv worker threads.
 *
 * @param path - Path to the fjall database directory
 * @returns A `StorageApiAsync` instance ready to be passed to `localNode.setStorage()`
 */
export function getFjallStorage(path: string) {
  const napi = new FjallStorageNapi(path) as unknown as FjallStorageNapiTyped;
  const client = new FjallClient(napi);
  return new StorageApiAsync(client);
}
