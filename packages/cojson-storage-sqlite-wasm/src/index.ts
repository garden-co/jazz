import type { StorageAPI } from "cojson";
import { getSqliteStorageAsync } from "cojson";
import { SqliteWasmDriver } from "./SqliteWasmDriver.js";
import { SqliteWasmWorkerStorage } from "./SqliteWasmWorkerStorage.js";

export { SqliteWasmDriver, SqliteWasmWorkerStorage };

/**
 * Create a SQLite Wasm storage adapter for Jazz.
 *
 * When `useOPFS` is `true` (default), a **Web Worker** is spawned that runs
 * the entire storage stack (`SqliteWasmDriver` → `SQLiteClientAsync` →
 * `StorageApiAsync`). The main thread receives a thin `StorageAPI` proxy.
 * Because communication happens at the `StorageAPI` level (not individual
 * SQL queries), postMessage overhead is minimal.
 *
 * OPFS persistence requires APIs (`createSyncAccessHandle`,
 * `Atomics.wait`) that are only available inside dedicated Web Workers,
 * which is why the worker is required.
 *
 * When `useOPFS` is `false`, the database runs in-memory on the main
 * thread (useful for tests).
 *
 * If OPFS initialisation fails inside the worker (e.g. missing
 * COOP/COEP headers, unsupported browser), the worker falls back to
 * an in-memory database automatically.
 *
 * **Requirements for OPFS persistence:**
 * - Server must set `Cross-Origin-Opener-Policy: same-origin` and
 *   `Cross-Origin-Embedder-Policy: require-corp` headers
 * - `@sqlite.org/sqlite-wasm` must be excluded from bundler
 *   optimisation (e.g. `optimizeDeps.exclude` in Vite)
 *
 * @param filenameOrDriver - Database file name for OPFS (default: `"jazz-cojson.sqlite3"`),
 *   or a pre-created `SqliteWasmDriver` to reuse an existing in-memory connection.
 * @param useOPFS - Whether to attempt OPFS persistence via a Web Worker (default: `true`).
 *   Ignored when a driver instance is passed.
 * @returns A fully initialised `StorageAPI` instance
 *
 * @example
 * ```typescript
 * import { getSqliteWasmStorage } from "cojson-storage-sqlite-wasm";
 *
 * const storage = await getSqliteWasmStorage();
 * node.setStorage(storage);
 * ```
 */
export async function getSqliteWasmStorage(
  filenameOrDriver: string | SqliteWasmDriver = "jazz-cojson.sqlite3",
  useOPFS = true,
): Promise<StorageAPI> {
  // Pre-created driver → always main-thread in-memory
  if (filenameOrDriver instanceof SqliteWasmDriver) {
    return await getSqliteStorageAsync(filenameOrDriver);
  }

  if (useOPFS) {
    const workerStorage = new SqliteWasmWorkerStorage(filenameOrDriver);
    await workerStorage.initialize();
    return workerStorage;
  }

  // In-memory on main thread (tests, fallback)
  const driver = new SqliteWasmDriver(filenameOrDriver, false);
  return await getSqliteStorageAsync(driver);
}
