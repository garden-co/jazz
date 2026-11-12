/**
 * Expo web takes this module in place of `browser-worker-assets.ts`, through
 * `withJazz` from `jazz-tools/expo/metro`. See that module for the exports.
 *
 * Metro does not follow `new URL(..., import.meta.url)`, and Expo rewrites
 * `new SharedWorker(new URL(...))` into a dedicated Worker. So both assets go
 * through Metro's own graph here, which serves them in development and writes
 * them to an export under content-hashed names:
 * - the worker as a worker chunk, started by URL as a real SharedWorker;
 * - the WASM as an asset (`withJazz` adds `wasm` to `assetExts`).
 *
 * The chunk is the shipped worker. Expo compiles its `import.meta` to a
 * registry that worker chunks do not have, so the worker must not read
 * `import.meta.url` here: the page hands it the WASM URL, and the loader asks
 * for its own module's URL only when it has none.
 */

// Metro's module-scoped `require`. `unstable_resolveWorker` is how Expo names
// a worker chunk without constructing the worker.
declare const require: {
  (path: string): unknown;
  unstable_resolveWorker(path: string): string;
};

function pageUrl(path: string): string {
  return new URL(path, location.href).href;
}

/**
 * Expo's `import.meta.url` is the URL of the script that is executing, and is
 * null once that script has finished (SDK 54). A Metro bundle is served from
 * the page's origin, so the page stands in for the module.
 */
export function browserRuntimeModuleUrl(): string {
  return location.href;
}
export function bundledBrowserWorkerUrl(): string {
  return pageUrl(require.unstable_resolveWorker("../worker/jazz-broker-worker.js"));
}

/** The binary shipped beside the worker, which is the pair the worker's glue was checked against. */
export function bundledWasmUrl(): string | null {
  return pageUrl(require("../worker/jazz_wasm_bg.wasm") as string);
}

/** A Metro chunk is a classic script. */
export function bundledBrowserWorkerType(): WorkerType | null {
  return "classic";
}
