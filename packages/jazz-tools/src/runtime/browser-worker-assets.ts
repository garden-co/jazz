/**
 * Keep browser asset literals visible to bundlers, outside native imports.
 *
 * Metro does not follow these literals, so Expo web takes
 * `browser-worker-assets.metro.ts` in place of this module, through `withJazz`
 * from `jazz-tools/expo/metro`. Both export the same names.
 */
export function browserRuntimeModuleUrl(): string {
  return import.meta.url;
}
export function bundledBrowserWorkerUrl(): string {
  return new URL("../worker/jazz-broker-worker.js", import.meta.url).href;
}

/**
 * URL of the WASM binary, for a bundler that cannot follow wasm-bindgen's own
 * `new URL("jazz_wasm_bg.wasm", import.meta.url)`. These bundlers can, so
 * wasm-bindgen keeps resolving the binary itself.
 */
export function bundledWasmUrl(): string | null {
  return null;
}

/**
 * Script type of the worker at `bundledBrowserWorkerUrl()`, for a bundler that
 * must start it from that URL. Null here: these bundlers build the worker from
 * the `new SharedWorker(new URL(...))` expression instead.
 */
export function bundledBrowserWorkerType(): WorkerType | null {
  return null;
}
