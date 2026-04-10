import type { RuntimeSourcesConfig } from "./context.js";

function isHttpUrl(moduleUrl: string): boolean {
  const protocol = new URL(moduleUrl).protocol;
  return protocol === "http:" || protocol === "https:";
}

function resolveBrowserAssetBase(locationHref: string): string {
  return new URL("/", locationHref).href;
}

function resolveConfiguredUrl(url: string, locationHref: string | undefined): string {
  if (locationHref) {
    return new URL(url, locationHref).href;
  }

  return new URL(url).href;
}

function resolveConfiguredBaseUrl(
  baseUrl: string,
  locationHref: string | undefined,
): string | null {
  if (!locationHref) {
    return null;
  }

  return new URL(baseUrl, locationHref).href;
}

function resolveDerivedWasmUrl(
  runtimeModuleUrl: string,
  locationHref: string | undefined,
  allowHttpPageFallback: boolean,
): string | null {
  if (
    !locationHref ||
    isHttpUrl(runtimeModuleUrl) ||
    (!allowHttpPageFallback && isHttpUrl(locationHref))
  ) {
    return null;
  }

  return new URL("jazz_wasm_bg.wasm", resolveBrowserAssetBase(locationHref)).href;
}

export function resolveRuntimeConfigSyncInitInput(
  runtime?: RuntimeSourcesConfig,
): { module: BufferSource | WebAssembly.Module } | null {
  if (runtime?.wasmModule) {
    return { module: runtime.wasmModule };
  }

  if (runtime?.wasmSource) {
    return { module: runtime.wasmSource };
  }

  return null;
}

export function resolveRuntimeConfigWasmUrl(
  runtimeModuleUrl: string,
  locationHref: string | undefined,
  runtime?: RuntimeSourcesConfig,
): string | null {
  if (runtime?.wasmUrl) {
    return resolveConfiguredUrl(runtime.wasmUrl, locationHref);
  }

  if (runtime?.baseUrl) {
    const baseUrl = resolveConfiguredBaseUrl(runtime.baseUrl, locationHref);
    if (baseUrl) {
      return new URL("jazz_wasm_bg.wasm", baseUrl).href;
    }
  }

  // In any web-hosted context (HTTP/HTTPS page), we are inside a bundled app.
  // Bundlers (Vite, Turbopack, webpack) transform the `new URL('*.wasm', import.meta.url)`
  // pattern in jazz_wasm.js and bake in the correct asset URL at build time.
  // Returning null lets wasm-bindgen use that bundler-resolved URL.
  // We only fall through to compute a root-relative URL when the page is served
  // from a non-HTTP origin (e.g. file://) — a static HTML page with the WASM
  // copied to the same directory.
  return resolveDerivedWasmUrl(runtimeModuleUrl, locationHref, false);
}

export function resolveWorkerBootstrapWasmUrl(
  runtimeModuleUrl: string,
  locationHref: string | undefined,
  runtime?: RuntimeSourcesConfig,
): string | null {
  if (runtime?.wasmUrl) {
    return resolveConfiguredUrl(runtime.wasmUrl, locationHref);
  }

  if (runtime?.baseUrl) {
    const baseUrl = resolveConfiguredBaseUrl(runtime.baseUrl, locationHref);
    if (baseUrl) {
      return new URL("jazz_wasm_bg.wasm", baseUrl).href;
    }
  }

  // Worker bootstrap still needs an explicit wasm URL when the page is HTTP-hosted
  // but the runtime module itself is bundled from a file:// URL.
  return resolveDerivedWasmUrl(runtimeModuleUrl, locationHref, true);
}

export function resolveRuntimeConfigWorkerUrl(
  runtimeModuleUrl: string,
  locationHref: string | undefined,
  runtime?: RuntimeSourcesConfig,
): string {
  if (runtime?.workerUrl) {
    return resolveConfiguredUrl(runtime.workerUrl, locationHref);
  }

  if (runtime?.baseUrl) {
    const baseUrl = resolveConfiguredBaseUrl(runtime.baseUrl, locationHref);
    if (baseUrl) {
      return new URL("worker/jazz-worker.js", baseUrl).href;
    }
  }

  if (!locationHref || isHttpUrl(runtimeModuleUrl)) {
    return new URL("../worker/jazz-worker.js", runtimeModuleUrl).href;
  }

  return new URL("worker/jazz-worker.js", resolveBrowserAssetBase(locationHref)).href;
}

export function appendWorkerRuntimeWasmUrl(workerUrl: string, wasmUrl: string | null): string {
  if (!wasmUrl) {
    return workerUrl;
  }

  const url = new URL(workerUrl);
  url.searchParams.set("jazz-wasm-url", wasmUrl);
  return url.href;
}

export function readWorkerRuntimeWasmUrl(locationHref: string | undefined): string | null {
  if (!locationHref) {
    return null;
  }

  return new URL(locationHref).searchParams.get("jazz-wasm-url");
}
